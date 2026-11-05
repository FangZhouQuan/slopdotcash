/** Reopen an existing value delivery when shared accepted commits have no
 * joint assessment (PRD sections 5 and 7).
 *
 * Group discovery can land after a single-PR assessment was published or its
 * inference attempts were used up. Those deliveries are terminal, so the
 * scheduler never collects the group, and the leaderboard keeps refusing the
 * overlapping single assessments. Replaying the GitHub delivery is a no-op,
 * and a synthetic delivery would invent provenance.
 *
 * This scan reclaims the original delivery. The joint source digest is a new
 * inference key, so the single-PR attempt records stay immutable. One scan
 * per repository and producer revision keeps a failed joint attempt from
 * being reopened on every schedule.
 */

import { assertProjectDefinition } from "../../src/lib/project-schema.mjs";
import type { ProjectDefinition } from "../../src/lib/projects.mjs";
import { GitHubClient } from "./github";
import type { D1Database } from "./persistence";
import { registeredProject } from "./registry";
import { collectAcceptedCommitCensus } from "./value-accepted-commit-census";
import type { ValueEvidence } from "./value-evidence";
import type { ValueClient } from "./value-publication";
import { type ValueSettingsEnv, valueSettings } from "./value-settings";
import { currentWorkUnitRecord } from "./value-work-unit-journal";

type ReopenEnv = ValueSettingsEnv & {
  SLOPBOT_DB: D1Database;
  SLOPBOT_PRODUCER_REVISION?: string;
  SLOPBOT_APP_ID: string;
  SLOPBOT_APP_PRIVATE_KEY: string;
};

/** collectValueGroup refuses a group larger than this. Reopening one would
 * only turn a terminal delivery into a permanent source-incomplete retry.
 */
const MAX_GROUP = 32;
/** Reopens issued from one census. A later schedule continues the rest. */
const REOPEN_LIMIT = 20;

interface Candidate {
  repositoryId: number;
  installationId: number;
  owner: string;
  repo: string;
}

function githubPath(url: string): string {
  return new URL(url).pathname.replace(/^\/|\/$/gu, "");
}

function compareCodeUnits(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0;
}

async function recordScan(
  db: D1Database,
  repositoryId: number,
  producerRevision: string,
  at: string,
) {
  await db
    .prepare(
      `INSERT INTO slopbot_value_group_scans
       (repository_id, producer_revision, scanned_at)
       VALUES (?, ?, ?)
       ON CONFLICT DO NOTHING`,
    )
    .bind(repositoryId, producerRevision, at)
    .run();
}

async function activeProject(
  db: D1Database,
  candidate: Candidate,
): Promise<{ project: ProjectDefinition; path: string } | null> {
  const direct = await registeredProject(db, candidate);
  const pathFrom = (
    project: ProjectDefinition,
    owner: string,
    repo: string,
  ) => {
    const key = `${owner}/${repo}`.toLowerCase();
    return (
      project.repositories
        .map((repository) => githubPath(repository.githubUrl))
        .find((path) => path.toLowerCase() === key) ?? null
    );
  };
  if (direct) {
    const path = pathFrom(direct, candidate.owner, candidate.repo);
    return path ? { project: direct, path } : null;
  }
  const row = await db
    .prepare(
      `SELECT manifest FROM projects
       WHERE status = 'active'
         AND json_extract(manifest, '$.authority.repositoryId') = ?`,
    )
    .bind(String(candidate.repositoryId))
    .first<{ manifest: string }>();
  if (!row) return null;
  const parsed = assertProjectDefinition(JSON.parse(row.manifest));
  if (parsed.status !== "active") return null;
  const primary = githubPath(parsed.repositories[0].githubUrl).split("/");
  const owner = primary[0];
  const repo = primary[1];
  if (!owner || !repo) return null;
  const allowed = await registeredProject(db, {
    owner,
    repo,
    repositoryId: candidate.repositoryId,
  });
  if (!allowed) return null;
  const path = pathFrom(allowed, owner, repo);
  return path ? { project: allowed, path } : null;
}

/** Connected pull requests that list the same accepted commit. */
function sharedGroups(
  carriers: ReadonlyMap<string, readonly { number: number; nodeId: string }[]>,
): { number: number; nodeId: string }[][] {
  const parent = new Map<string, string>();
  const byId = new Map<string, { number: number; nodeId: string }>();
  const find = (id: string): string => {
    const current = parent.get(id) ?? id;
    if (current === id) return id;
    const root = find(current);
    parent.set(id, root);
    return root;
  };
  const union = (left: string, right: string) => {
    const leftRoot = find(left);
    const rightRoot = find(right);
    if (leftRoot !== rightRoot) parent.set(leftRoot, rightRoot);
  };
  for (const matches of carriers.values()) {
    const unique: { number: number; nodeId: string }[] = [];
    const seen = new Set<string>();
    for (const match of matches) {
      if (seen.has(match.nodeId)) continue;
      seen.add(match.nodeId);
      unique.push({ number: match.number, nodeId: match.nodeId });
      byId.set(match.nodeId, unique[unique.length - 1]);
      if (!parent.has(match.nodeId)) parent.set(match.nodeId, match.nodeId);
    }
    for (let index = 1; index < unique.length; index += 1)
      union(unique[0].nodeId, unique[index].nodeId);
  }
  const groups = new Map<string, { number: number; nodeId: string }[]>();
  for (const id of byId.keys()) {
    const root = find(id);
    const group = groups.get(root) ?? [];
    const member = byId.get(id);
    if (member) group.push(member);
    groups.set(root, group);
  }
  return [...groups.values()];
}

async function reopenDelivery(
  db: D1Database,
  repositoryId: number,
  number: number,
  at: string,
): Promise<boolean> {
  const row = await db
    .prepare(
      `SELECT p.delivery_id AS deliveryId
       FROM slopbot_value_requests r
       JOIN slopbot_value_processing p ON p.delivery_id = r.delivery_id
       WHERE r.repository_id = ? AND r.pull_request_number = ?
         AND p.state = 'completed'
         AND p.outcome IN ('published', 'attempts-exhausted', 'current')
         AND NOT EXISTS (
           SELECT 1 FROM slopbot_value_requests other
           JOIN slopbot_value_processing busy
             ON busy.delivery_id = other.delivery_id
           WHERE other.repository_id = r.repository_id
             AND other.pull_request_number = r.pull_request_number
             AND busy.state IN ('waiting', 'running')
         )
       ORDER BY p.updated_at, p.delivery_id
       LIMIT 1`,
    )
    .bind(repositoryId, number)
    .first<{ deliveryId: string }>();
  if (!row) return false;
  await db
    .prepare(
      `UPDATE slopbot_value_processing
       SET state = 'waiting', outcome = NULL, retry_at = ?1,
         claim_token = ?2, lease_until = ?1, updated_at = ?1,
         incomplete_since = NULL
       WHERE delivery_id = ?3 AND state = 'completed'
         AND outcome IN ('published', 'attempts-exhausted', 'current')`,
    )
    .bind(at, crypto.randomUUID(), row.deliveryId)
    .run();
  const updated = await db
    .prepare("SELECT state FROM slopbot_value_processing WHERE delivery_id = ?")
    .bind(row.deliveryId)
    .first<{ state: string }>();
  return updated?.state === "waiting";
}

/** Census one repository and reclaim original deliveries whose accepted
 * commits overlap and have no current joint record. Returns how many
 * deliveries were reopened.
 */
export async function reopenSharedCommitAssessments(
  db: D1Database,
  input: {
    producerRevision: string;
    startAt: number;
    repositoryId: number;
    repositoryNodeId: string;
    current: string;
    github: ValueClient;
    at?: string;
  },
): Promise<number> {
  if (
    !/^[a-f0-9]{40}$/.test(input.producerRevision) ||
    !Number.isFinite(input.startAt) ||
    !Number.isSafeInteger(input.repositoryId) ||
    input.repositoryId <= 0
  )
    throw new Error("Shared-commit reopen identity is invalid");
  const at = input.at ?? new Date().toISOString();
  const census = await collectAcceptedCommitCensus(
    input.github,
    {
      repository: input.current,
      repositoryNodeId: input.repositoryNodeId,
      repositoryDatabaseId: input.repositoryId,
    } as ValueEvidence,
    input.startAt,
    input.current,
  );
  if (!census) return 0;
  const pending: { number: number; nodeId: string }[][] = [];
  for (const group of sharedGroups(census.carriers)) {
    if (group.length < 2 || group.length > MAX_GROUP) continue;
    const uncovered: { number: number; nodeId: string }[] = [];
    for (const member of group) {
      const joint = await currentWorkUnitRecord(
        db,
        input.repositoryNodeId,
        member.nodeId,
      );
      if (!joint) uncovered.push(member);
    }
    if (uncovered.length > 0) pending.push(uncovered);
  }
  let reopened = 0;
  for (const group of pending) {
    if (reopened >= REOPEN_LIMIT) break;
    const ordered = [...group].sort(
      (left, right) =>
        left.number - right.number ||
        compareCodeUnits(left.nodeId, right.nodeId),
    );
    for (const member of ordered) {
      if (await reopenDelivery(db, input.repositoryId, member.number, at)) {
        reopened += 1;
        break;
      }
    }
  }
  if (!(pending.length > REOPEN_LIMIT && reopened >= REOPEN_LIMIT))
    await recordScan(db, input.repositoryId, input.producerRevision, at);
  return reopened;
}

async function nextUnscannedRepository(
  db: D1Database,
  producerRevision: string,
): Promise<Candidate | null> {
  return db
    .prepare(
      `SELECT r.repository_id AS repositoryId,
         r.installation_id AS installationId, r.owner, r.repo
       FROM slopbot_value_processing p
       JOIN slopbot_value_requests r ON r.delivery_id = p.delivery_id
       WHERE p.state = 'completed'
         AND p.outcome IN ('published', 'attempts-exhausted', 'current')
         AND NOT EXISTS (
           SELECT 1 FROM slopbot_value_group_scans s
           WHERE s.repository_id = r.repository_id
             AND s.producer_revision = ?
         )
       ORDER BY p.updated_at, r.repository_id
       LIMIT 1`,
    )
    .bind(producerRevision)
    .first<Candidate>();
}

/** One repository per schedule. A missing census or GitHub read is retried
 * next time and does not mark the repository scanned.
 */
export async function reopenDueSharedCommitAssessments(
  env: ReopenEnv,
  githubForInstallation: (installationId: number) => Promise<ValueClient> = (
    installationId,
  ) =>
    GitHubClient.forInstallation(
      env.SLOPBOT_APP_ID,
      env.SLOPBOT_APP_PRIVATE_KEY,
      installationId,
    ),
): Promise<number> {
  const settings = valueSettings(env);
  const revision = env.SLOPBOT_PRODUCER_REVISION;
  if (!settings || !revision || !/^[a-f0-9]{40}$/.test(revision)) return 0;
  const db = env.SLOPBOT_DB;
  const candidate = await nextUnscannedRepository(db, revision);
  if (!candidate) return 0;
  const at = new Date().toISOString();
  try {
    const active = await activeProject(db, candidate);
    if (!active) {
      await recordScan(db, candidate.repositoryId, revision, at);
      return 0;
    }
    const { project, path } = active;
    const github = await githubForInstallation(candidate.installationId);
    const identity = await github.get<{
      id: number;
      node_id: string;
      private: boolean;
    }>(`/repos/${path}`);
    if (!identity) return 0;
    const primary = githubPath(project.repositories[0].githubUrl);
    const nodeMismatch =
      path.toLowerCase() === primary.toLowerCase() &&
      identity.node_id !== project.authority.repositoryNodeId;
    if (
      identity.private !== false ||
      identity.id !== candidate.repositoryId ||
      nodeMismatch
    ) {
      await recordScan(db, candidate.repositoryId, revision, at);
      return 0;
    }
    return await reopenSharedCommitAssessments(db, {
      producerRevision: revision,
      startAt: settings.start,
      repositoryId: candidate.repositoryId,
      repositoryNodeId: identity.node_id,
      current: path,
      github,
      at,
    });
  } catch (error) {
    console.error(
      JSON.stringify({
        event: "value_group_reopen_retry",
        repositoryId: candidate.repositoryId,
        kind: error instanceof Error ? error.name : "unknown",
      }),
    );
    return 0;
  }
}
