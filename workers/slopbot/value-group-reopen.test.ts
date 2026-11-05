/** PRD sections 5 and 7. Shared accepted commits need one joint assessment.
 * A published or attempts-exhausted single-PR delivery is terminal, so later
 * group discovery never runs and publication stops on the overlap guard.
 * Reclaim the original delivery. Do not invent a GitHub delivery or delete
 * inference attempts.
 */
import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { assertProjectDefinition } from "../../src/lib/project-schema.mjs";
import { REWARD_ASSET_NETWORKS } from "../../src/lib/reward-asset.mjs";
import { applicationManifest } from "../../tests/project-application-manifest";
import { MIGRATIONS, sqliteD1 } from "../../tests/sqlite-d1";
import { recordDelivery } from "./persistence";
import { reopenDueSharedCommitAssessments } from "./value-group-reopen";
import { claimValueRequest } from "./value-processing";

const repository = "fresh-registration/toolkit";
const repositoryId = 123456789;
const nodeId = "R_example";
const revision = "a".repeat(40);
const at = "2026-11-01T04:00:00.000Z";

function database() {
  const queue = sqliteD1(MIGRATIONS);
  const project = assertProjectDefinition(
    applicationManifest("fresh-registration", repository, {
      status: "active",
      rewardAsset: {
        network: "ethereum",
        kind: "stablecoin",
        ...REWARD_ASSET_NETWORKS.ethereum.stablecoin,
        chainId: REWARD_ASSET_NETWORKS.ethereum.chainId,
        cluster: null,
        verifiedAt: at,
      },
    }),
  );
  const manifest = JSON.stringify(project);
  const digest = createHash("sha256").update(manifest).digest("hex");
  queue.sqlite.run(
    "INSERT INTO project_revisions(project_id,revision,manifest,manifest_sha256,reason,created_at) VALUES (?,1,?,?,?,?)",
    [project.id, manifest, digest, "registration workflow", at],
  );
  queue.sqlite.run(
    "INSERT INTO projects(id,status,revision,manifest,manifest_sha256,created_at,updated_at) VALUES (?,'active',1,?,?,?,?)",
    [project.id, manifest, digest, at, at],
  );
  queue.sqlite.run(
    "INSERT INTO project_repositories(repository_key,project_id) VALUES (?,?)",
    [repository, project.id],
  );
  return queue;
}

async function seed(
  queue: ReturnType<typeof database>,
  rows: { id: string; number: number; outcome: string }[],
) {
  for (const row of rows) {
    await recordDelivery(queue.db, row.id, "pull_request");
    queue.sqlite.run(
      "INSERT INTO slopbot_value_requests(delivery_id,installation_id,repository_id,owner,repo,pull_request_number,received_at) VALUES (?,?,?,?,?,?,?)",
      [
        row.id,
        1,
        repositoryId,
        "fresh-registration",
        "toolkit",
        row.number,
        at,
      ],
    );
    queue.sqlite.run(
      "INSERT INTO slopbot_value_processing(delivery_id,claim_token,lease_until,state,outcome,retry_at,updated_at,attempts) VALUES (?,?,?,'completed',?,NULL,?,0)",
      [row.id, `token-${row.id}`, at, row.outcome, at],
    );
  }
}

const pulls = [
  {
    id: "PR_3842",
    number: 3842,
    commit: "b".repeat(40),
    merge: "a".repeat(40),
  },
  {
    id: "PR_3914",
    number: 3914,
    commit: "b".repeat(40),
    merge: "c".repeat(40),
  },
  { id: "PR_100", number: 100, commit: "d".repeat(40), merge: "e".repeat(40) },
];

function client(overlap: "shared" | "distinct" | "incomplete") {
  let graphqlCalls = 0;
  return {
    get calls() {
      return graphqlCalls;
    },
    async get<T>(path: string): Promise<T | null> {
      if (path !== `/repos/${repository}`) return null;
      return { id: repositoryId, node_id: nodeId, private: false } as T;
    },
    async graphql<T>(
      query: string,
      variables: Record<string, unknown>,
    ): Promise<T> {
      graphqlCalls += 1;
      expect(query).toContain("search(query:");
      expect(String(variables.q)).toStartWith(
        `repo:${repository} is:pr is:merged merged:>=2026-11-01`,
      );
      if (overlap === "incomplete") {
        return {
          search: {
            issueCount: 2,
            nodes: [],
            pageInfo: { hasNextPage: false, endCursor: null },
          },
        } as T;
      }
      const nodes = pulls.map((pull) => ({
        id: pull.id,
        number: pull.number,
        url: `https://github.com/${repository}/pull/${pull.number}`,
        mergedAt: "2026-11-01T03:35:48Z",
        repository: {
          id: nodeId,
          databaseId: repositoryId,
          nameWithOwner: repository,
          isPrivate: false,
        },
        mergeCommit: { oid: pull.merge },
        commits: {
          totalCount: 1,
          nodes: [
            {
              commit: {
                oid:
                  overlap === "distinct" && pull.number === 3914
                    ? "f".repeat(40)
                    : pull.commit,
              },
            },
          ],
          pageInfo: { hasNextPage: false, endCursor: null },
        },
      }));
      return {
        search: {
          issueCount: nodes.length,
          nodes,
          pageInfo: { hasNextPage: false, endCursor: null },
        },
      } as T;
    },
    async fileText(): Promise<null> {
      throw new Error("policy text is not part of group reopen");
    },
  };
}

function env(queue: ReturnType<typeof database>) {
  return {
    SLOPBOT_DB: queue.db,
    SLOPBOT_APP_ID: "1",
    SLOPBOT_APP_PRIVATE_KEY: "unused",
    SLOPBOT_PRODUCER_REVISION: revision,
    SLOPBOT_VALUE_MONTHLY_BUDGET_MICRO_USD: "alerts-only",
  };
}

function outcome(queue: ReturnType<typeof database>, deliveryId: string) {
  return queue.sqlite
    .query(
      "SELECT state, outcome FROM slopbot_value_processing WHERE delivery_id = ?",
    )
    .get(deliveryId) as { state: string; outcome: string | null };
}

const overlapping = [
  { id: "delivery-3842", number: 3842, outcome: "attempts-exhausted" },
  { id: "delivery-3914", number: 3914, outcome: "published" },
  { id: "delivery-100", number: 100, outcome: "published" },
];

test("reopens the original delivery for a shared-commit group", async () => {
  const queue = database();
  try {
    await seed(queue, overlapping);
    queue.sqlite.run(
      "INSERT INTO slopbot_value_inference_attempts(attempt_id,inference_key,started_at) VALUES (?,?,?)",
      ["attempt-3842", "1".repeat(64), at],
    );
    const github = client("shared");
    // A completed delivery cannot be claimed again, so shared-commit discovery
    // never runs for it.
    expect(await claimValueRequest(queue.db, "delivery-3842")).toBeNull();
    expect(
      await reopenDueSharedCommitAssessments(env(queue), async () => github),
    ).toBe(1);
    expect(outcome(queue, "delivery-3842")).toEqual({
      state: "waiting",
      outcome: null,
    });
    expect(outcome(queue, "delivery-3914")).toEqual({
      state: "completed",
      outcome: "published",
    });
    expect(outcome(queue, "delivery-100")).toEqual({
      state: "completed",
      outcome: "published",
    });
    expect(
      queue.sqlite
        .query("SELECT count(*) AS count FROM slopbot_value_requests")
        .get(),
    ).toEqual({ count: 3 });
    expect(
      queue.sqlite
        .query("SELECT count(*) AS count FROM slopbot_deliveries")
        .get(),
    ).toEqual({ count: 3 });
    expect(
      queue.sqlite
        .query("SELECT attempt_id FROM slopbot_value_inference_attempts")
        .get(),
    ).toEqual({ attempt_id: "attempt-3842" });
    expect(await claimValueRequest(queue.db, "delivery-3842")).not.toBeNull();
    expect(
      await reopenDueSharedCommitAssessments(env(queue), async () => github),
    ).toBe(0);
    expect(github.calls).toBe(1);
  } finally {
    queue.sqlite.close();
  }
});

test("leaves independent assessments terminal", async () => {
  const queue = database();
  try {
    await seed(queue, overlapping);
    const github = client("distinct");
    expect(
      await reopenDueSharedCommitAssessments(env(queue), async () => github),
    ).toBe(0);
    expect(outcome(queue, "delivery-3842").state).toBe("completed");
    expect(outcome(queue, "delivery-3914").state).toBe("completed");
    expect(
      queue.sqlite
        .query("SELECT count(*) AS count FROM slopbot_value_group_scans")
        .get(),
    ).toEqual({ count: 1 });
  } finally {
    queue.sqlite.close();
  }
});

test("does not record a scan when the commit census is incomplete", async () => {
  const queue = database();
  try {
    await seed(queue, overlapping);
    const github = client("incomplete");
    expect(
      await reopenDueSharedCommitAssessments(env(queue), async () => github),
    ).toBe(0);
    expect(
      await reopenDueSharedCommitAssessments(env(queue), async () => github),
    ).toBe(0);
    expect(github.calls).toBe(2);
    expect(outcome(queue, "delivery-3842").state).toBe("completed");
    expect(
      queue.sqlite
        .query("SELECT count(*) AS count FROM slopbot_value_group_scans")
        .get(),
    ).toEqual({ count: 0 });
  } finally {
    queue.sqlite.close();
  }
});

test("does not reclaim a historical delivery", async () => {
  const queue = database();
  try {
    await seed(queue, [
      { id: "delivery-3842", number: 3842, outcome: "historical" },
      { id: "delivery-3914", number: 3914, outcome: "published" },
      { id: "delivery-100", number: 100, outcome: "published" },
    ]);
    const github = client("shared");
    expect(
      await reopenDueSharedCommitAssessments(env(queue), async () => github),
    ).toBe(1);
    expect(outcome(queue, "delivery-3842")).toEqual({
      state: "completed",
      outcome: "historical",
    });
    expect(outcome(queue, "delivery-3914")).toEqual({
      state: "waiting",
      outcome: null,
    });
  } finally {
    queue.sqlite.close();
  }
});

test("does not reopen a group that already has a joint record", async () => {
  const queue = database();
  try {
    await seed(queue, overlapping);
    const record = {
      schemaVersion: "slop-work-unit-record:v1",
      projectId: "fresh-registration",
      sourceDigest: "a".repeat(64),
      sources: ["PR_3842", "PR_3914"].map((pullRequestNodeId) => ({
        repositoryNodeId: nodeId,
        pullRequestNodeId,
      })),
      claims: [],
      supersedes: [],
      legacyPredecessors: [],
      revisionReason: null,
    };
    const recordJson = JSON.stringify(record);
    queue.sqlite.run(
      `INSERT INTO slopbot_work_unit_journal
       (record_digest, assessment_key, project_id, source_digest, record_json, recorded_at)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        "c".repeat(64),
        "b".repeat(64),
        "fresh-registration",
        record.sourceDigest,
        recordJson,
        at,
      ],
    );
    const github = client("shared");
    expect(
      await reopenDueSharedCommitAssessments(env(queue), async () => github),
    ).toBe(0);
    expect(outcome(queue, "delivery-3842")).toEqual({
      state: "completed",
      outcome: "attempts-exhausted",
    });
    expect(outcome(queue, "delivery-3914")).toEqual({
      state: "completed",
      outcome: "published",
    });
  } finally {
    queue.sqlite.close();
  }
});
