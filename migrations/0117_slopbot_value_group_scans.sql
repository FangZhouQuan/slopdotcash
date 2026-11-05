-- PRD sections 5 and 7. A shared accepted commit needs one joint value
-- assessment. Discovery of that group is corrected evidence, so an existing
-- durable delivery may be claimed again. This row records that one producer
-- revision already scanned a repository, which keeps the scan from repeating
-- every schedule and does not reset inference attempts or invent a delivery.
CREATE TABLE slopbot_value_group_scans (
 repository_id INTEGER NOT NULL CHECK(repository_id > 0),
 producer_revision TEXT NOT NULL CHECK(length(producer_revision) = 40),
 scanned_at TEXT NOT NULL,
 PRIMARY KEY (repository_id, producer_revision)
) STRICT;
CREATE TRIGGER slopbot_value_group_scans_no_update BEFORE UPDATE ON slopbot_value_group_scans
BEGIN SELECT RAISE(ABORT, 'value group scans are immutable'); END;
CREATE TRIGGER slopbot_value_group_scans_no_delete BEFORE DELETE ON slopbot_value_group_scans
BEGIN SELECT RAISE(ABORT, 'value group scans are immutable'); END;
