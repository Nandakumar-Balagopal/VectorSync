-- Which materialization currently serves a source table.
--
-- Until now nothing recorded it. Two configurations over one table could both be LIVE, both
-- publishing their own Tier-2 projection, and nothing said which one a reader should use -- so a
-- model migration had no cutover step and callers had to know a config id out of band.
--
-- A partial unique index rather than a uniqueness constraint on the column: it has to allow many
-- rows with serving = false and exactly one with true per source table, which a plain UNIQUE
-- cannot express. Enforced in the database rather than in the service because promotion is the one
-- operation where two concurrent callers would otherwise both believe they won.
ALTER TABLE materializations
    ADD COLUMN serving boolean NOT NULL DEFAULT false;

CREATE UNIQUE INDEX uk_materializations_serving
    ON materializations (source_table)
    WHERE serving;

COMMENT ON COLUMN materializations.serving IS
    'True for the one materialization a reader should query for this source table.';
