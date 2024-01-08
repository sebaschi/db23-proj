DROP TABLE IF EXISTS fbcount_copy;

CREATE TABLE fbcount_copy AS
    SELECT * FROM footbikecount;

ALTER TABLE fbcount_copy ADD fuss_total INTEGER;
UPDATE fbcount_copy SET fuss_total = fuss_in + fuss_out;



ALTER TABLE fbcount_copy
    DROP COLUMN IF EXISTS fuss_in,
    DROP COLUMN IF EXISTS fuss_out,
    ADD FOREIGN KEY (id) REFERENCES footbikecount;

ALTER TABLE fbcount_copy ADD velo_total INTEGER;
UPDATE fbcount_copy SET velo_total = velo_in + velo_out;

ALTER TABLE fbcount_copy
DROP COLUMN IF EXISTS velo_in,
DROP COLUMN IF EXISTS velo_out;