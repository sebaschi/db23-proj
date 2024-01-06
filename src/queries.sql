select p.id, a.accidentuid, m.id
from footbikecount p, accidents a, mivcount m
where p.weekday_en = a.accidentweekday_en AND a.accidentweekday_en = m.weekday_en
AND p.weekday_en = m.weekday_en AND p.hrs = a.accidenthour AND a.accidenthour = m.hrs
AND p.hrs = m.hrs AND (p.ost - m.ekoord between -100 AND 100) AND (p.nord - m.nkoord between -100 AND 100);

DROP TABLE IF EXISTS Contemporaneous2;

CREATE TABLE Contemporaneous2 (
    p_id INTEGER,
    accidentuid VARCHAR(256),
    m_id INTEGER,
    weekday_en VARCHAR(10),
    hrs INTEGER,
    distance DOUBLE PRECISION
);


CREATE TABLE Intermediate2 AS
SELECT
    p.id AS p_id,
    a.accidentuid,
    m.id AS m_id,
    p.weekday_en,
    p.hrs,
    SQRT(POWER(p.ost - m.ekoord, 2) + POWER(p.nord - m.nkoord, 2)) AS distance
FROM
    footbikecount p,
    accidents a,
    mivcount m
WHERE
    p.weekday_en = a.accidentweekday_en
    AND a.accidentweekday_en = m.weekday_en
    AND p.weekday_en = m.weekday_en
    AND p.hrs = a.accidenthour
    AND a.accidenthour = m.hrs
    AND p.hrs = m.hrs
    AND (p.ost - m.ekoord BETWEEN -100 AND 100)
    AND (p.nord - m.nkoord BETWEEN -100 AND 100);

INSERT INTO Contemporaneous2 (p_id, accidentuid, m_id, weekday_en, hrs, distance)
SELECT p_id, accidentuid, m_id, weekday_en, hrs, distance FROM Intermediate2;
