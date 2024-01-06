DROP TABLE IF EXISTS fbcount_copy;

CREATE TABLE fbcount_copy AS
    SELECT * FROM footbikecount;

ALTER TABLE fbcount_copy ADD fuss_total INTEGER;
UPDATE fbcount_copy SET fuss_total = fuss_in + fuss_out;

ALTER TABLE fbcount_copy
    DROP COLUMN IF EXISTS fuss_in,
    DROP COLUMN IF EXISTS fuss_out,
    ADD PRIMARY KEY (id);

ALTER TABLE fbcount_copy ADD velo_total INTEGER;
UPDATE fbcount_copy SET velo_total = velo_in + velo_out;

ALTER TABLE fbcount_copy
DROP COLUMN velo_in,
DROP COLUMN velo_out;

select count(*), accidentyear from accidents
group by accidentyear
order by accidentyear;

SELECT COUNT(*), accidentweekday_en
FROM accidents
GROUP BY accidentweekday_en
ORDER BY COUNT(*);

SELECT accidentyear AS year, accidentweekday_en AS weekday, COUNT(*) AS count
FROM accidents
GROUP BY weekday, year
ORDER BY year, COUNT(*);

select distinct msid from mivcount;

    SELECT accidentyear AS year, accidentweekday_en AS weekday, COUNT(*) AS count
    FROM accidents
    GROUP BY year, weekday
    ORDER BY year, weekday;

SELECT accidentyear AS year, accidentmonth AS month, count() as count
FROM accidents
GROUP BY year, month;

SELECT accidentyear as year, accidentmonth as month, count(*) as count
from accidents
where accidentinvolvingpedestrian=True
group by month, year
order by year, month;

drop table if exists accident_copy;

create table accident_copy as
    select * from accidents;
alter table accident_copy add severity varchar;
update accident_copy set severity = 'Accident with property damage'
where accidentseveritycategory='as4';

update accident_copy set severity = 'Accident with light injuries'
where accidentseveritycategory='as3';

update accident_copy set severity = 'Accident with severe injuries'
where accidentseveritycategory='as2';

update accident_copy set severity = 'Accidents with fatalities'
where accidentseveritycategory='as1';


    SELECT accidentyear AS year, accidentmonth AS month, accidentinvolvingpedestrian AS ped,
     accidentinvolvingbicycle as bike,
     accidentinvolvingmotorcycle as moto,count(*) as count
    FROM accidents
    GROUP BY year, month, ped, bike, moto
    ORDER BY year, month;


