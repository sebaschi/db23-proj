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