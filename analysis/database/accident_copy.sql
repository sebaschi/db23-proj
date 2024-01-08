drop table if exists accident_copy;

create table accident_copy as
    select * from accidents;
alter table accident_copy add severity varchar;
alter table  accident_copy add foreign key (accidentuid) references accidents;
update accident_copy set severity = 'Accident with property damage'
where accidentseveritycategory='as4';

update accident_copy set severity = 'Accident with light injuries'
where accidentseveritycategory='as3';

update accident_copy set severity = 'Accident with severe injuries'
where accidentseveritycategory='as2';

update accident_copy set severity = 'Accidents with fatalities'
where accidentseveritycategory='as1';