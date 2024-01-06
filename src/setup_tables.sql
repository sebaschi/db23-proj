CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS FootBikeCount;

DROP TABLE IF EXISTS Accidents;

DROP TABLE IF EXISTS MivCount;

drop table if exists signaled_speeds;


CREATE TABLE FootBikeCount (
    ID INTEGER ,
    NORD INTEGER ,
    OST INT ,
    DATE VARCHAR(10) ,
    HRS INTEGER ,
    VELO_IN INTEGER ,
    VELO_OUT INTEGER ,
    FUSS_IN INTEGER ,
    FUSS_OUT INTEGER ,
    Weekday_en VARCHAR(10) ,

    PRIMARY KEY(ID) ,
    CHECK (Weekday_en IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')),
    CHECK (Hrs BETWEEN 0 AND 23)


);



CREATE TABLE MivCount (
    ID INTEGER ,
    MSID VARCHAR(10) ,
    ZSID VARCHAR(10) ,
    Achse VARCHAR(256) ,
    NKoord INTEGER ,
    EKoord INTEGER ,
    Richtung VARCHAR(100) ,
    AnzFahrzeuge INTEGER ,
    AnzFahrzeugeStatus VARCHAR(20) ,
    Datum VARCHAR(10) ,
    Hrs Integer ,
    Weekday_en VARCHAR(10),

    PRIMARY KEY (ID),
    CHECK (Weekday_en IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')),
    CHECK (Hrs BETWEEN 0 AND 23)
);


CREATE TABLE Accidents (
    AccidentUID VARCHAR(256) ,
    AccidentYear INTEGER ,
    AccidentMonth INTEGER,
    AccidentWeekDay_en VARCHAR(10) ,
    AccidentHour INTEGER ,
    NKoord INTEGER ,
    EKoord INTEGER ,
    AccidentType_en VARCHAR(256) ,
    AccidentType VARCHAR(4) ,
    AccidentSeverityCategory VARCHAR(4) ,
    AccidentInvolvingPedestrian BOOLEAN ,
    AccidentInvolvingBicycle BOOLEAN ,
    AccidentInvolvingMotorcycle BOOLEAN ,
    RoadType VARCHAR(5) ,
    RoadType_en VARCHAR(256) ,
    Geometry geometry(Point, 4326) ,

    PRIMARY KEY (AccidentUID) ,
    CHECK ( AccidentHour BETWEEN 0 AND 23) ,
    CHECK (AccidentWeekDay_en IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'))
);
-- Ad hoc via generate sql functionality in pycharm
create table signaled_speeds
(
    id                    serial
        primary key,
    ausnahmen_fahrverbot  varchar,
    fahrverbot_ssv        varchar,
    lokalisationsname     varchar,
    objectid              double precision,
    publiziert_vsi_datum  timestamp with time zone,
    rechtskraeftig_datum  timestamp with time zone,
    temporegime           varchar,
    temporegime_technical varchar,
    umgesetzt_datum       timestamp with time zone,
    wkb_geometry          geometry(Point, 4326) --changed from MultiLineString
);