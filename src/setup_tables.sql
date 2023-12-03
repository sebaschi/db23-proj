CREATE EXTENSION IF NOT EXISTS postgis;

DROP TABLE IF EXISTS FootBikeCount;

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

DROP TABLE IF EXISTS MivCount;

CREATE TABLE MivCount (
    MSID VARCHAR(256) ,
    ZSID VARCHAR(256) NULL,
    Achse VARCHAR(256) ,
    NKoord INTEGER ,
    EKoord INTEGER ,
    Richtung VARCHAR(100) ,
    AnzFahrzeuge INTEGER ,
    AnzFahrzeugeStatus VARCHAR(20) ,
    Datum VARCHAR(10) ,
    Hrs Integer ,
    Weekday_en VARCHAR(10),
    MessungDatZeit VARCHAR(100),
    PRIMARY KEY (MSID, Achse,Richtung, AnzFahrzeuge, Datum, Hrs),
    CHECK (Weekday_en IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday')),
    CHECK (Hrs BETWEEN 0 AND 23)
);


DROP TABLE IF EXISTS Accidents;

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
    Geometry geometry(Point) ,

    PRIMARY KEY (AccidentUID) ,
    CHECK ( AccidentHour BETWEEN 0 AND 23) ,
    CHECK (AccidentWeekDay_en IN ('Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'))
);

COPY FootBikeCount FROM '/Users/seb/Projects/repos/group-1/src/datasets/integrated/FootBikeCount.csv'
    DELIMITER ','
    CSV HEADER;

COPY MivCount FROM '/Users/seb/Projects/repos/group-1/src/datasets/integrated/MivCount.csv'
    DELIMITER ','
    CSV HEADER;