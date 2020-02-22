CREATE TABLE IDF (
    attribuut VARCHAR(15),
    waarde VARCHAR(30),
    idf real,
    PRIMARY KEY (attribuut, waarde)
);

CREATE TABLE Hidf (
    attribuut VARCHAR(15),
    bandbreedte real,

    PRIMARY KEY (attribuut)
);

CREATE TABLE QF (
    attribuut VARCHAR(15),
    waarde VARCHAR(30),
    qf real,
    
    PRIMARY KEY (attribuut, waarde)
);

CREATE TABLE Hqf (
    attribuut VARCHAR(15),
    bandbreedte real,

    PRIMARY KEY (attribuut)
);

CREATE TABLE FrequentieA (
    attribuut VARCHAR(15),
    rqf int,

    PRIMARY KEY (attribuut)
);

CREATE TABLE Jaccard (
    attribuut VARCHAR(15),
    term1 VARCHAR(25),
    term2 VARCHAR(25),
    jaccard real,

    PRIMARY KEY (attribuut, term1, term2)
);