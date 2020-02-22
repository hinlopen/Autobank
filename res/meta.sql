CREATE TABLE Bijzonderheid (
    attribuut VARCHAR(15),
    waarde VARCHAR(30),
    idf real,
    PRIMARY KEY (attribuut, waarde)
);

CREATE TABLE Dichtheid (
    attribuut VARCHAR(15),
    bandbreedte real,

    PRIMARY KEY (attribuut)
);

CREATE TABLE Frequentie (
    attribuut VARCHAR(15),
    waarde VARCHAR(30),
    qf real,
    
    PRIMARY KEY (attribuut, waarde)
);

CREATE TABLE FrequentieA (
    attribuut VARCHAR(15),
    rqf int,

    PRIMARY KEY (attribuut)
);

CREATE TABLE Similarity (
    attribuut VARCHAR(15),
    term1 VARCHAR(25),
    term2 VARCHAR(25),
    jaccard real,

    PRIMARY KEY (attribuut, term1, term2)
);