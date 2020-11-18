
DROP TABLE IF EXISTS Location;
DROP TABLE IF EXISTS file;

CREATE TABLE location (
    location_id INT GENERATED ALWAYS AS IDENTITY,
    url VARCHAR(1024) NOT NULL,
    PRIMARY KEY(location_id)
);

CREATE SEQUENCE seq_location;

CREATE TABLE file (
    file_id INT GENERATED ALWAYS AS IDENTITY,
    location_id INT NOT NULL,
    name VARCHAR(1024) NOT NULL,
    PRIMARY KEY(file_id)
    CONSTRAINT fk_location FOREIGN KEY(location_id) REFERENCES location(id)
);

CREATE SEQUENCE seq_file;

