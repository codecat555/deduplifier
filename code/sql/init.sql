
DROP TABLE IF EXISTS Location CASCADE;
CREATE TABLE location (
    id INT GENERATED ALWAYS AS IDENTITY,
    url VARCHAR(1024) NOT NULL,
    PRIMARY KEY(id)
);

DROP TABLE IF EXISTS file CASCADE;
CREATE TABLE file (
    id INT GENERATED ALWAYS AS IDENTITY,
    location_id INT NOT NULL,
    name VARCHAR(1024) NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT fk_location FOREIGN KEY(location_id) REFERENCES location(id)
);

