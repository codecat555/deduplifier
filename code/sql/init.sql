
DROP TABLE IF EXISTS Location CASCADE;
CREATE TABLE location (
    id INT GENERATED ALWAYS AS IDENTITY,
    host VARCHAR(253) NOT NULL,
    path VARCHAR(1024) NOT NULL,
    PRIMARY KEY(id)
);

DROP TABLE IF EXISTS file CASCADE;
CREATE TABLE file (
    id INT GENERATED ALWAYS AS IDENTITY,
    location_id INT NOT NULL,
    name VARCHAR(1024) NOT NULL,
    create_date DATE NOT NULL,
    modify_date DATE NOT NULL,
    access_date DATE NOT NULL,
    discover_date DATE NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT fk_location FOREIGN KEY(location_id) REFERENCES location(id)
);

DROP TABLE IF EXISTS image_file CASCADE;
CREATE TABLE image_file (
    id INT GENERATED ALWAYS AS IDENTITY,
    file_id INT NOT NULL,
    name VARCHAR(1024) NOT NULL,
    imagehash_fingerprint VARCHAR(1024),
    PRIMARY KEY(id),
    CONSTRAINT fk_file FOREIGN KEY(file_id) REFERENCES file(id)
);
