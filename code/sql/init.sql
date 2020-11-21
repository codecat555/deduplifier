
DROP DATABASE IF EXISTS deduplifier;
CREATE DATABASE deduplifier;
CONNECT TO deduplifier;

--DROP TABLE IF EXISTS host CASCADE;
CREATE TABLE host (
    id INT GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(253) NOT NULL,
    PRIMARY KEY(id)
);

--DROP TABLE IF EXISTS drive CASCADE;
CREATE TABLE drive (
    id INT GENERATED ALWAYS AS IDENTITY,
    serialno VARCHAR(64) NOT NULL,
    PRIMARY KEY(id)
);

--DROP TABLE IF EXISTS volume CASCADE;
CREATE TABLE volume (
    id INT GENERATED ALWAYS AS IDENTITY,
    uuid VARCHAR(40) NOT NULL,
    PRIMARY KEY(id)
);

--DROP TABLE IF EXISTS path CASCADE;
CREATE TABLE path (
    id INT GENERATED ALWAYS AS IDENTITY,
    parent_path_id INT,
    path VARCHAR(40) NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT fk_parent_path FOREIGN KEY(parent_path_id) REFERENCES path(id)
);

--DROP TABLE IF EXISTS location CASCADE;
CREATE TABLE location (
    id INT GENERATED ALWAYS AS IDENTITY,
    host_id INT NOT NULL,
    drive_id INT NOT NULL,
    volume_id INT NOT NULL,
    path_id INT NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT fk_host FOREIGN KEY(host_id) REFERENCES host(id),
    CONSTRAINT fk_drive FOREIGN KEY(drive_id) REFERENCES drive(id),
    CONSTRAINT fk_volume FOREIGN KEY(volume_id) REFERENCES volume(id),
    CONSTRAINT fk_path FOREIGN KEY(path_id) REFERENCES path(id)
);

--DROP TABLE IF EXISTS file CASCADE;
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

--DROP TABLE IF EXISTS image_file CASCADE;
CREATE TABLE image_file (
    file_id INT NOT NULL,
    image_id INT NOT NULL,
    CONSTRAINT fk_file FOREIGN KEY(file_id) REFERENCES file(id)
    CONSTRAINT fk_image FOREIGN KEY(image_id) REFERENCES image(id)
);

--DROP TABLE IF EXISTS image CASCADE;
CREATE TABLE image (
    id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY
    name VARCHAR(1024) NOT NULL,
    imagehash_fingerprint VARCHAR(1024)

);
