drop database if exists deduplifier4;
create database deduplifier4 with template deduplifier3;
drop database if exists deduplifier3;
create database deduplifier3 with template deduplifier2;
drop database if exists deduplifier2;
create database deduplifier2 with template deduplifier1;
drop database if exists deduplifier1;
create database deduplifier1 with template deduplifier0;
drop database if exists deduplifier0;
create database deduplifier0 with template deduplifier;
