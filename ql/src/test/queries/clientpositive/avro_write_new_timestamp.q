-- HIVE-25104: Backward incompatible timestamp serialization in Avro for certain timezones
-- Test writing timestamps in Avro tables using old and new date/time APIs
-- using the appropriate configuration properties. 
CREATE TABLE employee(eid INT,birth TIMESTAMP) STORED AS AVRO;
-- Rows written using legacy conversion disabled are not backwards compatible.
-- Reading those rows in older versions of Hive (or other applications) might show
-- the timestamps (1, 2) shifted for some timezones (e.g., US/Pacific).
-- set hive.avro.timestamp.write.legacy.conversion.enabled=false;
INSERT INTO employee VALUES (1, '1220-01-01 00:00:00');
INSERT INTO employee VALUES (2, '1880-01-01 00:00:00');
INSERT INTO employee VALUES (3, '1884-01-01 00:00:00');
INSERT INTO employee VALUES (4, '1990-01-01 00:00:00');
-- No matter how timestamps are written they are always read correctly by the current
-- version of Hive by exploiting the metadata in the file
SELECT eid, birth FROM employee ORDER BY eid;
-- Changing the read property does not have any effect in the current version of Hive
-- since the file metadata contains the appropriate information to read them correctly 
-- set hive.avro.timestamp.legacy.conversion.enabled=false;
SELECT eid, birth FROM employee ORDER BY eid;
-- set hive.avro.timestamp.legacy.conversion.enabled=true;
SELECT eid, birth FROM employee ORDER BY eid;
