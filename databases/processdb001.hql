# processdb.hql - the data vault for storing attributes keys and relationships
# also the first code chunk for hubs links and satellites
--#######################################################################################################################################
--#######################################################################################################################################
--#######################################################################################################################################
--#######################################################################################################################################
CREATE DATABASE IF NOT EXISTS processdb;
USE processdb;
  
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.personhub (
  id         INT,
  keyid      STRING,
  firstname  STRING,
  lastname   STRING
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.personhub001 (
  firstname  STRING,
  lastname   STRING
)
CLUSTERED BY (firstname, lastname) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.personhub001;

INSERT INTO TABLE processdb.personhub001 
SELECT DISTINCT
  firstname, 
  lastname 
FROM 
  assessdb.personfull;

 
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.personhub002 (
  rid         BIGINT,
  tid         BIGINT,
  firstname   STRING,
  lastname    STRING
)
CLUSTERED BY (rid, tid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.personhub002; 

INSERT INTO TABLE processdb.personhub002
SELECT 
  ROW_NUMBER() OVER (ORDER BY firstname, lastname),
  unix_timestamp(), 
  firstname, 
  lastname 
FROM 
  processdb.personhub001;
  
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.personhub003 (
  keyid      STRING,
  firstname  STRING,
  lastname   STRING
)
CLUSTERED BY (keyid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.personhub003;

INSERT INTO TABLE processdb.personhub003
SELECT 
  CONCAT(tid, '/', rid), 
  firstname, 
  lastname 
FROM 
  processdb.personhub002;
  
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.personhub004 (
  keyid      STRING,
  firstname  STRING,
  lastname   STRING,
  CDC        STRING
)
CLUSTERED BY (keyid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.personhub004;

INSERT INTO TABLE processdb.personhub004
SELECT 
  A.keyid, 
  A.firstname, 
  A.lastname,
  B.keyid
FROM 
  processdb.personhub003 AS A
LEFT JOIN
  processdb.personhub AS B
ON
  A.firstname = B.firstname  AND A.lastname = B.lastname;
 
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.personhub005 (
  keyid      STRING,
  firstname  STRING,
  lastname   STRING
)
CLUSTERED BY (keyid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.personhub005;

INSERT INTO TABLE processdb.personhub005
SELECT 
  keyid, 
  firstname, 
  lastname 
FROM 
  processdb.personhub004
WHERE CDC IS NULL;

INSERT INTO TABLE processdb.personhub005
SELECT 
  keyid, 
  firstname, 
  lastname 
FROM 
  processdb.personhub;


--#######################################################################################################################################

TRUNCATE TABLE processdb.personhub;

INSERT INTO TABLE processdb.personhub
SELECT 
  ROW_NUMBER() OVER (ORDER BY keyid),
  keyid, 
  firstname, 
  lastname 
FROM 
  processdb.personhub005;
     
--#######################################################################################################################################   
--#######################################################################################################################################  
DROP TABLE processdb.personhub001; 
DROP TABLE processdb.personhub002; 
DROP TABLE processdb.personhub003; 
DROP TABLE processdb.personhub004;

--#######################################################################################################################################   
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.personsexsatellite001 (
  keyid      STRING,
  sex        STRING
)
CLUSTERED BY (keyid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.personsexsatellite001;

INSERT INTO TABLE processdb.personsexsatellite001
SELECT DISTINCT
  A.keyid, 
  B.sex 
FROM 
  processdb.personhub005 as A
JOIN  
  assessdb.personfull AS B
ON
  A.firstname = B.firstname AND A.lastname = B.lastname;
   
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.personsexsatellite (
  id         INT,
  keyid      STRING,
  sex        STRING,
  timestmp   BIGINT
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.personsexsatellite;

INSERT INTO TABLE processdb.personsexsatellite
SELECT 
  ROW_NUMBER() OVER (ORDER BY keyid),
  keyid, 
  sex,
  unix_timestamp()
FROM 
  processdb.personsexsatellite001;
   
--#######################################################################################################################################   
--#######################################################################################################################################
DROP TABLE processdb.objecthub001;
DROP TABLE processdb.personsexsatellite001;
  
--#######################################################################################################################################

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.objecthub (
  id          int,
  objecttype  string,
  objectname  string,
  objectid    int
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
   
--#######################################################################################################################################
TRUNCATE TABLE processdb.objecthub;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.objecthub001 (  
  objecttype  string,
  objectname  string,
  objectid    int
)
CLUSTERED BY (objecttype, objectname,objectid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
   
--#######################################################################################################################################
TRUNCATE TABLE processdb.objecthub001;
   
--#######################################################################################################################################
INSERT INTO TABLE processdb.objecthub001 
SELECT DISTINCT
  'intangible', 
  'bankaccount',
  accountid 
FROM 
  assessdb.account;

--#######################################################################################################################################
TRUNCATE TABLE processdb.objecthub;

INSERT INTO TABLE processdb.objecthub 
SELECT DISTINCT
  ROW_NUMBER() OVER (ORDER BY objecttype,objectname,objectid),
  objecttype, 
  objectname,
  objectid 
FROM 
  processdb.objecthub001;
     
--#######################################################################################################################################   
--#######################################################################################################################################  
CREATE TABLE IF NOT EXISTS processdb.objectbankaccountsatellite0001 (
  accountid           int,
  transactionid       int,
  balance             DECIMAL(18, 9)
)
CLUSTERED BY (accountid,transactionid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
   
--#######################################################################################################################################
TRUNCATE TABLE processdb.objectbankaccountsatellite0001;
   
--#######################################################################################################################################
INSERT INTO TABLE processdb.objectbankaccountsatellite0001
SELECT 
  accountid, 
  id as transactionid,
  balance 
FROM 
  assessdb.account;
   
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.objectbankaccountsatellite (
  id                  int,
  accountid           int,
  transactionid       int,
  balance             DECIMAL(18, 9),
  timestmp            bigint
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
   
--#######################################################################################################################################
TRUNCATE TABLE processdb.objectbankaccountsatellite;
   
--#######################################################################################################################################
INSERT INTO TABLE processdb.objectbankaccountsatellite
SELECT 
  ROW_NUMBER() OVER (ORDER BY accountid,transactionid),
  accountid, 
  transactionid,
  balance,
  unix_timestamp()
FROM 
  processdb.objectbankaccountsatellite0001;
   
--#######################################################################################################################################   
--#######################################################################################################################################
DROP TABLE processdb.objectbankaccountsatellite0001;
DROP TABLE processdb.objecthub001;

CREATE TABLE IF NOT EXISTS processdb.locationhub (
  id            INT,
  locationtype  STRING,
  locationname  STRING,
  locationid    INT
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.locationhub;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.locationhub001 (  
  locationtype  STRING,
  locationname  STRING,
  locationid    INT
)
CLUSTERED BY (locationtype, locationname,locationid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.locationhub001;

INSERT INTO TABLE processdb.locationhub001 
SELECT DISTINCT
  'intangible', 
  'geospace',
  id as locationid 
FROM 
  assessdb.postaddress;

--#######################################################################################################################################

INSERT INTO TABLE processdb.locationhub 
SELECT DISTINCT
  ROW_NUMBER() OVER (ORDER BY locationtype,locationname,locationid),
  locationtype, 
  locationname,
  locationid 
FROM 
  processdb.locationhub001;
     
--#######################################################################################################################################   
--#######################################################################################################################################  
CREATE TABLE IF NOT EXISTS processdb.locationgeospacesatellite0001 (
  locationid    INT,
  postcode      STRING,
  latitude      DECIMAL(18, 9),
  longitude     DECIMAL(18, 9),
  easting       INT,
  northing      INT,
  gridref       STRING,
  district      STRING,
  ward          STRING,
  districtcode  STRING,
  wardcode      STRING,
  country       STRING,
  countycode    STRING,
  constituency  STRING,
  typearea      STRING
)
CLUSTERED BY (locationid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.locationgeospacesatellite0001;

INSERT INTO TABLE processdb.locationgeospacesatellite0001
SELECT
  id as locationid,
  postcode,
  latitude,
  longitude,
  easting,
  northing,
  gridref,
  district,
  ward,
  districtcode,
  wardcode,
  country,
  countycode,
  constituency,
  typearea
FROM 
  assessdb.postaddress;
 
--#######################################################################################################################################

CREATE TABLE IF NOT EXISTS processdb.locationgeospace1satellite (
  id            INT,
  locationid    INT,
  postcode      STRING,
  timestmp      BIGINT
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.locationgeospace1satellite;

INSERT INTO TABLE processdb.locationgeospace1satellite
SELECT 
  ROW_NUMBER() OVER (ORDER BY locationid),
  locationid,
  postcode,
  unix_timestamp()
FROM 
  processdb.locationgeospacesatellite0001
ORDER BY locationid;
 
--#######################################################################################################################################

CREATE TABLE IF NOT EXISTS processdb.locationgeospace2satellite (
  id            INT,
  locationid    INT,
  latitude      DECIMAL(18, 9),
  longitude     DECIMAL(18, 9),
  timestmp      BIGINT
)
CLUSTERED BY (id, locationid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.locationgeospace2satellite;

INSERT INTO TABLE processdb.locationgeospace2satellite
SELECT 
  ROW_NUMBER() OVER (ORDER BY locationid),
  locationid,
  latitude,
  longitude,
  unix_timestamp()
FROM 
  processdb.locationgeospacesatellite0001;

 
--#######################################################################################################################################

CREATE TABLE IF NOT EXISTS processdb.locationgeospace3satellite (
  id            INT,
  locationid    INT,
  easting       INT,
  northing      INT,
  timestmp      BIGINT
)
CLUSTERED BY (id, locationid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.locationgeospace3satellite;

INSERT INTO TABLE processdb.locationgeospace3satellite
SELECT 
  ROW_NUMBER() OVER (ORDER BY locationid),
  locationid,
  easting,
  northing,
  unix_timestamp()
FROM 
  processdb.locationgeospacesatellite0001;  
--#######################################################################################################################################

CREATE TABLE IF NOT EXISTS processdb.locationgeospace4satellite (
  id            INT,
  locationid    INT,
  postcode      STRING,
  latitude      DECIMAL(18, 9),
  longitude     DECIMAL(18, 9),
  easting       INT,
  northing      INT,
  gridref       STRING,
  district      STRING,
  ward          STRING,
  districtcode  STRING,
  wardcode      STRING,
  country       STRING,
  countycode    STRING,
  constituency  STRING,
  typearea      STRING,
  timestmp      BIGINT
)
CLUSTERED BY (id, locationid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.locationgeospace4satellite;

INSERT INTO TABLE processdb.locationgeospace4satellite
SELECT 
  ROW_NUMBER() OVER (ORDER BY locationid),
  locationid,
  postcode,
  latitude,
  longitude,
  easting,
  northing,
  gridref,
  district,
  ward,
  districtcode,
  wardcode,
  country,
  countycode,
  constituency,
  typearea,
  unix_timestamp()
FROM 
  processdb.locationgeospacesatellite0001;  
--#######################################################################################################################################   
--#######################################################################################################################################
DROP TABLE processdb.locationgeospacesatellite0001;
DROP TABLE processdb.locationhub001;

CREATE TABLE IF NOT EXISTS processdb.eventhub (
  id          int,
  eventtype  string,
  eventname  string,
  eventid    int
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.eventhub;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.eventhub001 (  
  eventtype  string,
  eventname  string,
  eventid    int
)
CLUSTERED BY (eventtype, eventname,eventid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.eventhub001;

INSERT INTO TABLE processdb.eventhub001 
SELECT DISTINCT
  'intangible', 
  'banktransaction',
  id as eventid 
FROM 
  assessdb.account;

--#######################################################################################################################################

INSERT INTO TABLE processdb.eventhub 
SELECT DISTINCT
  ROW_NUMBER() OVER (ORDER BY eventtype,eventname,eventid),
  eventtype, 
  eventname,
  eventid 
FROM 
  processdb.eventhub001;
     
--#######################################################################################################################################   
--#######################################################################################################################################  
CREATE TABLE IF NOT EXISTS processdb.eventbanktransactionsatellite0001 (
  accountid           int,
  transactionid       int,
  balance             DECIMAL(18, 9)
)
CLUSTERED BY (accountid,transactionid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.eventbanktransactionsatellite0001;

INSERT INTO TABLE processdb.eventbanktransactionsatellite0001
SELECT 
  accountid, 
  id as transactionid,
  balance 
FROM 
  assessdb.account;
   
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.eventbanktransactionsatellite (
  id                  int,
  accountid           int,
  transactionid       int,
  balance             DECIMAL(18, 9),
  timestmp            bigint
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.eventbanktransactionsatellite;

INSERT INTO TABLE processdb.eventbanktransactionsatellite
SELECT 
  ROW_NUMBER() OVER (ORDER BY accountid,transactionid),
  accountid, 
  transactionid,
  balance,
  unix_timestamp()
FROM 
  processdb.eventbanktransactionsatellite0001;
   
--#######################################################################################################################################   
--#######################################################################################################################################
DROP TABLE processdb.eventbanktransactionsatellite0001;
DROP TABLE processdb.eventhub001;

CREATE TABLE IF NOT EXISTS processdb.timehub (
  id            INT,
  timeid    INT
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.timehub;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS processdb.timehub001 (
  timeid    INT
)
CLUSTERED BY (timeid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE processdb.timehub001;

INSERT INTO TABLE processdb.timehub001 
SELECT DISTINCT
  id as timeid 
FROM 
  assessdb.dates
WHERE yearnumber = 2015;

INSERT INTO TABLE processdb.timehub001 
SELECT DISTINCT
  id as timeid 
FROM 
  assessdb.dates
WHERE yearnumber = 2016;

--#######################################################################################################################################
TRUNCATE TABLE processdb.timehub;

INSERT INTO TABLE processdb.timehub 
SELECT DISTINCT
  ROW_NUMBER() OVER (ORDER BY timeid),
  timeid 
FROM 
  processdb.timehub001;
     
--#######################################################################################################################################   
--#######################################################################################################################################  
CREATE TABLE IF NOT EXISTS processdb.timesatellite0001 (
  timeid        INT,
  datetimes     string
)
CLUSTERED BY (timeid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

--#######################################################################################################################################
TRUNCATE TABLE processdb.timesatellite0001;

--#######################################################################################################################################
INSERT INTO TABLE processdb.timesatellite0001
SELECT
  id as timeid,
  datetimes
FROM 
  assessdb.dates
WHERE yearnumber = 2015;

--#######
INSERT INTO TABLE processdb.timesatellite0001
SELECT
  id as timeid,
  datetimes
FROM 
  assessdb.dates
WHERE yearnumber = 2016;

--#######################################################################################################################################

CREATE TABLE IF NOT EXISTS processdb.time1satellite (
  id            INT,
  timeid        INT,
  datetimes     STRING,
  timestmp      BIGINT
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

--#######################################################################################################################################
TRUNCATE TABLE processdb.time1satellite;

--#######################################################################################################################################
INSERT INTO TABLE processdb.time1satellite
SELECT 
  ROW_NUMBER() OVER (ORDER BY timeid),
  timeid,
  datetimes,
  unix_timestamp()
FROM 
  processdb.timesatellite0001
ORDER BY timeid; 

--#######################################################################################################################################
DROP TABLE processdb.timesatellite0001;
DROP TABLE processdb.timehub001;