# assessdb.hql to validate raw data from the retrievedb
--#######################################################################################################################################
--#######################################################################################################################################
--#######################################################################################################################################

--#######################################################################################################################################
CREATE DATABASE IF NOT EXISTS assessdb;
USE assessdb;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.firstname001 (
  firstnameid    string,
  firstname      string,
  sex            string
)
CLUSTERED BY (firstnameid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.firstname001;

INSERT INTO TABLE assessdb.firstname001 
SELECT firstnameid, firstname, sex 
FROM retrievedb.rawfirstname 
WHERE firstnameid <> '"id"';

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.firstname002 (
  firstnameid    string,
  firstname      string,
  sex            string
)
CLUSTERED BY (firstnameid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.firstname002;

INSERT INTO TABLE assessdb.firstname002 
SELECT firstnameid, rtrim(ltrim(firstname)), rtrim(ltrim(sex)) 
FROM assessdb.firstname001;


--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.firstname003 (
  firstnameid    int,
  firstname      string,
  sex            string
)
CLUSTERED BY (firstnameid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.firstname003;

INSERT INTO TABLE assessdb.firstname003 
SELECT CAST(firstnameid as INT), SUBSTRING(firstname,2,LENGTH(firstname)-2), SUBSTRING(sex,2,LENGTH(sex)-2) 
FROM assessdb.firstname002;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.firstname (
  firstnameid    int,
  firstname      string,
  sex            string
)
CLUSTERED BY (firstnameid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.firstname;

INSERT INTO TABLE assessdb.firstname 
SELECT firstnameid, firstname, sex 
FROM assessdb.firstname003 
ORDER BY firstnameid;

TRUNCATE TABLE assessdb.firstname;

INSERT INTO TABLE assessdb.firstname 
SELECT firstnameid, firstname, sex 
FROM assessdb.firstname003 
SORT BY firstnameid;

SELECT firstnameid, firstname, sex from assessdb.firstname SORT BY firstname LIMIT 10;

--#######################################################################################################################################
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.lastname001 (
  lastnameid    string,
  lastname      string
)
CLUSTERED BY (lastnameid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.lastname001;

INSERT INTO TABLE assessdb.lastname001 
SELECT lastnameid, lastname 
FROM retrievedb.rawlastname 
WHERE lastnameid <> '"id"';

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.lastname002 (
  lastnameid    string,
  lastname      string
)
CLUSTERED BY (lastnameid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.lastname002;

INSERT INTO TABLE assessdb.lastname002 
SELECT lastnameid, rtrim(ltrim(lastname)) 
FROM assessdb.lastname001;


--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.lastname003 (
  lastnameid    int,
  lastname      string
)
CLUSTERED BY (lastnameid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.lastname003;

INSERT INTO TABLE assessdb.lastname003 
SELECT CAST(lastnameid as INT), SUBSTRING(lastname,2,LENGTH(lastname)-2) 
FROM assessdb.lastname002;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.lastname (
  lastnameid    int,
  lastname      string
)
CLUSTERED BY (lastnameid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.lastname;

INSERT INTO TABLE assessdb.lastname 
SELECT lastnameid, lastname 
FROM assessdb.lastname003 
ORDER BY lastnameid;

--#######################################################################################################################################
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.person001 (
  persid         string,
  firstnameid    string,
  lastnameid     string
)
CLUSTERED BY (persid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.person001;

INSERT INTO TABLE assessdb.person001 
SELECT persid, firstnameid, lastnameid 
FROM retrievedb.rawperson 
WHERE persid <> '"id"';

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.person002 (
  persid         int,
  firstnameid    int,
  lastnameid     int
)
CLUSTERED BY (persid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.person002;

INSERT INTO TABLE assessdb.person002 
SELECT CAST(persid as INT), CAST(firstnameid as INT), CAST(lastnameid as INT) 
FROM assessdb.person001;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.person (
  persid         int,
  firstnameid    int,
  lastnameid     int
)
CLUSTERED BY (persid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.person;

INSERT INTO TABLE assessdb.person 
SELECT persid, firstnameid, lastnameid 
FROM assessdb.person002;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.personfull(
  persid       int,
  firstnameid  int,
  firstname    string,
  lastnameid   int,
  lastname     string,
  sex          string
)
CLUSTERED BY (persid) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.personfull;

INSERT INTO TABLE assessdb.personfull
SELECT person.persid, person.firstnameid, firstname.firstname, person.lastnameid, lastname.lastname, firstname.sex
FROM assessdb.firstname 
JOIN assessdb.person
ON firstname.firstnameid = person.firstnameid
JOIN assessdb.lastname 
ON lastname.lastnameid = person.lastnameid;

--#######################################################################################################################################
--#######################################################################################################################################
DROP TABLE assessdb.firstname001;
DROP TABLE assessdb.firstname002;
DROP TABLE assessdb.firstname003;
DROP TABLE assessdb.lastname001;
DROP TABLE assessdb.lastname002;
DROP TABLE assessdb.lastname003;
DROP TABLE assessdb.person001;
DROP TABLE assessdb.person002;

--#######################################################################################################################################
--#######################################################################################################################################
--#######################################################################################################################################

--#######################################################################################################################################

CREATE TABLE IF NOT EXISTS assessdb.datetime001 (
  id            string,
  datetimes     string,
  monthname     string,
  yearnumber    string,
  monthnumber   string,
  daynumber     string,
  hournumber    string,
  minutenumber  string,
  ampm          string
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.datetime001;

INSERT INTO TABLE assessdb.datetime001 
SELECT
  id,
  datetimes,
  monthname,
  yearnumber,
  monthnumber,
  daynumber,
  hournumber,
  minutenumber,
  ampm
FROM retrievedb.rawdatetime 
WHERE id <> '"id"';


--#######################################################################################################################################

CREATE TABLE IF NOT EXISTS assessdb.datetime002 (
  id            string,
  datetimes     string,
  monthname     string,
  yearnumber    string,
  monthnumber   string,
  daynumber     string,
  hournumber    string,
  minutenumber  string,
  ampm          string
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.datetime002;

INSERT INTO TABLE assessdb.datetime002 
SELECT
  id,
  rtrim(ltrim(datetimes)),
  rtrim(ltrim(monthname)),
  rtrim(ltrim(yearnumber)),
  rtrim(ltrim(monthnumber)),
  rtrim(ltrim(daynumber)),
  rtrim(ltrim(hournumber)),
  rtrim(ltrim(minutenumber)),
  rtrim(ltrim(ampm))
FROM assessdb.datetime001;

--#######################################################################################################################################

CREATE TABLE IF NOT EXISTS assessdb.datetime003 (
  id            int,
  datetimes     string,
  monthname     string,
  yearnumber    int,
  monthnumber   int,
  daynumber     int,
  hournumber    int,
  minutenumber  int,
  ampm          string
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.datetime003;

INSERT INTO TABLE assessdb.datetime003 
SELECT
  CAST(id as INT),
  SUBSTRING(datetimes,2,LENGTH(datetimes)-2),
  SUBSTRING(monthname,2,LENGTH(monthname)-2),
  CAST(yearnumber as INT),
  CAST(monthnumber as INT),
  CAST(daynumber as INT),
  CAST(hournumber as INT),
  CAST(minutenumber as INT),
  SUBSTRING(ampm,2,LENGTH(ampm)-2)
FROM assessdb.datetime002;


--#######################################################################################################################################

CREATE TABLE IF NOT EXISTS assessdb.dates (
  id            int,
  datetimes     string,
  monthname     string,
  yearnumber    int,
  monthnumber   int,
  daynumber     int,
  hournumber    int,
  minutenumber  int,
  ampm          string
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.dates;

INSERT INTO TABLE assessdb.dates
SELECT
  id,
  datetimes,
  monthname,
  yearnumber,
  monthnumber,
  daynumber,
  hournumber,
  minutenumber,
  ampm
FROM assessdb.datetime003;

--#######################################################################################################################################
DROP TABLE assessdb.datetime001;
DROP TABLE assessdb.datetime002;
DROP TABLE assessdb.datetime003;

--#######################################################################################################################################
--#######################################################################################################################################
--#######################################################################################################################################

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.address001 (
  id            STRING,
  postcode      STRING,
  latitude      STRING,
  longitude     STRING,
  easting       STRING,
  northing      STRING,
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
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.address001;

INSERT INTO TABLE assessdb.address001 
SELECT
  id,
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
FROM retrievedb.rawaddress 
WHERE id <> '"id"';


--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.address002 (
  id            STRING,
  postcode      STRING,
  latitude      STRING,
  longitude     STRING,
  easting       STRING,
  northing      STRING,
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
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.address002;

INSERT INTO TABLE assessdb.address002 
SELECT
  id,
  rtrim(ltrim(postcode)),
  rtrim(ltrim(latitude)),
  rtrim(ltrim(longitude)),
  rtrim(ltrim(easting)),
  rtrim(ltrim(northing)),
  rtrim(ltrim(gridref)),
  rtrim(ltrim(district)),
  rtrim(ltrim(ward)),
  rtrim(ltrim(districtcode)),
  rtrim(ltrim(wardcode)),
  rtrim(ltrim(country)),
  rtrim(ltrim(countycode)),
  rtrim(ltrim(constituency)),
  rtrim(ltrim(typearea))
FROM assessdb.address001;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.address003 (
  id            INT,
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
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.address003;

INSERT INTO TABLE assessdb.address003 
SELECT
  CAST(id as INT),
  SUBSTRING(postcode,2,LENGTH(postcode)-2),
  CAST(latitude as DECIMAL(18, 9)),
  CAST(longitude as DECIMAL(18, 9)),
  CAST(easting as INT),
  CAST(northing as INT),
  SUBSTRING(gridref,2,LENGTH(gridref)-2),
  SUBSTRING(district,2,LENGTH(district)-2),
  SUBSTRING(ward,2,LENGTH(ward)-2),
  SUBSTRING(districtcode,2,LENGTH(districtcode)-2),
  SUBSTRING(wardcode,2,LENGTH(wardcode)-2),
  SUBSTRING(country,2,LENGTH(country)-2),
  SUBSTRING(countycode,2,LENGTH(countycode)-2),
  SUBSTRING(constituency,2,LENGTH(constituency)-2),
  SUBSTRING(typearea,2,LENGTH(typearea)-2)
FROM assessdb.address002;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.postaddress (
  id            INT,
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
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

INSERT INTO TABLE assessdb.postaddress
SELECT
  id,
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
  assessdb.address003; 
  
--#######################################################################################################################################  
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.addresshistory001 (
  id            STRING,
  pid           STRING,
  aid           STRING,
  did1          STRING,
  did2          STRING
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.addresshistory001;

INSERT INTO TABLE assessdb.addresshistory001
SELECT
  id,
  pid,
  aid ,
  did1,
  did2
FROM
  retrievedb.rawaddresshistory
WHERE id <> '"id"'; 
 
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.addresshistory002 (
  id            INT,
  pid           INT,
  aid           INT,
  did1          INT,
  did2          INT
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.addresshistory002;

INSERT INTO TABLE assessdb.addresshistory002
SELECT
  CAST(id as INT),
  CAST(pid as INT),
  CAST(aid as INT),
  CAST(did1 as INT),
  CAST(did2 as INT)
FROM
  assessdb.addresshistory001;
  
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.addresshistory (
  id            INT,
  pid           INT,
  aid           INT,
  did1          INT,
  did2          INT
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.addresshistory;

INSERT INTO TABLE assessdb.addresshistory
SELECT
  id,
  pid,
  aid ,
  did1,
  did2
FROM
  assessdb.addresshistory002;
 
--####################################################################################################################################### 
DROP TABLE assessdb.address001;
DROP TABLE assessdb.address002;
DROP TABLE assessdb.address003;

DROP TABLE assessdb.addresshistory001;
DROP TABLE assessdb.addresshistory002;

--#######################################################################################################################################
--#######################################################################################################################################
--#######################################################################################################################################

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.account001 (
  id         string,
  pid        string,
  accountno  string,
  balance    string
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.account001;

INSERT INTO TABLE assessdb.account001 
SELECT
  id,
  pid,
  accountno,
  balance
FROM retrievedb.rawaccount 
WHERE id <> '"id"';


--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.account002 (
  id         string,
  pid        string,
  accountno  string,
  balance    string
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.account002;

INSERT INTO TABLE assessdb.account002 
SELECT
  id,
  pid,
  rtrim(ltrim(accountno)),
  balance
FROM assessdb.account001;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.account003 (
  id         INT,
  pid        INT,
  accountid  INT,
  accountno  string,
  balance    DECIMAL(18, 9)
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.account003;

INSERT INTO TABLE assessdb.account003 
SELECT
  CAST(id as INT),
  CAST(pid as INT),
  CAST(accountno as INT),
  CONCAT('AC',accountno),
  CAST(balance as DECIMAL(18, 9))
FROM assessdb.account002;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS assessdb.account (
  id         INT,
  pid        INT,
  accountid  INT,
  accountno  string,
  balance    DECIMAL(18, 9)
)
CLUSTERED BY (id) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

TRUNCATE TABLE assessdb.account;

INSERT INTO TABLE assessdb.account
SELECT
  id,
  pid,
  accountid,
  accountno,
  balance
 FROM
  assessdb.account003; 
 
--#######################################################################################################################################
--#######################################################################################################################################
DROP TABLE assessdb.account001;
DROP TABLE assessdb.account002;
DROP TABLE assessdb.account003;

