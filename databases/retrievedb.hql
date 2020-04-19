# retrieve.hql to retrieve raw data from different sources
--#######################################################################################################################################
--#######################################################################################################################################
--#######################################################################################################################################
DROP DATABASE retrievedb CASCADE;

--#######################################################################################################################################
CREATE DATABASE IF NOT EXISTS retrievedb;
USE retrievedb;

--#######################################################################################################################################
DROP TABLE retrievedb.rawfirstname;
DROP TABLE retrievedb.rawlastname;
DROP TABLE retrievedb.rawperson;
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS retrievedb.rawfirstname (
  firstnameid    string,
  firstname      string,
  sex            string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'C:/users/james/hivewarehouse/rawfirstname.csv' OVERWRITE INTO TABLE retrievedb.rawfirstname;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS retrievedb.rawlastname (
  lastnameid    string,
  lastname      string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'C:/users/james/hivewarehouse/rawlastname.csv' OVERWRITE INTO TABLE retrievedb.rawlastname;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS retrievedb.rawperson (
  persid         string,
  firstnameid    string,
  lastnameid     string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'C:/users/james/hivewarehouse/rawperson.csv' OVERWRITE INTO TABLE retrievedb.rawperson;

--#######################################################################################################################################

DROP TABLE retrievedb.rawaccount;
CREATE TABLE IF NOT EXISTS retrievedb.rawaccount (
  id         string,
  pid        string,
  accountno  string,
  balance    string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'C:/users/james/hivewarehouse/rawaccount.csv' OVERWRITE INTO TABLE retrievedb.rawaccount;

DROP TABLE retrievedb.rawdatetime;
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS retrievedb.rawdatetime (
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
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'C:/users/james/hivewarehouse/rawdatetime.csv' OVERWRITE INTO TABLE retrievedb.rawdatetime;

DROP TABLE retrievedb.rawaddress;
CREATE TABLE IF NOT EXISTS retrievedb.rawaddress (
  id            string,
  Postcode      string,
  Latitude      string,
  Longitude     string,
  Easting       string,
  Northing      string,
  GridRef       string,
  District      string,
  Ward          string,
  DistrictCode  string,
  WardCode      string,
  Country       string,
  CountyCode    string,
  Constituency  string,
  TypeArea      string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'C:/users/james/hivewarehouse/rawaddress.csv' OVERWRITE INTO TABLE retrievedb.rawaddress;

--#######################################################################################################################################
--#######################################################################################################################################
DROP TABLE retrievedb.rawaddresshistory;
CREATE TABLE IF NOT EXISTS retrievedb.rawaddresshistory (
  id            string,
  pid           string,
  aid           string,
  did1          string,
  did2          string
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH 'C:/users/james/hivewarehouse/rawaddresshistory.csv' OVERWRITE INTO TABLE retrievedb.rawaddresshistory;
