# transformdb that contians the analytic model (dimensions and facts)
--#######################################################################################################################################
--#######################################################################################################################################
--#######################################################################################################################################
--#######################################################################################################################################
CREATE DATABASE IF NOT EXISTS transformdb;
USE transformdb;
  
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS transformdb.dimperson (
  personkey  BIGINT,
  firstname  STRING,
  lastname   STRING
)
CLUSTERED BY (firstname, lastname,personkey) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
  
--#######################################################################################################################################
TRUNCATE TABLE transformdb.dimperson;
  
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS transformdb.dimperson001 (
  firstname  STRING,
  lastname   STRING
)
CLUSTERED BY (firstname, lastname) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
  
--#######################################################################################################################################
TRUNCATE TABLE transformdb.dimperson001;
    
--#######################################################################################################################################
INSERT INTO TABLE transformdb.dimperson001 
SELECT DISTINCT
  firstname, 
  lastname 
FROM 
  processdb.personhub;

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS transformdb.dimperson002 (
  personkey  BIGINT,
  firstname  STRING,
  lastname   STRING
)
CLUSTERED BY (firstname, lastname,personkey) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');

--#######################################################################################################################################
TRUNCATE TABLE transformdb.dimperson002; 

--#######################################################################################################################################
INSERT INTO TABLE transformdb.dimperson002
SELECT 
  ROW_NUMBER() OVER (ORDER BY firstname, lastname),
  firstname, 
  lastname 
FROM 
  transformdb.dimperson001;
  
--#######################################################################################################################################
INSERT INTO TABLE transformdb.dimperson
SELECT 
  personkey,
  firstname, 
  lastname 
FROM 
  transformdb.dimperson002
ORDER BY firstname, lastname, personkey;
 
--#######################################################################################################################################
#INSERT INTO TABLE transformdb.dimperson
#(999997,'Ruff','Hond'),
#(999998,'Robbie','Rot'),
#(999999,'Helen','Kat');
  
--#######################################################################################################################################
DROP TABLE transformdb.dimperson001;
DROP TABLE transformdb.dimperson002;
  
--#######################################################################################################################################


CREATE TABLE IF NOT EXISTS transformdb.dimaccount (
  accountkey      BIGINT,
  accountnumber   INT
)
CLUSTERED BY (accountnumber,accountkey) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
  
--#######################################################################################################################################
TRUNCATE TABLE transformdb.dimaccount;
 
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS transformdb.dimaccount001 (
  accountnumber   INT
)
CLUSTERED BY (accountnumber) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
  
--#######################################################################################################################################
TRUNCATE TABLE transformdb.dimaccount001;

--#######################################################################################################################################
INSERT INTO TABLE transformdb.dimaccount001 
SELECT DISTINCT
  objectid 
FROM 
  processdb.objecthub
WHERE objecttype = 'intangible'
AND objectname = 'bankaccount';
  
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS transformdb.dimaccount002 (
  accountkey      BIGINT,
  accountnumber   INT
)
CLUSTERED BY (accountnumber,accountkey) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
  
--#######################################################################################################################################
TRUNCATE TABLE transformdb.dimaccount002;

--#######################################################################################################################################
INSERT INTO TABLE transformdb.dimaccount002 
SELECT DISTINCT
  ROW_NUMBER() OVER (ORDER BY accountnumber DESC),
  accountnumber  
FROM 
  transformdb.dimaccount001;

--#######################################################################################################################################
INSERT INTO TABLE transformdb.dimaccount 
SELECT DISTINCT
  accountkey,
  accountnumber  
FROM 
  transformdb.dimaccount002
ORDER BY accountnumber; 

--#######################################################################################################################################
#INSERT INTO TABLE transformdb.dimaccount 
#VALUES
#(88888887,208887),
#(88888888,208888),
#(88888889,208889);

--#######################################################################################################################################
DROP TABLE transformdb.dimaccount001;
DROP TABLE transformdb.dimaccount002;
 
--#######################################################################################################################################

CREATE TABLE IF NOT EXISTS transformdb.fctpersonaccount (
  personaccountkey     BIGINT,
  personkey            BIGINT,
  accountkey           BIGINT,
  balance             DECIMAL(18, 9)
)
CLUSTERED BY (personkey,accountkey) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
  
--#######################################################################################################################################
TRUNCATE TABLE transformdb.fctpersonaccount;
 
--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS transformdb.fctpersonaccount001 (
  personkey            BIGINT,
  accountkey           BIGINT,
  balance             DECIMAL(18, 9)
)
CLUSTERED BY (personkey,accountkey) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
   
--#######################################################################################################################################
TRUNCATE TABLE transformdb.fctpersonaccount001;
   
--#######################################################################################################################################
#INSERT INTO TABLE transformdb.fctpersonaccount001
#VALUES
#(999997,88888887,10.60),
#(999997,88888887,400.70),
#(999997,88888887,-210.90),
#(999998,88888888,1000.00),
#(999998,88888888,1990.60),
#(999998,88888888,900.70),
#(999999,88888889,160.60),
#(999999,88888889,180.70),
#(999999,88888889,100.60),
#(999999,88888889,120.90),
#(999999,88888889,180.69),
#(999999,88888889,130.30);

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS transformdb.fctpersonaccount002 (
  personkey      BIGINT,
  accountkey     BIGINT,
  balance        DECIMAL(18, 9)
)
CLUSTERED BY (personkey,accountkey) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
   
--#######################################################################################################################################
TRUNCATE TABLE transformdb.fctpersonaccount002;     
     
--#######################################################################################################################################
INSERT INTO TABLE transformdb.fctpersonaccount002
SELECT 
CAST(personkey AS BIGINT), 
CAST(accountkey AS BIGINT), 
CAST(SUM(balance) AS DECIMAL(18, 9))
FROM transformdb.fctpersonaccount001
GROUP BY personkey, accountkey;

--#######################################################################################################################################
INSERT INTO TABLE transformdb.fctpersonaccount
SELECT 
ROW_NUMBER() OVER (ORDER BY personkey, accountkey),
CAST(personkey AS BIGINT), 
CAST(accountkey AS BIGINT), 
CAST(balance AS DECIMAL(18, 9))
FROM transformdb.fctpersonaccount002;

--#######################################################################################################################################
DROP TABLE transformdb.fctpersonaccount001;
DROP TABLE transformdb.fctpersonaccount002; 


--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS transformdb.dimaddress(
  addresskey    BIGINT,
  postcode      STRING
)
CLUSTERED BY (addresskey) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
  
--#######################################################################################################################################
TRUNCATE TABLE transformdb.dimaddress;
  
--#######################################################################################################################################
#INSERT INTO TABLE transformdb.dimaddress
#VALUES
#(1,'KA12 8RR'),
#(2,'FK8 1EJ'),
#(3,'EH1 2NG');
 
--#######################################################################################################################################

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS transformdb.dimdatetime(
  datetimekey    BIGINT,
  datetimestr    STRING
)
CLUSTERED BY (datetimekey) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
  
--#######################################################################################################################################
TRUNCATE TABLE transformdb.dimdatetime;
  
--#######################################################################################################################################
#INSERT INTO TABLE transformdb.dimdatetime
#VALUES
#(1,'2015/08/23 16h00'),
#(2,'2015/10/03 17h00'),
#(3,'2015/11/12 06h00');

--#######################################################################################################################################

--#######################################################################################################################################
CREATE TABLE IF NOT EXISTS transformdb.fctpersonaddressdate(
  personaddressdatekey      BIGINT,
  personkey                 BIGINT,
  addresskey                BIGINT,
  datetimekey               BIGINT
)
CLUSTERED BY (datetimekey) INTO 1 BUCKETS
STORED AS orc 
TBLPROPERTIES('transactional' = 'true','orc.compress'='ZLIB','orc.create.index'='true');
  
--#######################################################################################################################################
TRUNCATE TABLE transformdb.fctpersonaddressdate;
  
--#######################################################################################################################################
#INSERT INTO TABLE transformdb.fctpersonaddressdate
#VALUES
#(1,999997,1,1),
#(2,999998,2,2),
#(3,999999,3,3);
 
--#######################################################################################################################################
