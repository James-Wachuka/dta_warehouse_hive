###### Hive Warehouse
Hive is a good tool for analysis of data. Hive runs on hadoop and therefore utilises the capabilities of hadoop  i.e distributed file system and parallel processing of data, mapreduce among others. This implementation provides a simple guideline for implementing a data warehouse in hive. Here we are going to convert the sun models into star models. That is to say we provide  fields with data types and then transalate the the star schemas into hive  code to build the warehouse. Prior to this we need to validate some rules. 
1. An interlink matrix(containing the facts and dimensions) should be clearly set. Dimensions on the left side and facts on the top row.Remove any duplicates.
2. Dimensions. The core selectors of the data warehouse. Tables containing information related to the business needs like customers, sales, stores.
3. Facts are measures of the analytic model created from the sun model. Example of measure would be customer count.
4. Check that the dimensions have the correct type of business needs(does it address the SCDS:type 0;type 1,type 2,type 3,outriggers,fast-changing,etc).

A *sun model* is an abstract representation of our business for which we'll implement an analytic model

###### Structure of the warehouse
1. A retrieve database for collecting and storing data from the business. All the data from different sources is stored here.
2. Assess database to validate the data used in for building the warehouse. Here we apply some basic transformations to our data ensure correctness, accuracy among other rules.
3. process database is our data vault as it holds the data for the analytic model. An implementation of  a process database will include *hubs* (business key: That rarely change) *links* (asscociation and transactions between the business keys 
*satellites* ( isolated tables for storing the detail attributes)
4. Transform database will hold the analytic model(consisting of facts and dimensions)

###### Tools 
*hive* -  donwload a stable version from http://hive.apache.org/releases.html preferably verion 2 and above for hadoop-2.7+
NOTE: some hive realeases do not contain the windows executables, I  recommend hive-2.1.1
2. unzip the file and place the contents in your preffered location. I used `C:\hadoop-2.9.2\hive`
3. set up user variables:
`HIVE_HOME` as `C:\hadoop-2.9.2\hive`
`HIVE_BIN` as `C:\hadoop-2.9.2\hive\bin`
`HIVE_LIB` as `C:\hadoop-2.9.2\hive\lib`
add to the path variable `C:\hadoop-2.9.2\hive\bin`,`C:\hadoop-2.9.2\hive\lib`

edit hive-site.mxl depending on the metastore database(I used mysql)

```
<configuration>
<!-- database connections -->
<property>
<name>javax.jdo.option.ConnectionURL</name>
<value>jdbc:mysql://localhost:3306/metastore?useSSL=false</value>
</property>
<property>
<name>javax.jdo.option.ConnectionDriverName</name>
<value>com.mysql.jdbc.Driver</value>
</property>
<property>
<name>javax.jdo.option.ConnectionUserName</name>
<value>hiveuser</value>
</property>
<property>
<name>javax.jdo.option.ConnectionPassword</name>
<value>hivepwd</value>
</property>
<property>
<name>datanucleus.schema.autoCreateTables</name>
<value>true</value>
</property>
property>
<name>datanucleus.schema.autoCreateAll</name>
<value>true</value>
</property>
<!-- Transactional support:ACID -->
<property>
<name>hive.exec.dynamic.partition.mode</name>
<value>nonstrict</value>
</property>
<property>
<name>hive.enforce.bucketing</name>
<value>true</value>
</property>
<property>
<name>hive.support.concurrency</name>
<value>true</value>
</property>
<property>
<name>hive.txn.manager</name>
<value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
</property>
<property>
<name>hive.compactor.initiator.on</name>
<value>true</value>
</property>
<property>
<name>hive.compactor.worker.threads</name>
<value>10</value>
</property>
</configuration>
```



4. Create directory for hive warehouse in hadoop. Start hadoop daemons and run the following . leave hadoop safemode using `hadoop dfsadmin -safemode leave`
```
hadoop fs -mkdir /tmp
hadoop fs -mkdir /user/hive
hadoop fs -mkdir /user/hive/warehouse
```

give read/write priviliges to these directories
```
hadoop fs -chmod 777 /tmp
hadoop fs -chmod 777 /user/hive
hadoop fs -chmod 777 /user/hive/warehouse
```

5. Finally run hive using `hive` command
If you run get into errors (hive cannot connect to the metastore database-due to schema problems),  Create the schema manually before running hive for the first time. In `hive\scripts\metastore\` there are schemas for the metastore database.Using mysql for the metastore I created a metastore database and run the schema for `hive-schema-2.1.0.mysql` against the database. Also the TXN tables are required to hold transaction information(To support database transactions in hive ). Run the `hive-txn-schema-2.1.0.mysql` against the database.

###### Files
csv files for experimentation with the warehouse.These files hold the data for building the warehouse.you can use custom files or agents such as *sqoop* or *flume* to get some data into your databases.Included are all the scripts for implementing the complete data warehouse.

###### execution
run `hive -f example.hql` for executing hive scripts containing hive code (easier for debugging). Or using hive prompt `hive` command.Two modes of execution can not exist at the same time.

###### what next
create some data marts(aggregations of the analytic model) for  easy reporting and analysis.

###### optimization:
Hive by default uses mapreduce as its execution engine, using Tez will improve execution time
using Tez as execution engine for hive set the following properties:
`<property>
<name>hive.execution.engine</name>
<value>tez</value>`

`hive.prewarm.enabled=true`
`hive.prewarm.numcontainers=10`

using file formats optimized for hive eg ORC, parquet set the following properties for ORC:
`orc.compress = SNAPPY` 
`orc.compress.size = 262144` 
`orc.stripe.size= 64 MB`
`orc.row.index.stride =10000` 
`orc.create.index= true`
usage:
`CREATE TABLE person_orc STORED AS ORC tblproperties("orc.compress"="SNAPPY")
 AS SELECT * FROM person;`

hive by default executes by processing one row at a time which requires  mutliple layers of virtual method calls within the nested loop(inefficient for  CPU). Vectorized query execution eliminates this problem by reading rows in batch and applying operations to the entire collection instead of one. 
`hive.vectorized.execution.enabled=true`

*Cost based optimization engine* This method use stats from tables to optimize queries.First we need to enable stats collection by setting the following property
`hive.stats.autogather = true`
Then we compute stats for tables we want to query from, using:
`ANALYZE TABLE person_orc COMPUTE STATISTICS;`
`ANALYZE TABLE person_orc COMPUTE STATISTICS FOR COLUMNS;`
Now we need to set properties for hive to use the CBO engine
`hive.cbo.enable=true`
`hive.compute.query.using.stats = true`
`hive.stats.fetch.column.stats = true`
`hive.stats.fetch.partition.stats = true`









