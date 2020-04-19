###### Hive Warehouse
Hive is a great tool for analysis of data. Hive runs on hadoop and therefore utilises the capabilities of hadoop  i.e distributed file system and parallel processing of data, mapreduce among others. This implementation provides a simple guideline for implementing a data warehouse in hive. Here we are going to convert the sun models into star models. That is to say we provide  fields with data types and then transalate the the star schemas into hive  code to build the warhouse. Prior to this we need to validate some rules. 
1. An interlink matrix(containing the facts and dimensions) should be clearly set. Dimensions on the left side and facts on the top row.Remove any duplicates.
2. Dimensions. The core selectors of the data warehouse. Tables containing information related to the business needs like customers, sales, stores.
3. Facts are measures of the analytic model created fromm the sun model. Example of measure would be customer count.
4. Check that the dimensions have the correct type of business needs(does it address the SCDS:type 0;type 1,type 2,type 3,outriggers,fast-changing,etc).

A *sun model* is an abstract representation of our business for which we'll implement an analytic model.

####### Structure of the warehouse
1. A retrieve database for collecting data and storing for the business. All the data from different sources is stored here.
2. Assess database to validate the data used in for building the warehouse. Here we apply some basic transformations to our data ensure correctness, accuracy among other rules.
3. process database is our data vault as it holds the data for the analytic model. An implementation of  a process database will include *hubs* (business key: That rarely change) *links* (asscociation and transactions between the business keys 
*satellites* ( isolated tables for storing the detail attributes)
4. Transform database will hold the analytic model(consisting of facts and dimensions)

####### Tools 
*hive* is an  important tool for this project and therefore it should be installed  and configured accordingly. I will give a step by step guide to get hive running on your machine. I assume that hadoop is installed and running on your machine. refer here - http://github.com/jewachu/hadoop_on_windows
1. donwload a stable version from http://hive.apache.org/releases.html preferably verion 2 and above for hadoop-2.7+
NOTE: some hive realeases do not the windows executables, I  recommend hive-2.1.1
2. unzip the file and place the contents in your preffered location. I used `C:\hadoop-2.9.2\hive`
3. set up user variables:
`HIVE_HOME` as `C:\hadoop-2.9.2\hive`
`HIVE_BIN` as `C:\hadoop-2.9.2\hive\bin`
`HIVE_LIB` as `C:\hadoop-2.9.2\hive\lib`
add to the path variable `C:\hadoop-2.9.2\hive\bin`,`C:\hadoop-2.9.2\hive\lib`

edit hive-site.mxl as depending on the metastore database(I used mysql)
`<configuration>
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
</configuration>`

4. Create directory for hive warehouse in hadoop. Start hadoop daemons and run the following commands.First you have to leave hadoop safemode using `hadoop dfsadmin -safemode leave`
`hadoop fs -mkdir /tmp`
`hadoop fs -mkdir /user/hive`
`hadoop fs -mkdir /user/hive/warehouse`
give read/write priviliges to these directories
`hadoop fs -chmod 777 /tmp`
`hadoop fs -chmod 777 /user/hive`
`hadoop fs -chmod 777 /user/hive/warehouse`

5. Finally run hive using `hive` command.
If you run get into errors it is mostly because hive cannot connect to the metastore database(due to schema problems). I recommend creating the schema manually before running hive for the first time. In `hive\scripts\metastore\` there are schemas for the metastore database.Since I am using mysql for the metastore I created a metastore database and run the schema for `hive-schema-2.1.0.mysql` against the database. Also the TXN tables are required to hold transaction information(To support database transactions in hive ). I run the `hive-txn-schema-2.1.0.mysql` against the database.

With all these done hive is guaranteed to run on your machine.

####### Files
I have included some csv files for experimentation with the warehouse.These files hold the data for building the warehouse.However feel free to use custom files or agents such as *sqoop* or *flume* to get some data into your databases.Since I am using hive scripts to build the warehouse I have included all the scripts for implementing the complete data warehouse.

####### execution
Use `hive -f example.hql` to run the hive scripts containing hive code.This is simpler than typing the entire code on a hive prompt also it is easier for debugging. You can execute hive commands using hive prompt. Using `hive` command. Key to note that the two modes of execution can not exist at the same time.

####### what next
On completion of this example, you should create some data marts(aggregations of the analytic model) for  easy reporting and analysis.