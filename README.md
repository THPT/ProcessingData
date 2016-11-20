# ProcessingData

Using Spark + Hive.

Import from MySQL to Hive using Sqooq:

```
./bin/sqoop import --connect jdbc:mysql://127.0.0.1/svcdb --username grok --password grok --table user  --hive-overwrite --hive-import  --hive-drop-import-delims

./bin/sqoop import --connect jdbc:mysql://127.0.0.1/svcdb --username grok --password grok --table selling_order  --hive-overwrite --hive-import  --hive-drop-import-delims

./bin/sqoop import --connect jdbc:mysql://127.0.0.1/svcdb --username grok --password grok --table selling_store  --hive-overwrite --hive-import  --hive-drop-import-delims

./bin/sqoop import --connect jdbc:mysql://127.0.0.1/svcdb --username grok --password grok --table product  --hive-overwrite --hive-import  
--hive-drop-import-delims

./bin/sqoop import --connect jdbc:mysql://127.0.0.1/svcdb --username grok --password grok --table video  --hive-overwrite --hive-import  --hive-drop-import-delims -m 1
```



Run:
```
mvn clean && mvn compile && mvn package && spark-submit --class ProcessingData target/ProcessingData-0.0.1-SNAPSHOT.jar
```
