# ProcessingData

Using Spark + Hive.

Import from MySQL to Hive using Sqooq:

```
./bin/sqoop import --connect jdbc:mysql://127.0.0.1/svcdb \
--username grok --password grok --table product \
--hive-overwrite --hive-import  --hive-drop-import-delims
```
