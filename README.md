# dynamodb-hive-serde
Hive Deserializer for DynamoDB backup data format.

When AWS Data Pipeline is used to [export backups](http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-importexport-ddb-pipelinejson-verifydata2.html) of DynamoDB tables, the file format is somewhat difficult to parse in Hive. This custom deserializer makes it easy to process files in hive without any pre-processing.

Simply install the DynamoDbSerDe jar and specify the row format as the DynamoDB SerDe in your queries. Pick the DynamoDb column names you want to access and a type they should be. Per line of data the DynamoDb SerDe will locate the columns you specified and coerce the values into the types you specify.

Example query:
```sql
ADD jar /path/to/jar/dynamodb-hive-serde-1.0-SNAPSHOT.jar;

CREATE EXTERNAL TABLE dynamodb (id string, updated_at string, created_at string, version int)
ROW FORMAT SERDE 'com.lyft.hive.serde.DynamoDbSerDe'
LOCATION '/dynamodb/input/';
```

## Timestamp format
You can specify a custom time format, which will be used to construct a [Joda Time DateTimeFormatter](http://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html). For example:
```sql
CREATE EXTERNAL TABLE dynamodb (id string, updated_at timestamp, created_at timestamp, version int)
ROW FORMAT SERDE 'com.lyft.hive.serde.DynamoDbSerDe'
WITH SERDEPROPERTIES ('input.timestamp.format'='yyyy-MM-dd\'T\'HH:mm:ss.SSSSSSZ')
LOCATION '/dynamodb/input/';
```
