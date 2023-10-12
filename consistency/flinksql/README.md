# Transactions - Flink SQL

This test uses a locally installed Flink (latest version; 1.17.1).

## Step 1

Run the Python script to produce transactions to Kafka (parent directory).

## Step 2

Start Flink cluster. Make sure to run a Kafka cluster on localhost:9092 beforehand.
```
start-cluster.sh
```

## Step 3

Run sql-client.sh. Make sure to have put a matching version of the Flink SQL Connector JAR (e.g. for 1.17.1 - flink-sql-connector-kafka-3.0.0.jar) into the lib directory of the local Flink installation:  
```
sql-client.sh -l lib
```

## Step 4

Paste the contents of ``transactions-flinksql.sql`` into the SQL Client (one by one, unfortunately).

As long as new messages enter the source topic ``transactions``, the sink topic ``total_flink`` will contain (a lot of) messages with all kinds of totals:
```
[{"total": 0.0}, {"total": 0.0}, {"total": 1.0}, {"total": 1.0}, {"total": -1.0}, ...]
```

Once you stop new messages from arriving in ``transactions``, you get the correct total:
```
{"total": 0.0}
```
