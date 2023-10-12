# Transactions - ksqlDB

This test uses a Confluent Platform Docker image for ksqlDB.

## Step 1

Run the Python script to produce transactions to Kafka (parent directory).

## Step 2

Run ksqlDB + Kafka:
```
docker-compose up -d
```

## Step 3

Run ksql:
```
ksql
```

## Step 4

Paste the contents of ``transactions-ksql.sql`` into ksql.

As long as new messages enter the source topic ``transactions``, the sink topic ``total_ksqldb`` will contain messages with all kinds of totals:
```
([{'KSQL_COL_1': -8.0}, {'KSQL_COL_1': 4.0}, {'KSQL_COL_1': -1.0}, {'KSQL_COL_1': 3.0}, {'KSQL_COL_1': -1.0}, {'KSQL_COL_1': 2.0}, {'KSQL_COL_1': 27.0}, ...]
```

Once you stop new messages from arriving in ``transactions``, you get the correct total:
```
{'KSQL_COL_1': 0.0}
```
