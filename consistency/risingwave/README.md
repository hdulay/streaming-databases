# Transactions - RisingWave

This test uses the latest RisingWave Docker image.

## Step 1

Run the Python script to produce transactions to Kafka (parent directory).

## Step 2

Start RisingWave and Kafka:
```
docker-compose up -d
```

## Step 3

Run psql:
```
psql -h localhost -p 4566 -d dev -U root
```

## Step 4

Paste the contents of ``transactions-risingwave.sql`` into psql.

The messages in the sink topic ``total_risingwave`` will be constantly written to but all messages show the correct total (=0):
```
[{'sum': '0'}, {'sum': '0'}, {'sum': '0'}, {'sum': '0'}, {'sum': '0'}, {'sum': '0'}, {'sum': '0'}...]
```

Not quite as impressive as Materialize, but still :)
