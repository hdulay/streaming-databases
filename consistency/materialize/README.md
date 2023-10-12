# Transactions - Materialize

This test uses the latest Materialize Docker image.

## Step 1

Run the Python script to produce transactions to Kafka (parent directory).

## Step 2

Run Materialize + Kafka:
```
docker-compose up -d
```

## Step 3

Run psql:
```
psql postgres://localhost:6875/materialize -U materialize"
```

## Step 4

Paste the contents of ``transactions-materialize.sql`` into psql.

The sink topic ``total`` will contain only one message, regardless of how many messages are being thrown into the source topic ``transactions``:
```
{'before': None, 'after': {'sum': '0'}}
```

That's the power of Differential Dataflow :-)
