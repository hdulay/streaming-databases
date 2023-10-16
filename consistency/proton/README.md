# Transactions - Proton

This test uses the latest Timeplus Proton Docker image.

## Step 1

Run the Python script to produce transactions to Kafka (parent directory).

## Step 2

Start Proton and Kafka:

```
docker compose up -d
```

## Step 3

`docker exec` to the Proton container and run `proton-client -n` command to start the SQL client (e.g. ``docker exec -it proton_proton_1 proton-client -n``)

Run commands in `transactions-proton.sql` one by one or copy and paste all statements.
