CREATE TABLE transactions (
    id VARCHAR PRIMARY KEY,
    from_account INT,
    to_account INT,
    amount DOUBLE,
    ts VARCHAR
) WITH (
    kafka_topic='transactions', 
    value_format='json', 
    partitions=1,
    timestamp='ts',
    timestamp_format='yyyy-MM-dd HH:mm:ss.SSS'
);

CREATE TABLE credits WITH (
    kafka_topic='credits',
    value_format='json') AS
SELECT
    to_account AS account, 
    sum(amount) AS credits
FROM
    transactions
GROUP BY
    to_account
EMIT CHANGES;

CREATE TABLE debits WITH (
    kafka_topic='debits',
    value_format='json') AS
SELECT
    from_account AS account, 
    sum(amount) AS debits
FROM
    transactions
GROUP BY
    from_account
EMIT CHANGES;

CREATE TABLE balance WITH (
    kafka_topic='balance',
    value_format='json') AS
SELECT
    credits.account AS account, 
    credits - debits AS balance
FROM
    credits 
INNER JOIN
    debits
ON
    credits.account = debits.account
EMIT CHANGES;

CREATE TABLE total WITH (
    kafka_topic='total_ksqldb',
    value_format='json') AS
SELECT
    'foo',
    sum(balance)
FROM
    balance
GROUP BY
    'foo'
EMIT CHANGES;
