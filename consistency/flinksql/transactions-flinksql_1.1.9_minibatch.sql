SET 'table.exec.mini-batch.enabled' = 'true';

SET 'table.exec.mini-batch.allow-latency' = '5S';

SET 'table.exec.mini-batch.size' = '50';
-- no mini batching: 77319
-- 1: 77319
-- 10: 403
-- 20: 49
-- 50: 1
-- 50 ooo: 1
-- 100: 1
-- 5000: 1

CREATE TABLE transactions (
id  BIGINT,
from_account INT,
to_account INT,
amount DOUBLE,
ts TIMESTAMP(3)
) WITH (  
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'transactions_flink',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'true',
    'json.ignore-parse-errors' = 'false'
);

CREATE VIEW credits(account, credits) AS SELECT to_account as account, sum(amount) as credits FROM transactions GROUP BY to_account;

CREATE VIEW debits(account, debits) AS SELECT from_account as account, sum(amount) as debits FROM transactions GROUP BY from_account;

CREATE VIEW balance(account, balance) AS SELECT credits.account, credits - debits as balance FROM credits, debits WHERE credits.account = debits.account;

CREATE VIEW total(total) AS SELECT sum(balance) FROM balance;

CREATE TABLE total_sink (
    total DOUBLE,
    PRIMARY KEY (total) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'property-version' = 'universal',
    'properties.bootstrap.servers' = 'localhost:9092',
    'topic' = 'total_flinksql',
    'key.format' = 'json',
    'value.format' = 'json',
    'properties.group.id' = 'total_flinksql'
);

INSERT INTO total_sink SELECT * FROM total;

-- DROP VIEW total;

-- DROP VIEW balance;

-- DROP VIEW debits;

-- DROP VIEW credits;

-- DROP TABLE transactions;
