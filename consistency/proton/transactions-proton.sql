CREATE EXTERNAL STREAM transactions_s(raw string) SETTINGS type='kafka', brokers='broker:9092', topic='transactions'

CREATE TABLE transactions (
id  BIGINT,
from_account INT,
to_account INT,
amount DOUBLE,
ts TIMESTAMP(3),
WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
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

CREATE VIEW accepted_transactions(id) AS SELECT id FROM transactions;

CREATE VIEW outer_join_with_time(id, other_id) AS SELECT t1.id, t2.id as other_id FROM transactions as t1 LEFT JOIN transactions as t2 ON t1.id = t2.id AND t1.ts = t2.ts;

CREATE VIEW outer_join_without_time(id, other_id) AS SELECT t1.id, t2.id as other_id FROM (SELECT id FROM transactions) as t1 LEFT JOIN (SELECT id FROM transactions) as t2 ON t1.id = t2.id;

CREATE VIEW credits(account, credits) AS SELECT to_account as account, sum(amount) as credits FROM transactions GROUP BY to_account;

CREATE VIEW debits(account, debits) AS SELECT from_account as account, sum(amount) as debits FROM transactions GROUP BY from_account;

CREATE VIEW balance(account, balance) AS SELECT credits.account, credits - debits as balance FROM credits, debits WHERE credits.account = debits.account;

CREATE VIEW total(total) AS SELECT sum(balance) FROM balance;

-- CREATE VIEW credits2(account, credits, ts) AS SELECT to_account as account, sum(amount) as credits, max(ts) as ts FROM transactions GROUP BY to_account;

-- CREATE VIEW debits2(account, debits, ts) AS SELECT from_account as account, sum(amount) as debits, max(ts) as ts FROM transactions GROUP BY from_account;

-- CREATE VIEW balance2(account, balance, ts) AS SELECT credits2.account, credits - debits as balance, credits2.ts FROM credits2, debits2 WHERE credits2.account = debits2.account AND credits2.ts = debits2.ts;

-- CREATE VIEW total2(total) AS SELECT sum(balance) FROM balance2;

CREATE TABLE total_sink (
    total DOUBLE,
    PRIMARY KEY (total) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'property-version' = 'universal',
    'properties.bootstrap.servers' = 'localhost:9092',
    'topic' = 'total',
    'key.format' = 'json',
    'value.format' = 'json',
    'properties.group.id' = 'total_flink'
);
