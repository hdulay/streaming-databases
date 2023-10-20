
CREATE TABLE transactions (
id  BIGINT,
from_account INT,
to_account INT,
amount DOUBLE,
ts TIMESTAMP(3),
WATERMARK FOR ts AS ts - INTERVAL '10' MINUTE
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


with credits AS (SELECT to_account as account, sum(amount) as credits FROM transactions GROUP BY to_account),
debits AS (SELECT from_account as account, sum(amount) as debits FROM transactions GROUP BY from_account),
balance AS (
    SELECT c.account, c.credits - d.debits as balance 
    FROM credits c
    join debits d on d.account=c.account
)
SELECT sum(balance) FROM balance;

SELECT window_start, window_end, SUM(price)
  FROM TABLE(TUMBLE(TABLE balance, DESCRIPTOR(bidtime), INTERVAL '10' SECOND))
  GROUP BY window_start, window_end;