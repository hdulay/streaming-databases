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
    'properties.group.id' = 'transactions_flinksql',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'true',
    'json.ignore-parse-errors' = 'false'
);

CREATE VIEW credits(account, credits, ts) AS SELECT to_account as account, sum(amount) as credits, ts FROM transactions GROUP BY to_account, ts;

CREATE VIEW debits(account, debits, ts) AS SELECT from_account as account, sum(amount) as debits, ts FROM transactions GROUP BY from_account, ts;

CREATE VIEW balance(account, balance) AS SELECT credits.account, credits - debits as balance FROM credits, debits WHERE credits.account = debits.account and credits.ts = debits.ts;

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
