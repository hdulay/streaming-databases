CREATE EXTERNAL STREAM transactions_s(raw string) SETTINGS type='kafka', brokers='broker:29092', topic='transactions';

CREATE VIEW transactions AS 
    SELECT raw:id::int as id, raw:from_account::int as from_account, raw:to_account::int as to_account, 
           raw:amount::double as amount, raw:ts::datetime64 as ts
    FROM transactions_s;

-- CREATE VIEW accepted_transactions AS SELECT id FROM transactions;

-- CREATE VIEW outer_join_with_time AS SELECT t1.id, t2.id as other_id FROM transactions as t1 LEFT JOIN transactions as t2 ON t1.id = t2.id AND t1.ts = t2.ts;

-- CREATE VIEW outer_join_without_time AS SELECT t1.id, t2.id as other_id FROM (SELECT id FROM transactions) as t1 LEFT JOIN (SELECT id FROM transactions) as t2 ON t1.id = t2.id;

CREATE VIEW credits AS SELECT to_account as account, sum(amount) as credits FROM transactions GROUP BY to_account;

CREATE VIEW debits AS SELECT from_account as account, sum(amount) as debits FROM transactions GROUP BY from_account;

CREATE VIEW balance AS SELECT credits.account, credits - debits as balance FROM credits, debits WHERE credits.account = debits.account;

CREATE MATERIALIZED VIEW total AS SELECT sum(balance) as total FROM balance;

-- TODO for the rest

-- CREATE VIEW credits2(account, credits, ts) AS SELECT to_account as account, sum(amount) as credits, max(ts) as ts FROM transactions GROUP BY to_account;

-- CREATE VIEW debits2(account, debits, ts) AS SELECT from_account as account, sum(amount) as debits, max(ts) as ts FROM transactions GROUP BY from_account;

-- CREATE VIEW balance2(account, balance, ts) AS SELECT credits2.account, credits - debits as balance, credits2.ts FROM credits2, debits2 WHERE credits2.account = debits2.account AND credits2.ts = debits2.ts;

-- CREATE VIEW total2(total) AS SELECT sum(balance) FROM balance2;

CREATE EXTERNAL STREAM total_s(raw string) SETTINGS type='kafka', brokers='broker:29092', topic='total_proton';

CREATE MATERIALIZED VIEW total2 INTO total_s AS SELECT * FROM total;
