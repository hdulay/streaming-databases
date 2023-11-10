CREATE EXTERNAL STREAM transactions_s(raw string) SETTINGS type='kafka', brokers='broker:29092', topic='transactions';

CREATE VIEW transactions AS 
    SELECT raw:id::int as id, raw:from_account::int as from_account, raw:to_account::int as to_account, 
           raw:amount::double as amount, raw:ts::datetime64 as ts
    FROM transactions_s;

CREATE VIEW credits AS SELECT to_account as account, sum(amount) as credits, ts FROM transactions GROUP BY to_account, ts;

CREATE VIEW debits AS SELECT from_account as account, sum(amount) as debits, ts FROM transactions GROUP BY from_account, ts, ts;

CREATE VIEW balance AS SELECT credits.account, credits - debits as balance FROM credits, debits WHERE credits.account = debits.account AND credits.ts = debits.ts;

CREATE MATERIALIZED VIEW total AS SELECT sum(balance) as total FROM balance;

-- CREATE EXTERNAL STREAM total_s(raw string) SETTINGS type='kafka', brokers='broker:29092', topic='total_proton';

-- CREATE MATERIALIZED VIEW total2 INTO total_s AS SELECT * FROM total;
-- Code: 48. DB::Exception: Received from localhost:8463. DB::Exception: MaterializedView doesn't support target storage is ExternalStream. (NOT_IMPLEMENTED)

DROP VIEW total;

DROP VIEW balance;

DROP VIEW debits;

DROP VIEW credits;

DROP VIEW transactions;

DROP STREAM transactions_s;
