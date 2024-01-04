CREATE EXTERNAL STREAM transactions_s(raw string) SETTINGS type='kafka', brokers='broker:29092', topic='transactions';

CREATE VIEW transactions AS 
    SELECT raw:id::int as id, raw:from_account::int as from_account, raw:to_account::int as to_account, 
           raw:amount::double as amount, raw:ts::datetime64 as ts
    FROM transactions_s;

CREATE VIEW credits AS SELECT to_account as account, sum(amount) as credits, ts FROM transactions GROUP BY to_account, ts;

CREATE VIEW debits AS SELECT from_account as account, sum(amount) as debits, ts FROM transactions GROUP BY from_account, ts, ts;

CREATE VIEW balance AS SELECT credits.account, credits - debits as balance FROM credits, debits WHERE credits.account = debits.account AND credits.ts = debits.ts;

-- make sure total_proton topic is created before creatint this external stream
CREATE EXTERNAL STREAM total_s(total int) SETTINGS type='kafka', brokers='broker:29092', topic='total_proton', data_format='JSONEachRow';

CREATE MATERIALIZED VIEW total INTO total_s AS SELECT sum(balance) as total FROM balance;

DROP VIEW total;

DROP VIEW balance;

DROP VIEW debits;

DROP VIEW credits;

DROP STREAM transactions;

DROP STREAM total_s;
