CREATE EXTERNAL STREAM transactions(
    id int,
    from_account int,
    to_account int,
    amount int,
    ts datetime64) 
SETTINGS type='kafka', brokers='broker:29092', topic='transactions', data_format='JSONEachRow';

CREATE VIEW credits AS SELECT to_account as account, sum(amount) as credits FROM transactions GROUP BY to_account;

CREATE VIEW debits AS SELECT from_account as account, sum(amount) as debits FROM transactions GROUP BY from_account;

CREATE VIEW balance AS SELECT credits.account, credits - debits as balance FROM credits, debits WHERE credits.account = debits.account;

-- make sure total_proton topic is created before creatint this external stream
CREATE EXTERNAL STREAM total_s(total int) SETTINGS type='kafka', brokers='broker:29092', topic='total_proton', data_format='JSONEachRow';

CREATE MATERIALIZED VIEW total INTO total_s AS SELECT sum(balance) as total FROM balance;

DROP VIEW total;

DROP VIEW balance;

DROP VIEW debits;

DROP VIEW credits;

DROP STREAM transactions;

DROP STREAM total_s;
