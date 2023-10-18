CREATE TABLE transactions (
  id INT,
  from_account INT,
  to_account INT,
  amount INT,
  ts TEXT
) WITH (
  connector = 'kafka',
  format = 'json',
  type = 'source',
  bootstrap_servers = 'broker:29092',
  topic = 'transactions'
);
CREATE VIEW credits AS SELECT to_account as account, sum(amount) as credits FROM transactions GROUP BY to_account;
CREATE VIEW debits AS SELECT from_account as account, sum(amount) as debits FROM transactions GROUP BY from_account;
CREATE VIEW balance AS SELECT credits.account, credits - debits as balance FROM credits INNER JOIN debits ON credits.account = debits.account;
-- failed to plan total: failed to plan balance: don't support joins with updating inputs
-- CREATE MATERIALIZED VIEW total AS SELECT sum(balance) as total FROM balance;
-- select * from total;
