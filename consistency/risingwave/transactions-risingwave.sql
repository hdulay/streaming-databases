create table if not exists transactions (
   id int, from_account int, to_account int, amount int, ts timestamp
)
with (
   connector='kafka',
   topic='transactions',
   properties.bootstrap.server='broker:29092',
   scan.startup.mode='earliest',
   scan.startup.timestamp_millis='140000000'
)
row format json;
create view accounts as select from_account as account from transactions union select to_account from transactions;
create materialized view credits as select transactions.to_account as account, sum(transactions.amount) as credits from transactions left join accounts on transactions.to_account = accounts.account group by to_account;
create materialized view debits as select transactions.from_account as account, sum(transactions.amount) as debits from transactions left join accounts on transactions.from_account = accounts.account group by from_account;
create materialized view balance as select credits.account as account, credits - debits as balance from credits inner join debits on credits.account = debits.account;
create materialized view total as select sum(balance) from balance;
create sink total_sink from total
with (
   connector='kafka',
   properties.bootstrap.server='broker:29092',
   topic='total',
   type='append-only',
   force_append_only='true'
);

drop sink total_sink;
drop materialized view total;
drop materialized view balance;
drop materialized view debits;
drop materialized view credits;
drop view accounts;
drop table transactions;
