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
create view credits as select to_account as account, sum(amount) as credits from transactions group by to_account;
create view debits as select from_account as account, sum(amount) as debits from transactions group by from_account;
create view balance as select credits.account as account, credits - debits as balance from credits inner join debits on credits.account = debits.account;
create materialized view total as select sum(balance) from balance;
create sink total_sink from total
with (
   connector='kafka',
   properties.bootstrap.server='broker:29092',
   topic='total_risingwave',
   type='append-only',
   force_append_only='true'
);

drop sink total_sink;
drop materialized view total;
drop view balance;
drop view debits;
drop view credits;
drop table transactions;
