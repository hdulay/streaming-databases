create connection kafka_connection to kafka (broker 'broker:29092');
create source transactions_source from kafka connection kafka_connection (topic 'transactions', start offset (0)) key format text value format text include key envelope upsert with (size='1');
create materialized view transactions as select ((text::jsonb)->>'id')::string as id, ((text::jsonb)->>'from_account')::int as from_account, ((text::jsonb)->>'to_account')::int as to_account, ((text::jsonb)->>'amount')::int as amount, ((text::jsonb)->>'ts')::timestamp as ts, key from transactions_source;
create view accounts as select from_account as account from transactions union select to_account from transactions;
create view credits as select transactions.to_account as account, sum(transactions.amount) as credits from transactions left join accounts on transactions.to_account = accounts.account group by to_account;
create view debits as select transactions.from_account as account, sum(transactions.amount) as debits from transactions left join accounts on transactions.from_account = accounts.account group by from_account;
create view balance as select credits.account as account, credits - debits as balance from credits inner join debits on credits.account = debits.account;
create materialized view total as select sum(balance) from balance;
create sink total_sink from total into kafka connection kafka_connection (topic 'total') format json envelope debezium with (size='1');
