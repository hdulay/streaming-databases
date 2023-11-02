#!/bin/python

import pathway as pw

rdkafka_settings = {
    "bootstrap.servers": "localhost:56512",
    "group.id": "pw",
    "session.timeout.ms": "6000"
}

class InputSchema(pw.Schema):
  id: int
  from_account: int
  to_account: int
  amount: int
  ts: str


t = pw.io.kafka.read(
    rdkafka_settings,
    topic="transactions",
    schema=InputSchema,
    format="json",
    autocommit_duration_ms=1000
)

credits = pw.sql('SELECT to_account, sum(amount) as credits FROM T GROUP BY to_account', T=t)
debits = pw.sql('SELECT from_account, sum(amount) as debits FROM T GROUP BY from_account', T=t)
balance = pw.sql('SELECT CC.to_account, credits - debits as balance FROM CC join DD on CC.to_account = DD.from_account', CC=credits, DD=debits)
total = pw.sql('SELECT sum(balance) as total FROM BB', BB=balance)
pw.io.kafka.write(total, rdkafka_settings=rdkafka_settings, topic_name='balance', format='json')
pw.run()
