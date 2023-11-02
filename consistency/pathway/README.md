# Consistency Test for Pathway

`pip install pathway`

You will need to consume from Kafka/RedPanda:

`rpk topic create transactions`

`rpk topic consume balance`

The value of `total` stays consistent.

```json
{
  "topic": "balance",
  "key": "\ufffd3\ufffd\u0010@\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000",
  "value": "{\"total\":0,\"diff\":1,\"time\":1698960910176}",
  "timestamp": 1698960911158,
  "partition": 0,
  "offset": 2
}
```