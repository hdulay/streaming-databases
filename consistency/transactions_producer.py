import datetime, json, random, time
from kafi.kafi import Cluster

c = Cluster("local")
c.touch("transactions", partitions=1)
p = c.producer("transactions")
random.seed(42)
for id_int in range(0, 10000):
  row_str = json.dumps({
        "id": id_int,
        "from_account": random.randint(0, 9),
        "to_account": random.randint(0, 9),
        "amount": 1,
        "ts": datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")
    })
  print(row_str)
  p.produce(row_str, key=str(id_int))
  if id_int % 1000 == 0:
    p.flush()
  time.sleep(0.01)
p.close()
