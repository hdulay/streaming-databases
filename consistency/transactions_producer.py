import random
import json

import datetime
import time
from kafi.kafi import Cluster

c = Cluster("local")
p = c.producer("transactions")
id = 0
random.seed(42)
while True:
    row = json.dumps({
        'id': id,
        'from_account': random.randint(0,9),
        'to_account': random.randint(0,9),
        'amount': 1,
        'ts': datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")
    })
    print(row)
    p.produce(row, key=str(id))
    if id % 1000 == 0:
      p.flush()
    id += 1
    time.sleep(0.01)
    #
    # p.close()
