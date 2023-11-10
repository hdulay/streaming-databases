import random
import json

import datetime
import time
from kafi.kafi import Cluster

c = Cluster("local")
c.touch("transactions", partitions=1)
p = c.producer("transactions")
random.seed(42)
delayed_row_str_delayed_id_int_tuple_list = []
delayed_int = 0
for id_int in range(0, 10000):
  row_str = json.dumps({
        "id": id_int,
        "from_account": random.randint(0, 9),
        "to_account": random.randint(0, 9),
        "amount": 1,
        "ts": datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")
    })
  #
  if random.randint(0, 9) == 9:
    delayed_row_str_delayed_id_int_tuple_list.append((row_str, id_int))
    delayed_int += 1
  else:
    print(row_str)
    p.produce(row_str, key=str(id_int))
    #
    if id_int % 1000 == 0 or id_int == 9999:
      random.shuffle(delayed_row_str_delayed_id_int_tuple_list)
      for (delayed_row_str, delayed_id_int) in delayed_row_str_delayed_id_int_tuple_list:
        print("delayed: " + delayed_row_str)
        p.produce(delayed_row_str, key=str(delayed_id_int))
      #
      delayed_row_str_delayed_id_int_tuple_list = []
      #
      p.flush()
  #
  time.sleep(0.01)
#
p.close()
#
print(delayed_int)
