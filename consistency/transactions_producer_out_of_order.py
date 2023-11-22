import datetime
import json
import random
import time
from kafi.kafi import Cluster

c = Cluster("local")
c.touch("transactions", partitions=1)
p = c.producer("transactions")
random.seed(42)
delayed_row_dict_list = []
delayed_int = 0
for id_int in range(0, 10000):
  row_dict = {"id": id_int,
              "from_account": random.randint(0, 9),
              "to_account": random.randint(0, 9),
              "amount": 1,
              "ts": datetime.datetime.now().isoformat(sep=" ", timespec="milliseconds")}
  #
  if random.randint(0, 9) == 9:
    delayed_row_dict_list.append(row_dict)
    delayed_int += 1
  else:
    print(row_dict)
    p.produce(json.dumps(row_dict), key=str(id_int))
    #
    if id_int % 1000 == 0 or id_int == 9999:
      random.shuffle(delayed_row_dict_list)
      for delayed_row_dict in delayed_row_dict_list:
        print(f"delayed: {delayed_row_dict}")
        p.produce(json.dumps(delayed_row_dict), key=str(delayed_row_dict["id"]))
      #
      delayed_row_dict_list = []
      #
      p.flush()
  #
  time.sleep(0.01)
#
p.close()
#
print(delayed_int)
