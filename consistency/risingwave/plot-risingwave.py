import matplotlib.pyplot as plt
import pandas as pd

from kafi.kafi import *

c = Cluster("local")
int_float_tuple_list = c.map("total_risingwave", key_type="json", map_function=lambda message_dict: (message_dict["timestamp"][1], message_dict["value"]["sum"]), offsets={0:1})[0]
df = pd.DataFrame(int_float_tuple_list, columns=["timestamp", "total"])
df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
df.plot.scatter(x="timestamp", y="total")
df.to_csv("total_risingwave.csv")
plt.show()
