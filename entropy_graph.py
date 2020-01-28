import pandas as pd
import matplotlib.pyplot as plt
import numpy as np


df = pd.read_csv('synapse.json.USlog.entropy')
X = sorted(list(df['object_kse'].values))
Y = list(np.arange(len(df['name'].values)))
plt.scatter(x=X,y=Y)
plt.show()