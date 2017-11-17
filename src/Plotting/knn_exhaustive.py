import numpy as np
import pandas as pd

import seaborn as sns
import matplotlib.pyplot as plt

df = pd.read_csv("../../knn_relief_cleaned.txt", header=0)

df_relief = df.loc[df['genes_selection_method'] == 'relief']
df_random = df.loc[df['genes_selection_method'] == 'random']

#fig, ax = plt.subplots()

plt.scatter(1 - df_relief.filter(items=['class2_true_positive_rate']), df_relief.filter(items=['class1_true_positive_rate']), color="b", alpha=.1)
#ax.scatter(1 - df_random['class1_true_positive_rate'], df_random['class2_true_positive_rate'], color="r", alpha=.1)
plt.show()