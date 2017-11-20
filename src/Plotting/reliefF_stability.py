import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

df = pd.read_csv("../../results/reliefF_stability_250.txt", header=None, index_col=0)

df = df.drop(17789, 1)

df_sorted = df.apply(lambda row: sorted(enumerate(row), key=lambda k: -k[1]), axis=1)

#Take the 1% features selected
k=1
n = 250

df_sel = df_sorted.iloc[:, 0:k]

final = df_sel.iloc[n-1,:]
final_set = set(map(lambda x: x[0], final.values))

def hamming_with_final(row, final):
	dis = 0
	for idx, val in enumerate(row):
		if val[0] != final.iloc[idx][0]:
			dis = dis + 1
	return dis

def Jaccard_sim(row, final_set):
	row_set = set(map(lambda v: v[0], row.iloc[:k].values))
	return  float(len(row_set.intersection(final_set))) / float(len(row_set.union(final_set)))

df_hamm = df_sel.apply(lambda row: hamming_with_final(row, final), axis=1)

df_jacc = df_sel.apply(lambda row: Jaccard_sim(row, final_set), axis=1)

print(df_hamm)
print(df_jacc)
print(df_sorted.iloc[95:99, :])

sns.set()
ax = plt.stem(df_hamm)
plt.xlabel("n")
plt.ylabel("Hamming distance with the last iteration")
plt.show()