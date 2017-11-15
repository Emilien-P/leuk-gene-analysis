import pandas as pd
import sklearn.preprocessing as prep
df = pd.read_csv("../../resources/mile_transposed.csv", index_col=0)

df = (df - df.mean()) / df.std() #standardize each feature, should we normalize instead?

df.to_csv("../../resources/mile_transposed_preprocessed.csv")