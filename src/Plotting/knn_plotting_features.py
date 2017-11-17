import numpy as np
import pandas as pd
from sklearn.neighbors import NearestNeighbors
from matplotlib.colors import ListedColormap

import seaborn as sns

import matplotlib.pyplot as plt

df = pd.read_csv("../../resources/mile_transposed_preprocessed.txt", header=0)

selected_features_index = list(6709, 3333)

feature1 = "28990_at"
feature2 = '162963_at'

x = df[feature1].values
y = df[feature2].values

