import pandas as pd
import json

with open('./deps_graph.json', 'r') as file:
    data = json.load(file)

# This handles nested and irregular JSON structures
df = pd.json_normalize(data)
df.to_csv('output.csv', index=False)
