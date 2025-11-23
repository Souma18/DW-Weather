import pandas as pd

file_path = r"venv\\data\\raw\\tc-20251121_005650-run1.csv"

df = pd.read_csv(file_path)

print(df.head(20))
print(df.info())
