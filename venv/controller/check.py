import pandas as pd

file_path = r"venv\\data\\raw\\thunderstorms_20251118222608_3.csv"

df = pd.read_csv(file_path)

print(df.head(20))
print(df.info())
