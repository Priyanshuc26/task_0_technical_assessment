import pandas as pd
import csv

# Forcing pandas to ignore quotes entirely when splitting
df = pd.read_csv('data.csv', quoting=csv.QUOTE_NONE)

# Clean the column headers (remove leftover quotes and spaces)
df.columns = df.columns.str.replace('"', '').str.strip().str.lower()

# Clean the first and last column's data (they have leftover quotes too)
df['volume_usd'] = df['volume_usd'].astype(str).str.replace('"', '')

df['timestamp'] = df['timestamp'].astype(str).str.replace('"', '')
# Ensure our 'close' column is strictly numerical for the math
df['close'] = pd.to_numeric(df['close'], errors='coerce')
print(df.shape)