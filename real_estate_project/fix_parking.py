# src/fix_parking.py
import pandas as pd
import numpy as np
import os

IN_PATH = "data/final_data.csv"
OUT_PATH = IN_PATH  # overwrite in place (or change to final_data_fixed.csv to keep original)

if not os.path.exists(IN_PATH):
    raise FileNotFoundError(IN_PATH + " not found.")

df = pd.read_csv(IN_PATH)

# Replace infinities, just in case
df = df.replace([np.inf, -np.inf], np.nan)

# Check Parking_Space exists
if "Parking_Space" in df.columns:
    # Fill with 0 (no parking) — safer than leaving all-NaN for imputer
    df["Parking_Space"] = df["Parking_Space"].fillna(0)
    print("Filled Parking_Space NaNs with 0.")
else:
    print("Column Parking_Space not present — nothing to do.")

# Save
df.to_csv(OUT_PATH, index=False)
print("Saved cleaned data to:", OUT_PATH)
print("Sample values for Parking_Space:", df["Parking_Space"].head(5).tolist())
