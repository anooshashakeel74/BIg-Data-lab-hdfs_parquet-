import os
import pandas as pd
from hdfs import InsecureClient

# HDFS settings (can also come from .env)
HDFS_HOST = os.getenv("HDFS_HOST", "http://namenode:9870")  # replace 'namenode' with your actual HDFS hostname
HDFS_PATH = os.getenv("HDFS_PATH", "/user/root/students.parquet")

# Local Parquet file
LOCAL_FILE = "data/students.parquet"
os.makedirs(os.path.dirname(LOCAL_FILE), exist_ok=True)

# Step 1: initial student data
students = [
    {"id": 7, "name": "Anoosha", "age": 21},
    {"id": 4, "name": "Noor", "age": 23}
]

df = pd.DataFrame(students)
df.to_parquet(LOCAL_FILE, index=False)
print("Initial data stored locally:")
print(df)

# Step 2: Connect to HDFS and upload
client = InsecureClient(HDFS_HOST, user='root')
client.upload(HDFS_PATH, LOCAL_FILE, overwrite=True)
print(f"✅ Uploaded to HDFS at {HDFS_PATH}")

# Step 3: Example: update a row (change Bob's name and age)
df.loc[df["id"] == 2, "name"] = "Bobby"
df.loc[df["id"] == 2, "age"] = 22
df.to_parquet(LOCAL_FILE, index=False)
client.upload(HDFS_PATH, LOCAL_FILE, overwrite=True)
print("\nAfter update stored in HDFS:")
print(df)

# Step 4: Example: delete a row (remove Alice)
df = df[df["id"] != 1]
df.to_parquet(LOCAL_FILE, index=False)
client.upload(HDFS_PATH, LOCAL_FILE, overwrite=True)
print("\nAfter deletion stored in HDFS:")
print(df)
