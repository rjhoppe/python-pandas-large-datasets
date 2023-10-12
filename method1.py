#!pip install memory_profiler
# %load_ext memory_profiler

import pandas as pd
from sqlalchemy import create_engine
import sys

suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']

# Converts bytes to human readable format
def convert_bytes(nbytes):
  i = 0
  while nbytes >= 1024 and i < len(suffixes)-1:
    nbytes /= 1024.
    i += 1
  f = ('%.2f' % nbytes).rstrip('0').rstrip()
  return '%s %s' % (f, suffixes[i])

# Load data from db to pandas df
def data_processing_using_pandas_01():
  engine = create_engine("postgresql://etl:demopass@localhost/AdventureWorks")
  df = pd.read_sql_query('SELECT * FROM large_dataset', engine)
  print(f"Dataframe total rows: {len(df)} ")
  print(df.memory_usage(deep=True).sum()/1024)
  print(sys.getsizeof(df)/1024)
  print('Total memory consumption ' + convert_bytes(df.memory_usage(index=True, deep=True).sum()))

# %memit data_processing_using_pandas_01()

# Processes all the data at once and uses high amount of memory

