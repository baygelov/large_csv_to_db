import pandas as pandas
from config import *

for i,chunk in enumerate(pandas.read_csv(CSV_PATH, chunksize=500000)):
    chunk.to_csv('chunk{}.csv'.format(i), index=False)