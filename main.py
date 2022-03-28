from config import *
import csv
import time
import pandas as pandas
from sqlalchemy import create_engine
from pathlib import Path
import sqlite3

def to_chunk(csv_file, chunksize):
    count = 0
    for i,chunk in enumerate(pandas.read_csv(csv_file, chunksize=chunksize)):
        chunk.to_csv('chunk{}.csv'.format(i), index=False)
    ##########
        count += 1
        if count == 3:
            break


def create_table(connection):
    
    connection.execute("""CREATE TABLE Land_Registry_Price (
                Transaction_unique_id, 
                Price, 
                Date_of_Transfer, 
                Postcode, 
                Property_Type, 
                Old_New, 
                Duration, 
                PAON, 
                SAON, 
                Street, 
                Locality, 
                Town_City, 
                District, 
                County, 
                PPD_Category_Type, 
                Record_Status)""")


def insert_into(connection):
    count = 0
    while count < 3:
        with open(f"chunk{count}.csv",'r', encoding='utf-8') as csv_chunk:
                connection.executemany("INSERT INTO Land_Registry_Price VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                    csv.reader(csv_chunk))
        count +=1
    connection.commit()
    connection.close()



def main():
    to_chunk(CSV_PATH, 1000)
    connection = sqlite3.connect(DB_PATH)
    create_table(connection)
    insert_into(connection)
 

if __name__=="__main__":
    main()