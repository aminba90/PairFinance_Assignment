from os import environ
from time import sleep
from sqlalchemy import create_engine , MetaData ,Table, Column, Integer, String, Float,DateTime, func, select
from sqlalchemy.exc import OperationalError
import math
import datetime
import json
import pandas as pd
import ast

print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')



# Distance calculation function
def calculate_distance(row):
    lat1 = math.radians(float(row['latitude']))
    lon1 = math.radians(float(row['longitude']))
    lat2 = math.radians(float(row['lead_lat']))
    lon2 = math.radians(float(row['lead_long']))
    distance = math.acos(math.sin(lat1) * math.sin(lat2) + math.cos(lat1) * 
                         math.cos(lat2) * math.cos(lon2 - lon1)) * 6371
    return distance

# Connect to PostgresSQL and select all records from devices table
postgres_conn = psql_engine.connect()


metadata = MetaData()

metadata_dest = MetaData()
#TEST
devices = Table(
    'devices', metadata,
    Column('device_id', String),
    Column('temperature', Integer),
    Column('location', String),
    Column('time', String),
)
#FINISH TEST
# Define the devices_aggregated table for MySQL
devices_aggregated_mysql = Table('devices_aggregated', metadata_dest,
    Column('device_id', String, primary_key=True),
    Column('hour', Integer, primary_key=True, default=func.now()),
    Column('max_temperature', Integer),
    Column('data_points', Integer),
    Column('total_distance', Float),
)
s = devices.select()
devices_ = postgres_conn.execute(s)

row = devices_.fetchall()
df = pd.DataFrame(row)

#calculate hour based on time.time
df['time'] = round(df['time'].astype(int) / 3600)

# flatten "geo" column
df['location'] = df['location'].apply(lambda x: json.loads(x))
df_geo = pd.json_normalize(df['location'])


# add latitude and longitude columns to original dataframe
df['latitude'] = df_geo['latitude']
df['longitude'] = df_geo['longitude']

# drop original "geo" column
df.drop('location',axis=1 , inplace=True)


#calculate The maximum temperatures measured for every device per hours.
df_max_temperature = df.groupby(['device_id', 'time']).agg({'temperature': 'max'}).reset_index()

#calculate amount of data points aggregated for every device per hours.
df_cnt_device = df.groupby(['device_id', 'time']).agg({'temperature': 'count'}).reset_index()
df_cnt_device=df_cnt_device.rename(columns={'temperature': 'count'})

# Sort DataFrame by column C and B
df = df.sort_values(['device_id', 'time'])

# Calculate lead of column A partitioned by column C
df['lead_lat'] = df.groupby('device_id')['latitude'].shift(-1)
df['lead_long'] = df.groupby('device_id')['longitude'].shift(-1)

#Calculate Distance and add it to the dataframe

df['distance'] = df.apply(lambda row: calculate_distance(row) , axis=1)

#calculate distance of device movement for every device per hours.
df_tot_device_distance = df.groupby(['device_id', 'time']).agg({'distance': 'sum'}).reset_index()

#join intermediate dataframes
df_merge_1  = pd.merge(df_max_temperature, df_cnt_device,on=['device_id', 'time'])
df_merge_2  = pd.merge(df_merge_1, df_tot_device_distance,on=['device_id', 'time'])


# Connect to MySQL and create a new table
while True:
    try:
         msql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
         metadata_dest.create_all(msql_engine)
         break
    except OperationalError:
        sleep(0.1)


# Insert the transformed data into the MySQL table

ins = devices_aggregated_mysql.insert()
with msql_engine.connect() as conn:
        while True:
             conn.execute(ins, df_merge_2)
             conn.commit()
             conn.close()
             print("Final Data successfully inserted to destination table")

#Closing the Transactions
postgres_conn.close()
