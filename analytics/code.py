import json
from datetime import datetime
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, DateTime, JSON, MetaData, select, func

# Define the connection parameters for PostgreSQL and MySQL
POSTGRES_HOST = 'postgres'
POSTGRES_PORT = 5432
POSTGRES_DB = 'mydatabase'
POSTGRES_USER = 'myuser'
POSTGRES_PASSWORD = 'mypassword'

MYSQL_HOST = 'mysql'
MYSQL_PORT = 3306
MYSQL_DB = 'mydatabase'
MYSQL_USER = 'myuser'
MYSQL_PASSWORD = 'mypassword'

# Define the function for calculating distance between two locations
def calculate_distance(lat1, lon1, lat2, lon2):
    from math import acos, sin, cos
    distance = acos(sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(lon2 - lon1)) * 6371
    return distance

# Connect to PostgreSQL and MySQL using SQLAlchemy
postgres_engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}')
postgres_conn = postgres_engine.connect()

mysql_engine = create_engine(f'mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}')
mysql_conn = mysql_engine.connect()

metadata = MetaData()

# Define the devices table for PostgreSQL
devices_postgres = Table('devices', metadata,
    Column('device_id', String, primary_key=True),
    Column('temperature', Integer),
    Column('location', JSON),
    Column('time', DateTime)
)

# Define the devices_aggregated table for MySQL
devices_aggregated_mysql = Table('devices_aggregated', metadata,
    Column('device_id', String, primary_key=True),
    Column('hour', DateTime, primary_key=True, default=func.now()),
    Column('max_temperature', Integer),
    Column('data_points', Integer),
    Column('total_distance', Float),
)

# Extract, transform, and load the data
devices = postgres_conn.execute(select([devices_postgres])).fetchall()

for device in devices:
    device_id = device.device_id
    temperature = device.temperature
    location = device.location
    lat = location.get('lat')
    lon = location.get('lon')
    time = device.time
    hour = time.replace(minute=0, second=0, microsecond=0)

    # Calculate the maximum temperature, the count of data points, and the total distance for each device and hour
    result = mysql_conn.execute(select([devices_aggregated_mysql]).where(devices_aggregated_mysql.c.device_id == device_id).where(devices_aggregated_mysql.c.hour == hour)).fetchone()
    if result:
        max_temperature = result.max_temperature
        if max_temperature is None or temperature > max_temperature:
            max_temperature = temperature
        data_points = result.data_points + 1
        total_distance = result.total_distance
        if lat and lon:
            prev_lat = result.location.get('lat')
            prev_lon = result.location.get('lon')
            if prev_lat and prev_lon:
                distance = calculate_distance(prev_lat, prev_lon, lat, lon)
                total_distance += distance
        location_dict = {'lat': lat, 'lon': lon}
        mysql_conn.execute(devices_aggregated_mysql.update().where(devices_aggregated_mysql.c.device_id == device_id).where(devices_aggregated_mysql.c.hour == hour).values(max_temperature=max_temperature, data_points=data_points, total_distance=total_distance, location=location_dict))
