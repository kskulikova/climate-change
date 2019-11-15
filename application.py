from flask import Flask
import mysql.connector as mysql
import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DECIMAL

# Configure application
app = Flask(__name__)
# Specify db connection
# db = mysql.connect(
#     user=os.environ.get("USER"),
#     password=os.environ.get("PASSWORD"),
#     host=os.environ.get("HOSTNAME"),
#     database=os.environ.get("DBNAME"),
#     raise_on_warnings=True)
engine = create_engine('mysql+mysqlconnector://'
                       +os.environ['USER']+':'+os.environ['PASSWORD']+'@'
                       +os.environ['HOST']+':'+os.environ['MYSQL_PORT']+'/mstemp', echo=False)
meta = MetaData()
# meta.reflect(bind=engine)

climate = Table(
   'c', meta,
   Column('stn', String(255), primary_key = True),
   Column('latitude', String(255)),
   Column('longitude', String(255)),
   Column('temperature', DECIMAL),
   Column('year', Integer),
   Column('month', Integer),
   Column('day', Integer)
)

meta.create_all(engine)



# cursor = engine.cursor()
# cursor.execute("CREATE TABLE climate_db(stn VARCHAR(255) NOT NULL PRIMARY KEY,"
#                " latitude DECIMAL(10, 8) NOT NULL, longitude DECIMAL(11, 8) NOT NULL,"
#                " temperature DECIMAL(5,2), year int, month int, day int)")


file = 'stations.csv'
colnames =['stn', 'wban', 'latitude', 'longitude']
df_stations = pd.read_csv(file, names=colnames, header=None)
df_stations = df_stations.dropna(subset=['stn', 'latitude', 'longitude'])

columns = ['stn', 'latitude', 'longitude', 'temperature', 'year', 'month', 'day']
df = pd.DataFrame(columns=columns)

year = 1975
while year < 1976:
    yfile = str(year) + '.csv'
    ycolnames =['stn', 'wban', 'month', 'day', 'temperature']
    df_year = pd.read_csv(yfile, names=ycolnames, header=None)
    df_year = df_year.dropna(subset=['stn', 'month', 'day', 'temperature'])
    # df_year['date'] = [datetime(year=int(year), month=int(df_year.iloc[row]['m']),
    #                           day=int(df_year.iloc[row]['d'])) for row in range(0, len(df_year))]
    df_year['year'] = [year for row in range(0, len(df_year))]
    merged_db = pd.merge(df_stations[['stn', 'latitude', 'longitude']],
                         df_year[['stn', 'temperature', 'year', 'month', 'day']], on='stn')
    df = pd.concat([df, merged_db])
    year += 1

print(df.head())
print(year)

# merged_csv = pd.read_csv('export2.csv', names=colnames)
# print(merged_csv[['stn', 'lat', 'long', 't', 'date']].head())
# merged_csv[['stn', 'lat', 'long', 't', 'date']].to_csv('export2.csv', index=False, header=True)

# df.to_sql(con=engine, name='c', if_exists='replace', index=False)
