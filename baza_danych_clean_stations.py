from sqlalchemy import Table, Column, Integer, String, MetaData
from sqlalchemy import create_engine
import csv

engine = create_engine('sqlite:///clean_database.db')

meta = MetaData()

#zdefiniowanie tabel i jej kolumn
stations = Table(
   'stations', meta,
   Column('id', Integer, primary_key=True),
   Column('station', String),
   Column('latitude', Integer),
   Column('longitude', Integer),
   Column('elevation', Integer),
   Column('name', String),
   Column('country', String),
   Column('state', String),
)

measures = Table(
   'measures', meta,
   Column('id',Integer, primary_key=True),
   Column('station', String),
   Column('date', String),
   Column('precip', Integer),
   Column('tobs', Integer),
)

#stworzenie tabeli
meta.create_all(engine)

#Utworzenie polaczenia z baza
conn = engine.connect()

#Pobranie danych z plikow csv
with open('clean_stations.csv', newline='') as stations_csv:
   station_reader = csv.reader(stations_csv, quoting=csv.QUOTE_NONE)
   for row in station_reader:
      conn.execute(stations.insert().values(station=row[0],latitude=row[1],longitude=row[2],elevation=row[3],name=row[4],country=row[5],state=row[6]))

with open('clean_measure.csv', newline='') as measures_csv:
   measures_reader = csv.reader(measures_csv,)
   for row in measures_reader:
      conn.execute(measures.insert().values(station=row[0],date=row[1],precip=row[2],tobs=row[3]))




print(conn.execute("SELECT * FROM measures LIMIT 5").fetchall())