from sqlalchemy import create_engine, Integer, Date, Float, String, ForeignKey, Column, select
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv
from datetime import datetime


engine = create_engine("sqlite:///stations_database.db", echo=True)
Base = declarative_base()


class CleanStation(Base):  
    __tablename__ = 'clean_station'

    station = Column(String, primary_key=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    elevation = Column(Float, nullable=False)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    state = Column(String, nullable=False)

class CleanMeasure(Base):
    __tablename__ = 'clean_measure'

    id = Column(Integer, primary_key=True )
    station = Column(String, ForeignKey('clean_station.station'))
    date = Column(Date, nullable=False)
    precip = Column(Float, nullable=False)
    tobs = Column(Integer, nullable=False)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

clean_station_url = "clean_stations.csv" 
clean_measure_url = "clean_measure.csv"

with open(clean_station_url, 'r') as cs:
    cs_all = csv.DictReader(cs)
    for row in cs_all:
        new_row = CleanStation(station=row['station'], latitude=float(row['latitude']), longitude=float(row['longitude']), 
                                elevation=float(row['elevation']), name=row['name'], country=row['country'], state=row['state'])
        session.add(new_row)
        session.commit()
    
with open(clean_measure_url, 'r') as cm:
    cm_all = csv.DictReader(cm)
    for row in cm_all:
        new_row = CleanMeasure(station=row['station'], date=datetime.strptime((row['date']), '%Y-%m-%d'), 
                               precip=float(row['precip']), tobs=int(row['tobs']))
        session.add(new_row)
        session.commit()

conn = engine.connect()

query = select(CleanMeasure, CleanStation.latitude, CleanStation.elevation, CleanStation.name, CleanStation.country, CleanStation.state
               ).join(CleanStation, CleanMeasure.station==CleanStation.station)

results =  conn.execute(query).fetchall() 

for row in results:
    print('\n', row)

conn.close()
session.close()



