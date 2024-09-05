from sqlalchemy import create_engine, MetaData, Integer, Date, Float, String, Table, Column
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import csv
from datetime import datetime


engine = create_engine("sqlite:///stations_database.db", echo=True)
Base = declarative_base()


class Clean_station(Base):
    __tablename__ = 'clean_station'

    station = Column(String, primary_key=True)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)
    elevation = Column(Float, nullable=False)
    name = Column(String, nullable=False)
    country = Column(String, nullable=False)
    state = Column(String, nullable=False)

class Clean_measure(Base):
    __tablename__ = 'clean_measure'

    id = Column(Integer, primary_key=True )
    station = Column(String)
    date = Column(Date, nullable=False)
    precip = Column(Float, nullable=False)
    tobs = Column(Integer, nullable=False)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

clean_station_url = r"C:\Python\Kodilla\Module_6\clean_stations.csv"
clean_measure_url = r"C:\Python\Kodilla\Module_6\clean_measure.csv"

with open(clean_station_url, 'r') as cs:
    cs_all = csv.reader(cs)
    num =0
    for row in cs_all:
        if num == 0:
            pass
        else:
            new_row = Clean_station(station=row[0], latitude=float(row[1]), longitude=float(row[2]), elevation=float(row[3]), name=row[4], country=row[5], state=row[6])
            session.add(new_row)
            session.commit()
        num += 1
    
with open(clean_measure_url, 'r') as cs:
    cs_all = csv.reader(cs)
    num =0
    for row in cs_all:
        if num == 0:
            pass
        else:
            new_row = Clean_measure(station=row[0], date=datetime.strptime((row[1]), '%Y-%m-%d'), precip=float(row[2]), tobs=int(row[3]))
            session.add(new_row)
            session.commit()
        num += 1
    
session.close()

