from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date
from sqlalchemy.orm import declarative_base


Base = declarative_base()

def generate_engine(sqlalchemy_connection_details='sqlite:///nicd_data.db'):
    
    engine = create_engine(sqlalchemy_connection_details, echo=True)

    class HospitaliastionData(Base):
        __tablename__ = 'hospitalisation'

        id = Column(Integer, primary_key=True)
        province = Column(String)
        facilities_reporting = Column(Integer)
        admissions_to_date = Column(Integer)
        deaths_to_date = Column(Integer)
        discharged_to_date = Column(Integer)
        currently_admitted = Column(Integer)
        current_in_icu = Column(Integer)
        currently_ventilated = Column(Integer)
        date = Column(Date)
        died_to_date = Column(Integer)
        currently_oxygenated = Column(Integer)
        admissions_in_previous_day = Column(Integer)

    Base.metadata.create_all(engine)
    
    return engine
