from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date
from sqlalchemy.orm import declarative_base


Base = declarative_base()


def generate_engine(sqlalchemy_connection_details="sqlite:///nicd_data.db"):

    engine = create_engine(sqlalchemy_connection_details, echo=True)

    class HospitaliastionData(Base):
        __tablename__ = "hospitalisation"

        id = Column(Integer, primary_key=True)
        facilities_reporting = Column(String)
        admissions_to_date = Column(Integer)
        died_to_date = Column(Integer)
        discharged_to_date = Column(Integer)
        currently_admitted = Column(Integer)
        currently_in_icu = Column(Integer)
        currently_ventilated = Column(Integer)
        currently_oxygenated = Column(Integer)
        admissions_in_previous_day = Column(Integer)
        date = Column(Date)

    Base.metadata.create_all(engine)

    return engine
