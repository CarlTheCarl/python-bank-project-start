from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base


DATABASE_URL = "postgresql+psycopg2://postgres:password123@localhost:5432/sebank" #replace this bit with the url to your actual postgreSQL server
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    Base.metadata.create_all(bind=engine)