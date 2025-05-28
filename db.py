from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base


DATABASE_URL = "postgresql+psycopg2://username:password@localhost:5432/your_database" #TODO: put actual databse URL here
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)


def init_db():
    Base.metadata.create_all(bind=engine)