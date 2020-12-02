import os
import time
import random
import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

DB_FILENAME = 'Database'
engine = create_engine(f'sqlite:///{DB_FILENAME}.db', convert_unicode=True)
session = sessionmaker(autocommit=False,
                       autoflush=False,
                       bind=engine)
s_session = scoped_session(session)
Base = declarative_base()
Base.query = s_session.query_property()
db_session = s_session()
