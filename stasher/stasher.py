from sqlalchemy import Column, Integer, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class FileHash(Base):
    __tablename__ = 'hashes'

    id = Column(Integer, primary_key=True)
    hash = Column(String(130))

db_file = 'test.db'
# db_file = ':memory:'
db = create_engine(f'sqlite:///{db_file}')

new_hash = FileHash(hash='D62B8B4FC68ED9B0A7465B2D9BD76BF5649F068853A370CB90AF9126D3C713D6720704ED5C7AD90B502BD571E06A3F648850437D4365A62BDBAE61A6A952BA87')
db_session = sessionmaker(bind=db)

session = db_session()

session.add(new_hash)
session.commit()
