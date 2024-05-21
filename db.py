from sqlalchemy import create_engine, Column, String, Integer, select
from sqlalchemy.orm import DeclarativeBase

engine = create_engine('sqlite:///bot_nesibintelk.db')


class Base(DeclarativeBase):
    pass


class NameNotUse(Base):
    __tablename__ = 'name_not_use'
    id = Column(Integer, primary_key=True)
    name = Column(String)


class BadWords(Base):
    __tablename__ = 'bad_words'
    id = Column(Integer, primary_key=True)
    word = Column(String)


class Birthday(Base):
    __tablename__ = 'birthday'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    date = Column(String)


class Boys(Base):
    __tablename__ = 'boys'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    count = Column(Integer)
    nick = Column(String)


class Holiday(Base):
    __tablename__ = 'holiday'
    id = Column(Integer, primary_key=True)
    date = Column(String)
    celebrate = Column(String)


class Phrases(Base):
    __tablename__ = 'phrases'
    id = Column(Integer, primary_key=True)
    phrase = Column(String)

conn = engine.connect()
query = select(NameNotUse)
r = conn.execute(query)
print(r.mappings().all())
