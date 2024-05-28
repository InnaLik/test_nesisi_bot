import asyncio
from typing import AsyncGenerator

from sqlalchemy import Column, Integer, String, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy.orm import DeclarativeBase

engine = create_async_engine('sqlite+aiosqlite:///bot_nesibintelk.db', echo=True)
sessionmaker = async_sessionmaker(engine, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


class Name(Base):
    """
    отображение имен участников, чтобы данные имена нельзя было добавить для реагирования
    """
    __tablename__ = 'name'
    id = Column(Integer, primary_key=True)
    name = Column(String)

    def __str(self):
        return f'{self.name}'

class BadWords(Base):
    """
    слова, на которые будет реагировать бот
    """
    __tablename__ = 'bad_words'
    id = Column(Integer, primary_key=True)
    word = Column(String)


class Birthday(Base):
    """
    дни рождения участников группы
    """
    __tablename__ = 'birthdays'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    date = Column(String)


class Boys(Base):
    """
    количество слов, на которые среагировал бот от каждого участника группы
    """
    __tablename__ = 'boys'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    count = Column(Integer)
    nick = Column(String)


class Holidays(Base):
    """
    праздники
    """
    __tablename__ = 'holidays'
    id = Column(Integer, primary_key=True)
    date = Column(String)
    celebrate = Column(String)


class Phrases(Base):
    """
    фразы, которыми бот отвечает
    """
    __tablename__ = 'phrases'
    id = Column(Integer, primary_key=True)
    phrase = Column(String)
#
# conn = engine.connect()
# query = select(Phrases)
# r = conn.execute(query)
# print(r.mappings().all())

