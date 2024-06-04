from typing import Annotated

from sqlalchemy import Column, Integer, String, select
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

engine = create_async_engine(url='sqlite+aiosqlite:///bot_nesibintelk.db',
                             echo=True)
async_session = async_sessionmaker(engine)

intpk = Annotated[int, mapped_column(primary_key=True)]
class Base(DeclarativeBase):
    pass


class Name(Base):
    """
    отображение имен участников, чтобы данные имена нельзя было добавить
    для реагирования
    """
    __tablename__ = 'name'
    id: Mapped[intpk]
    name: Mapped[str]


class BadWords(Base):
    """
    слова, на которые будет реагировать бот
    """
    __tablename__ = 'bad_words'
    id: Mapped[intpk]
    word: Mapped[str]


class Birthday(Base):
    """
    дни рождения участников группы
    """
    __tablename__ = 'birthdays'
    id: Mapped[intpk]
    name: Mapped[str]
    date: Mapped[str]


class Boys(Base):
    """
    количество слов, на которые среагировал бот от каждого участника группы
    """
    __tablename__ = 'boys'
    id: Mapped[intpk]
    name: Mapped[str]
    count: Mapped[int]
    nick: Mapped[str]


class Holidays(Base):
    """
    праздники
    """
    __tablename__ = 'holidays'
    id: Mapped[intpk]
    date: Mapped[str]
    celebrate: Mapped[str]


class Phrases(Base):
    """
    фразы, которыми бот отвечает
    """
    __tablename__ = 'phrases'
    id: Mapped[intpk]
    phrase: Mapped[str]
