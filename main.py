import aiosqlite

from aiogram import Bot, Dispatcher
from aiogram.dispatcher.filters import Command
from aiogram.types import Message
from aiogram import types
import logging
from string import punctuation
from dataclasses import dataclass
from pycbrf import ExchangeRates
import pandas as pd
from datetime import datetime
import asyncio
import aioschedule
from aiogram.types import *
from aiogram import executor


with open('token.txt') as file:
    API_TOKEN: str = file.read()

# Создаем объекты бота и диспетчера
bot: Bot = Bot(token=API_TOKEN)
dp: Dispatcher = Dispatcher(bot)

# запись лога от уровня INFO и выше в файл py_log.log + записывается время
logging.basicConfig(level=logging.INFO, filename="py_log.log",
                    format="%(asctime)s %(levelname)s %(message)s")


# Этот хэндлер будет срабатывать на команду "/start"
@dp.message_handler(Command(commands=["start"]))
async def process_start_command(message: Message):
    await message.answer(f'Привет, {message}')
    # await bot.send_message(chat_id='ID или название чата', text='Какой-то текст')


# при вызове команды help
@dp.message_handler(Command(commands=["help"]))
async def process_help_commands(message: Message):
    mess = f'<b>/add</b> - добавляет фразу\n<b>/del</b> - удаляет фразу\n<b>/all_phrases</b> - показывает все фразы в каталоге\n<b>/all_course</b> - покажет текущие курсы валют' \
           f'\n<b>/del_bad</b> - удалит слово\n<b>/add_bad</b> - добавит слово'
    await bot.send_message(message.chat.id, mess, parse_mode='html')


# при вызове команды all_course
@dp.message_handler(Command(commands=['all_course']))
async def process_all_course_command(message: Message):
    """при вызове команды all_course бот вернет в чат с пользователем сообщение о всех курсах
    на сегодняшний день"""
    await bot.send_message(message.chat.id, all_course_class.get())


# при вызове команда all_phrases
@dp.message_handler(Command(commands=['all_phrases']))
async def process_all_phrases_command(message: Message):
    """вернет пользователю в чат все фразы из бд"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        database_cursor = await database.cursor()
        await database_cursor.execute(f'Select phrase from phrases')
        mess = '\n'.join(i[0] for i in await database_cursor.fetchall())
        await  bot.send_message(message.chat.id, mess)
        await database.commit()


# при вызове команды add
@dp.message_handler(Command(commands=['add']))
async def process_add_command(message: Message):
    """добавит фразу в бд"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        phrase = ' '.join(message.text.split()[1:])
        if len(phrase) > 0:
            await database.execute(f"INSERT INTO phrases (phrase) VALUES (?)", (phrase,))
            await bot.send_message(message.chat.id, f'фраза "{phrase}" добавлена')
        else:
            await bot.send_message(message.chat.id, f'фраза не должна быть пустой')
        #без это строчки в конце данные не сохранятся в бд, она обязательна
        await database.commit()


# при вызове команды del
@dp.message_handler(Command(commands=['del']))
async def process_del_command(message: Message):
    """удалит фразу из бд"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        # database_cursor = database.cursor()
        phrase = ' '.join(message.text.split()[1:])
        #сохраняем id фразы, если она была найдена, обязательно в переменну, иначе не сможем через fetch обратиться
        answer = await database.execute('SELECT COUNT(id) from phrases where phrase = ?', (phrase,))
        answer_database = await answer.fetchone()
        if answer_database != (0,):
            await database.execute(f'DELETE FROM phrases WHERE phrase = ?', (phrase,))
            await bot.send_message(message.chat.id, f'фраза "{phrase}" удалена')
        else:
            await bot.send_message(message.chat.id, f'фразы не найдено, повторите попытку')
        await database.commit()


@dp.message_handler(Command(commands=['add_bad']))
async def process_add_bad_command(message: Message):
    """добавит слово, на которое бот будет реагировать в бд"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        word = ' '.join(message.text.lower().split()[1:])
        answer = await database.execute(f'SELECT count(name) from NAME where name = ?', (word,))
        answer_database = await answer.fetchone()
        if answer_database == (0,) and len(word) != 0:
            await database.execute(f'INSERT INTO bad_words (word) VALUES (?)', (word,))
            await bot.send_message(message.chat.id, f'слово <b>{word}</b> добавлено', parse_mode='html')
        else:
            await bot.send_message(message.chat.id, f'Такие слова не добавляю')
        await database.commit()

@dp.message_handler(Command(commands=['del_bad']))
async def process_del_bad_command(message: Message):
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        word = ' '.join(message.text.lower().split()[1:])
        answer = await database.execute(f'SELECT count(id) from bad_words where word = ?', (word,))
        answer_database = await answer.fetchone()
        if answer_database != (0,):
            await database.execute(f'DELETE FROM bad_words WHERE word = ?', (word,))
            await bot.send_message(message.chat.id, f'слово "{word}" удалено')
        else:
            await bot.send_message(message.chat.id, f'Такого слова нет')
        await database.commit()


@dp.message_handler(Command(commands=['taboo']))
async def process_taboo_command(message: Message):
    """дeйствия при вызове команды taboo - добавляет слова в таблицу NAME, чтобы эти слова потом нельзя было
        добавить в таблицу bad_words"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        word = ' '.join(message.text.lower().split()[1:])
        await database.execute(f'INSERT INTO NAME (name) VALUES (?)', (word,))
        await bot.send_message(message.chat.id,
                               f'слово "{word}" добавлено в список исключений, его нельзя будет добавить в таблицу bad_words')
        await database.commit()


@dp.message_handler(Command(commands=['taboo_del']))
async def process_taboo_del_command(message: Message):
    """дeйствия при вызове команды taboo_del - удаляет слово из таблицы NAME"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        word = ' '.join(message.text.lower().split()[1:])
        await database.execute(f'DELETE FROM NAME WHERE name = ?', (word,))
        await bot.send_message(message.chat.id,
                               f'слово "{word}" удалено из исключений и его можно добавлять в таблицу bad_words')
        await database.commit()

@dp.message_handler(Command(commands=['taboo_all']))
async def process_taboo_all_command(message: Message):
    """действия при вызове комканды taboo_all - покажет список всех исключений"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        answer = await database.execute(f'Select name from NAME')
        answer = await answer.fetchall()
        answer_database = '\n'.join([i[0] for i in answer])
        await bot.send_message(message.chat.id, answer_database)
        await database.commit()
@dp.message_handler()
async def all_text(message: Message):
    """обработка текстовых сообщений"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        mess = message.text.lower().split()
        list_word = [i.strip(punctuation) for i in mess]
        count_word = ['?' for _ in range(len(list_word))]
        insert_db = ', '.join(count_word)
        #в данной строчке мы берем каждое слово из написанного сообщения и проверяем есть ли слово в таблице bad_words
        #чтобы соответственно понимать отреагировать на сообщение или нет
        answer = await database.execute(f'SELECT id FROM bad_words WHERE word IN ({insert_db})', list_word)
        answer_database = await answer.fetchone()
        if answer_database:
            answer_message = await database.execute(f'select phrase from phrases order by random() limit 1')
            answer_to_mess = await answer_message.fetchall()
            await bot.send_message(message.chat.id, answer_to_mess[0][0])
            await database.execute('UPDATE boys SET count = count + 1 WHERE id = ?', (message.from_user.id,))
        await database.commit()





@dataclass
class Clipboard:

    course_dollar: str = ''
    course_ali: str = ''
    course_euro: str = ''


    @staticmethod
    def get_dollar():
        """для получения курса доллара"""
        rates = ExchangeRates(datetime.now())
        return str(rates['USD'].value)[0:5]

    @staticmethod
    def get_ali():
        """для получения курса али"""
        tables = pd.read_html('https://helpix.ru/currency/')
        for df in tables:
            if 'Aliexpress.ru' in df.columns:
                return df.loc[0, 'Aliexpress.ru']

    @staticmethod
    def get_euro():
        """для получения курса евро"""
        rates = ExchangeRates(datetime.now())
        return str(rates['EUR'].value)[0:5]

    def get_all_course(self):
        self.course_dollar = self.get_dollar()
        self.course_euro = self.get_euro()
        self.course_ali = self.get_ali()

    def get(self):
        return f'курс доллара: {self.course_dollar}\n' \
               f'курс евро: {self.course_euro}\n' \
               f'курс али: {self.course_ali}'


all_course_class = Clipboard()

async def send_course():
    # переделать - создать таблицу со всеми чатами, куда отправлять изменения курсак утром + добавить функционал по
    # добавлению групп
    await bot.send_message(chat_id=-736597313, text=all_course_class.get())
    await bot.send_message(chat_id=-1001214772818, text=all_course_class.get())

async def check_apartment():
    if datetime.now().day == 19:
        await bot.send_message(chat_id=-736597313, text='Подать данные по коммуналке')

async def birthday():
    dates = str(datetime.now().day).rjust(2, '0') + '.' + str(datetime.now().month).rjust(2, '0')
    async with aiosqlite.connect ('bot_nesibintelk.db') as database:
        answer = await database.execute('SELECT name FROM birthday WHERE date = ?', (dates,))
        answer_database = await answer.fetchone()
        if answer_database != (0,):
            await bot.send_message(chat_id=-1001214772818,
                     text=f'Сегодня свой день рождение празднует {answer_database[0]}! Давайте все вместе поздравим его!')

async def greeting():
    await bot.send_message(chat_id=-736597313, text='Доброе утро и хорошего дня!')
    await bot.send_message(chat_id=-1001214772818, text='Доброе утро, 36.6')

async def error():
    await bot.send_message(chat_id=472546754, text='ловим ошибку')

#переделать базу данных
async def check_out_boys():
    async  with aiosqlite.connect('bot_nesibintelk.db') as databasw:
        answer = await database.execute('Select nick, count from boys ORDER BY 2 DESC')
        answer_database= await answer.fetchall()
        text = '\n'.join([f'{i[0]} : {i[1]}' for i in answer_database])
        await bot.send_message(chat_id=472546754, text=f'Общая статистика: \n{text}')

schedule.every().day.at("08:00").do(birthday)
schedule.every().day.at('11:10').do(send_course)
schedule.every().day.at('12:00').do(check_apartment)
schedule.every().monday.at('17:00').do(check_out_boys)




async def scheduler():
    aioschedule.every(1).hours.do(error)
    aioschedule.every().day.at('09:00').do(greeting())
    schedule.every().day.at("08:00").do(birthday)
    schedule.every().day.at('11:10').do(send_course)
    schedule.every().day.at('12:00').do(check_apartment)
    schedule.every().friday.at('17:00').do(check_out_boys)

    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)
#
async def on_startup(_):
    asyncio.create_task(scheduler())

#запуск бота и времени
if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=False, on_startup=on_startup)