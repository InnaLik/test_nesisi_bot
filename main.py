"""
В данном модуле собрана основная логика работы телеграмм-бота
"""
from string import punctuation
from logging import getLogger
from datetime import datetime
from dataclasses import dataclass
import logging
import asyncio
import pyowm
import aiosqlite
import pandas as pd
import aioschedule
from aiogram import Bot, Dispatcher, executor
from aiogram.dispatcher.filters import Command
from pycbrf import ExchangeRates
from aiogram.types import Message
from chat_id import my, sibintek

with open('test_token.txt', encoding='utf-8') as file:
    API_TOKEN: str = file.read()

with open('api_weather.txt', encoding='utf-8') as file:
    API_WEATHER: str = file.read()

# Создаем объекты бота и диспетчера
bot: Bot = Bot(token=API_TOKEN)
dp: Dispatcher = Dispatcher(bot)


async def loggingg():
    """
    Функция для записи логов от уровня INFO и выше
    """
    logger = getLogger(__name__)
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        filename='py_log.log')
    logger.info('INF')
    logger.warning('WAR')
    logger.error('ERR')
    logger.critical('CRI')


@dp.message_handler(Command(commands=["start"]))
async def process_start_command(message: Message):
    """
    Этот handler будет срабатывать на команду "/start"
    """
    await message.answer('Привет')


@dp.message_handler(Command(commands=["help"]))
async def process_help_commands(message: Message):
    """
    При вызове команды /help
    """
    mess = '<b>/add</b> - добавляет фразу\n<b>/del</b> - удаляет фразу\n' \
           '<b>/all_phrases</b> - показывает все фразы в каталоге\n' \
           '<b>/all_course</b> - покажет текущие курсы валют\n' \
           '<b>/del_bad</b> - удалит слово\n<b>/add_bad</b> - добавит слово' \
           '\n<b>/holiday</b> - покажет какой сегодня праздник\n' \
           '<b>/weather</b> - покажет погоду на сегодня'
    await bot.send_message(message.chat.id, mess, parse_mode='html')


# при вызове команды all_course
@dp.message_handler(Command(commands=['all_course']))
async def process_all_course_command(message: Message):
    """При вызове команды all_course бот вернет в чат с
    пользователем сообщение о всех курсах на сегодняшний день"""
    await all_course_class.get_all_course()
    mess = await all_course_class.get()
    await bot.send_message(message.chat.id, mess)


# при вызове команда all_phrases
@dp.message_handler(Command(commands=['all_phrases']))
async def process_all_phrases_command(message: Message):
    """Вернет пользователю в чат все фразы из бд"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        database_cursor = await database.cursor()
        await database_cursor.execute('Select phrase from phrases')
        mess = '\n'.join(i[0] for i in await database_cursor.fetchall())
        await bot.send_message(message.chat.id, mess)
        await database.commit()


# при вызове команды add
@dp.message_handler(Command(commands=['add']))
async def process_add_command(message: Message):
    """добавит фразу в бд"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        phrase = ' '.join(message.text.split()[1:])
        if len(phrase) > 0:
            await database.execute("INSERT INTO phrases (phrase) VALUES (?)",
                                   (phrase,))
            await bot.send_message(message.chat.id,
                                   f'фраза "{phrase}" добавлена')
        else:
            await bot.send_message(message.chat.id,
                                   'фраза не должна быть пустой')
        # без это строчки в конце данные не сохранятся в бд, она обязательна
        await database.commit()


# при вызове команды del
@dp.message_handler(Command(commands=['del']))
async def process_del_command(message: Message):
    """Удалит фразу из бд"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        # database_cursor = database.cursor()
        phrase = ' '.join(message.text.split()[1:])
        # сохраняем id фразы, если она была найдена, обязательно в переменную,
        # иначе не сможем через fetch обратиться
        answer = await database.execute(
            'SELECT COUNT(id) from phrases where phrase = ?', (phrase,))
        answer_database = await answer.fetchone()
        if answer_database != (0,):
            await database.execute('DELETE FROM phrases WHERE phrase = ?',
                                   (phrase,))
            await bot.send_message(message.chat.id,
                                   f'фраза "{phrase}" удалена')
        else:
            await bot.send_message(message.chat.id,
                                   'фразы не найдено, повторите попытку')
        await database.commit()


@dp.message_handler(Command(commands=['add_bad']))
async def process_add_bad_command(message: Message):
    """Добавит слово, на которое бот будет реагировать в бд"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        word = ' '.join(message.text.lower().split()[1:])
        answer = await database.execute(
            'SELECT count(name) from NAME where name = ?', (word,))
        answer_database = await answer.fetchone()
        if answer_database == (0,) and len(word) != 0:
            await database.execute('INSERT INTO bad_words (word) VALUES (?)',
                                   (word,))
            await bot.send_message(message.chat.id,
                                   f'слово <b>{word}</b> добавлено',
                                   parse_mode='html')
        else:
            await bot.send_message(message.chat.id, 'Такие слова не добавляю')
        await database.commit()


@dp.message_handler(Command(commands=['del_bad']))
async def process_del_bad_command(message: Message):
    """
    удалит слово из таблицы bad_words, если оно там есть
    """
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        word = ' '.join(message.text.lower().split()[1:])
        answer = await database.execute(
            'SELECT count(id) from bad_words where word = ?', (word,))
        answer_database = await answer.fetchone()
        if answer_database != (0,):
            await database.execute('DELETE FROM bad_words WHERE word = ?',
                                   (word,))
            await bot.send_message(message.chat.id, f'слово "{word}" удалено')
        else:
            await bot.send_message(message.chat.id, 'Такого слова нет')
        await database.commit()


@dp.message_handler(Command(commands=['taboo']))
async def process_taboo_command(message: Message):
    """дeйствия при вызове команды taboo - добавляет слова в таблицу NAME,
    чтобы эти слова потом нельзя было добавить в таблицу bad_words"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        word = ' '.join(message.text.lower().split()[1:])
        await database.execute('INSERT INTO NAME (name) VALUES (?)', (word,))
        await bot.send_message(message.chat.id,
                               f'слово "{word}" добавлено в список '
                               f'исключений, его нельзя будет добавить в '
                               f'таблицу bad_words')
        await database.commit()


@dp.message_handler(Command(commands=['taboo_del']))
async def process_taboo_del_command(message: Message):
    """дeйствия при вызове команды taboo_del - удаляет слово из таблицы NAME"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        word = ' '.join(message.text.lower().split()[1:])
        await database.execute('DELETE FROM NAME WHERE name = ?', (word,))
        await bot.send_message(message.chat.id,
                               f'слово "{word}" удалено из исключений и его '
                               f'можно добавлять в таблицу bad_words')
        await database.commit()


@dp.message_handler(Command(commands=['taboo_all']))
async def process_taboo_all_command(message: Message):
    """действия при вызове комканды taboo_all -
    покажет список всех исключений"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        answer = await database.execute('Select name from NAME')
        answer = await answer.fetchall()
        answer_database = '\n'.join([i[0] for i in answer])
        await bot.send_message(message.chat.id, answer_database)
        await database.commit()


@dp.message_handler(Command(commands=['holiday']))
async def holiday(message: Message):
    """действия при вызове комканды holiday- покажет
    список праздников сегодня"""
    day_now = datetime.today().day
    month_now = datetime.today().month
    for_select = str(month_now).rjust(2, '0') + '-' + str(day_now)

    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        answer = await database.execute(
            'Select celebrate from holiday where date = ?', (for_select,))
        answer = await answer.fetchall()
        answer_database = '\n'.join([i[0] for i in answer])
        await bot.send_message(message.chat.id,
                               text=f'<b>Какой сегодня праздник</b>'
                                    f'\n{answer_database}',
                               parse_mode='html')
        await database.commit()


@dp.message_handler(Command(commands=['weather']))
async def weather(message: Message):
    """
    Покажет погоду на ближайший час
    """
    own = pyowm.OWM(API_WEATHER)
    mgr = own.weather_manager()
    observation = mgr.weather_at_coords(45.02, 38.59)
    weathers = observation.weather
    # <pyowm.weatherapi25.weather.Weather -
    # reference_time=2024-01-29 06:15:42+00:00,
    # status=clouds, detailed_status=overcast clouds>
    temp = weathers.temperature('celsius')
    # observation = mgr.weather_at_place('Krasnodar')
    # weathers = observation.weather
    # {'temp': 2.32, 'temp_max': 2.32, 'temp_min': 2.32,
    # 'feels_like': 2.32, 'temp_kf': None}
    # temp = weathers.temperature("celsius")
    # print(temp)
    await bot.send_message(message.chat.id,
                           f'<b>Прогноз погоды на сегодня</b>:'
                           f'\nмаксимальная температура '
                           f'<b>{round(temp["temp_max"])}°C</b>'
                           f'\nминимальная температура '
                           f'<b>{round(temp["temp_min"])}°C</b>'
                           f'\n'
                           f'температура сейчас '
                           f'<b>{round(temp["temp"])}°C</b>\n'
                           f'ощущается как '
                           f'<b>{round(temp["feels_like"])}°C</b>',
                           parse_mode='html')


@dp.message_handler()
async def all_text(message: Message):
    """обработка текстовых сообщений"""
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        mess = message.text.lower().split()
        list_word = [i.strip(punctuation) for i in mess]
        count_word = ['?' for _ in range(len(list_word))]
        insert_db = ', '.join(count_word)
        # в данной строчке мы берем каждое слово из написанного
        # сообщения и проверяем есть ли слово в таблице bad_words
        # чтобы соответственно понимать отреагировать на сообщение или нет
        answer = await database.execute(
            f'SELECT id FROM bad_words WHERE word IN ({insert_db})', list_word)
        answer_database = await answer.fetchone()
        if answer_database:
            answer_message = await database.execute(
                'select phrase from phrases order by random() limit 1')
            answer_to_mess = await answer_message.fetchall()
            await bot.send_message(message.chat.id, answer_to_mess[0][0])
            await database.execute(
                'UPDATE boys SET count = count + 1 WHERE id = ?',
                (message.from_user.id,))
        await database.commit()


@dataclass
class Clipboard:
    """
    для получения курса валют
    """
    course_dollar: str = ''
    course_ali: str = ''
    course_euro: str = ''

    @staticmethod
    async def get_dollar():
        """для получения курса доллара"""
        rates = ExchangeRates(datetime.now())
        return str(rates['USD'].value)[0:5]

    @staticmethod
    async def get_ali():
        """для получения курса али"""
        tables = pd.read_html('https://helpix.ru/currency/')
        for df in tables:
            if 'Aliexpress.ru' in df.columns:
                return df.loc[0, 'Aliexpress.ru']

    @staticmethod
    async def get_euro():
        """для получения курса евро"""
        rates = ExchangeRates(datetime.now())
        return str(rates['EUR'].value)[0:5]

    async def get_all_course(self):
        """ Запись курсов в переменные"""
        self.course_dollar = await self.get_dollar()
        self.course_euro = await self.get_euro()
        self.course_ali = await self.get_ali()

    async def get(self):
        """Получение всех курсов"""
        return f'курс доллара: {self.course_dollar}\n' \
               f'курс евро: {self.course_euro}\n' \
               f'курс али: {self.course_ali}'


all_course_class = Clipboard()


async def send_course():
    # переделать - создать таблицу со всеми чатами, куда отправлять
    # изменения курсак утром + добавить функционал по
    # добавлению групп
    """
    Отправка курсов
    """
    await all_course_class.get_all_course()
    mess = await all_course_class.get()
    await bot.send_message(chat_id=my, text=mess)
    await bot.send_message(chat_id=sibintek, text=mess)


async def check_apartment():
    """
    Напоминание по подаче данных
    """
    if datetime.now().day == 19:
        await bot.send_message(chat_id=my, text='Подать данные по коммуналке')


async def birthday():
    """
    Поздравление с днем рождения
    """
    dates = str(datetime.now().day).rjust(2, '0') + '.' + str(
        datetime.now().month).rjust(2, '0')
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        answer = await database.execute(
            'SELECT name FROM birthday WHERE date = ?', (dates,))
        answer_database = await answer.fetchone()
        if answer_database is not None:
            await bot.send_message(chat_id=sibintek,
                                   text=f'Сегодня свой день рождение '
                                        f'празднует {answer_database[0]}! '
                                        f'Давайте все вместе поздравим его!')


async def send_weather():
    """
    Запрос и отправка погоды
    """
    own = pyowm.OWM(API_WEATHER)
    mgr = own.weather_manager()
    observation = mgr.weather_at_place('Krasnodar')
    weathers = observation.weather
    # {'temp': 2.32, 'temp_max': 2.32, 'temp_min': 2.32,
    # 'feels_like': 2.32, 'temp_kf': None}
    temp = weathers.temperature("celsius")
    answer = f'<b>Прогноз погоды на сегодня</b>:\nмаксимальная температура ' \
             f'<b>{round(temp["temp_max"])}°C</b>\nминимальная температура ' \
             f'<b>{round(temp["temp_min"])}°C</b>\n' \
             f'температура сейчас <b>{round(temp["temp"])}°C</b>\n' \
             f'ощущается как <b>{round(temp["feels_like"])}°C</b>'
    await bot.send_message(chat_id=sibintek, text=answer, parse_mode='html')
    await bot.send_message(chat_id=my, text=answer)


async def greeting():
    """
    Приветствие бота с утра
    """
    await bot.send_message(chat_id=sibintek, text='Доброе утро, 36.6')


async def check_out_boys():
    """
    Раз в неделю запуск скрипта с количеством слов
    """
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        answer = await database.execute('SELECT nick, MAX(count) FROM boys')
        answer_database = await answer.fetchall()
        await bot.send_message(chat_id=-1001214772818,
                               text=f'Больше всего сообщений с '
                                    f'нецензурными словами за последние '
                                    f'семь дней '
                                    f'поступило от {answer_database[0][0]} в '
                                    f'количестве {answer_database[0][1]}')
        answer = await database.execute(
            'Select nick, count from boys ORDER BY 2 DESC')
        answer_database = await answer.fetchall()
        text = '\n'.join([f'{i[0]} : {i[1]}' for i in answer_database])
        await bot.send_message(chat_id=sibintek,
                               text=f'Общая статистика: \n{text}')
        await database.execute('UPDATE boys SET count = 0')
        await database.commit()


# подумать над тем, чтобы данные где-то хранить, а не каждый раз
# запрашивать их из бд
async def holiday_send():
    """действия при вызове комканды holiday- покажет список
     праздников сегодня"""
    day_now = datetime.today().day
    month_now = datetime.today().month
    for_select = str(month_now).rjust(2, '0') + '-' + str(day_now)
    async with aiosqlite.connect('bot_nesibintelk.db') as database:
        answer = await database.execute(
            'Select celebrate from holiday where date = ?', (for_select,))
        answer = await answer.fetchall()
        answer_database = '\n'.join([i[0] for i in answer])
        answer_for_user = f'<b>Какой сегодня праздник</b>\n{answer_database}'
        await bot.send_message(chat_id=my, text=answer_for_user,
                               parse_mode='html')
        answer_for_user = f'<b>Какой сегодня праздник</b>\n{answer_database}'
        await bot.send_message(chat_id=sibintek, text=answer_for_user,
                               parse_mode='html')


async def scheduler():
    """
    Запуск скриптов по времени
    """
    aioschedule.every().day.at('09:00').do(greeting)
    aioschedule.every().day.at("18:42").do(birthday)
    aioschedule.every().day.at('11:55').do(send_course)
    aioschedule.every().day.at('12:00').do(check_apartment)
    aioschedule.every().friday.at('17:00').do(check_out_boys)
    aioschedule.every().hours.do(all_course_class.get_all_course)
    aioschedule.every().day.at('09:10').do(holiday_send)
    aioschedule.every().day.at('09:02').do(send_weather)

    while True:
        await aioschedule.run_pending()
        await asyncio.sleep(1)


async def on_startup(_):
    """
    Запуск обработчика времени и логов
    """
    asyncio.create_task(scheduler())
    asyncio.create_task(loggingg())


# запуск бота и времени
if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=False, on_startup=on_startup)
