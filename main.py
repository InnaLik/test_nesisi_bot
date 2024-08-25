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
import pandas as pd
import aioschedule
from aiogram import Bot, Dispatcher, executor
from aiogram.dispatcher.filters import Command
from pycbrf import ExchangeRates
from aiogram.types import Message
from sqlalchemy import select, func, update, delete, insert
from chat_id import my, sibintek
import db

with open('test_token.txt', encoding='utf-8') as file:
    API_TOKEN: str = file.read()

with open('api_weather.txt', encoding='utf-8') as file:
    API_WEATHER: str = file.read()

# Создаем объекты бота и диспетчера
bot: Bot = Bot(token=API_TOKEN)
dp: Dispatcher = Dispatcher(bot)


async def loggingg():
    """
    Функция для записи логов от уровня INFO и выше.
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
    Этот handler будет срабатывать на команду "/start".

    Args:
        message: Сообщение, поступившее боту. В данном случае /start.
    """
    await message.answer(text='Привет, вызови команду /help, '
                              'чтобы узнать список доступных команд')


@dp.message_handler(Command(commands=["help"]))
async def process_help_commands(message: Message):
    """
    При вызове команды /help.

    Args:
        message: Сообщение, поступившее боту. В данном случае /help.
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
    пользователем сообщение о всех курсах на сегодняшний день."""
    await all_course_class.get_all_course()
    mess = await all_course_class.get()
    await bot.send_message(message.chat.id, mess)


# при вызове команда all_phrases
@dp.message_handler(Command(commands=['all_phrases']))
async def process_all_phrases_command(message: Message):
    """Вернет пользователю в чат все фразы из бд."""
    async with db.async_session() as session:
        query = select(db.Phrases.phrase)
        res = await session.execute(query)
        res = res.all()
        answer = '\n'.join([i[0] for i in res])
    await bot.send_message(message.chat.id, answer)


# при вызове команды add
@dp.message_handler(Command(commands=['add']))
async def process_add_command(message: Message):
    """Добавит фразу в бд."""
    phrase = ' '.join(message.text.split()[1:])
    if len(phrase) > 0:
        async with db.async_session() as session:
            smtp = insert(db.Phrases).values(phrase=phrase)
            await session.execute(smtp)
            await session.commit()
            await bot.send_message(message.chat.id,
                                   f'фраза "{phrase}" добавлена')
    else:
        await bot.send_message(message.chat.id,
                               'фраза не должна быть пустой')


# при вызове команды del
@dp.message_handler(Command(commands=['del']))
async def process_del_command(message: Message):
    """Удалит фразу из бд."""
    phrase = ' '.join(message.text.split()[1:])
    async with db.async_session() as session:
        query = select(db.Phrases).filter_by(phrase=phrase)
        res = await session.execute(query)
        if res.all():
            smtp = delete(db.Phrases).filter_by(phrase=phrase)
            await session.execute(smtp)
            await session.commit()
            await bot.send_message(message.chat.id,
                                   f'фраза "{phrase}" удалена')
        else:
            await bot.send_message(message.chat.id,
                                   'фразы не найдено, повторите попытку')


@dp.message_handler(Command(commands=['add_bad']))
async def process_add_bad_command(message: Message):
    """Добавит слово, на которое бот будет реагировать в бд."""
    word = ' '.join(message.text.lower().split()[1:])
    async with db.async_session() as session:
        # проверка, что слова нет в таблице исключений
        query = select(db.Name.id).filter_by(name=word)
        res = await session.execute(query)
        if not res.all() and len(word) != 0:
            smtp = insert(db.BadWords).values(word=word)
            await session.execute(smtp)
            await session.commit()
            await bot.send_message(message.chat.id,
                                   f'слово <b>{word}</b> добавлено',
                                   parse_mode='html')
        else:
            await bot.send_message(message.chat.id, 'Такие слова не добавляю')


@dp.message_handler(Command(commands=['del_bad']))
async def process_del_bad_command(message: Message):
    """Удалит слово из таблицы bad_words, если оно там есть."""
    word = ' '.join(message.text.lower().split()[1:])
    async with db.async_session() as session:
        query = select(db.BadWords.id).filter_by(word=word)
        res = await session.execute(query)
        if res.all():
            smtp = delete(db.BadWords).filter_by(word=word)
            await session.execute(smtp)
            await session.commit()
            await bot.send_message(message.chat.id, f'слово "{word}" удалено')
        else:
            await bot.send_message(message.chat.id, 'Такого слова нет')


@dp.message_handler(Command(commands=['taboo']))
async def process_taboo_command(message: Message):
    """Дeйствия при вызове команды taboo - добавляет слова в таблицу NAME,
    чтобы эти слова потом нельзя было добавить в таблицу bad_words."""
    word = ' '.join(message.text.lower().split()[1:])
    async with db.async_session() as session:
        smtp = insert(db.Name).values(name=word)
        await session.execute(smtp)
        await session.commit()
    await bot.send_message(message.chat.id,
                           f'слово "{word}" добавлено в список '
                           f'исключений, его нельзя будет добавить в '
                           f'таблицу bad_words')


@dp.message_handler(Command(commands=['taboo_del']))
async def process_taboo_del_command(message: Message):
    """
    Дeйствия при вызове команды taboo_del -
    удаляет слово из таблицы NAME.
    """
    word = ' '.join(message.text.lower().split()[1:])
    async with db.async_session() as session:
        smtp = delete(db.Name).filter_by(name=word)
        await session.execute(smtp)
        await session.commit()
    await bot.send_message(message.chat.id,
                           f'слово "{word}" удалено из исключений и его '
                           f'можно добавлять в таблицу bad_words')


@dp.message_handler(Command(commands=['taboo_all']))
async def process_taboo_all_command(message: Message):
    """
    Действия при вызове комканды taboo_all -
    покажет список всех исключений.
    """
    async with db.async_session() as session:
        query = select(db.Name.name)
        res = await session.execute(query)
        res = res.all()
        answer = '\n'.join([i[0] for i in res])
    await bot.send_message(message.chat.id, answer)


@dp.message_handler(Command(commands=['holiday']))
async def holiday(message: Message):
    """
    Действия при вызове комканды holiday- покажет
    список праздников сегодня.
    """
    day_now = datetime.today().day
    month_now = datetime.today().month
    for_select = str(month_now).rjust(2, '0') + '-' + str(day_now).rjust(2,
                                                                         '0')
    async with db.async_session() as session:
        query = select(db.Holidays.celebrate).filter_by(date=for_select)
        result = await session.execute(query)
        res = result.all()
    answer_database = '\n'.join([i[0] for i in res])
    await bot.send_message(message.chat.id,
                           text=f'<b>Какой сегодня праздник</b>'
                                f'\n{answer_database}',
                           parse_mode='html')


@dp.message_handler(Command(commands=['weather']))
async def weather(message: Message):
    """Покажет погоду на ближайший час."""
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
    """Обработка текстовых сообщений."""
    mess = message.text.lower().split()
    list_word = tuple([i.strip(punctuation) for i in mess])
    # в данной строчке мы берем каждое слово из написанного
    # сообщения и проверяем есть ли слово в таблице bad_words
    # чтобы соответственно понимать отреагировать на сообщение или нет
    async with db.async_session() as session:
        query = select(db.BadWords.id).filter(db.BadWords.word.in_(list_word))
        res = await session.execute(query)
        res = res.all()
        if res:
            query = select(db.Phrases.phrase).order_by(func.random()).limit(1)
            answer_message = await session.execute(query)
            answer_message = answer_message.first()
            await bot.send_message(message.chat.id, answer_message[0])
            sub_smtp = select(db.Boys.count).filter_by(id=message.from_user.id)
            smtp = update(db.Boys).values(count=sub_smtp.c.count + 1).where(
                db.Boys.id == message.from_user.id)
            await session.execute(smtp)
            await session.commit()


@dataclass
class Clipboard:
    """Для получения курса валют."""
    course_dollar: str = ''
    course_ali: str = ''
    course_euro: str = ''

    @staticmethod
    async def get_dollar():
        """Для получения курса доллара."""
        rates = ExchangeRates(datetime.now())
        return str(rates['USD'].value)[0:5]

    @staticmethod
    async def get_ali():
        """Для получения курса али."""
        tables = pd.read_html('https://helpix.ru/currency/')
        for df in tables:
            if 'Aliexpress.ru' in df.columns:
                return df.loc[0, 'Aliexpress.ru']

    @staticmethod
    async def get_euro():
        """Для получения курса евро."""
        rates = ExchangeRates(datetime.now())
        return str(rates['EUR'].value)[0:5]

    async def get_all_course(self):
        """Запись курсов в переменные."""
        self.course_dollar = await self.get_dollar()
        self.course_euro = await self.get_euro()
        self.course_ali = await self.get_ali()

    async def get(self):
        """Получение всех курсов."""
        return f'курс доллара: {self.course_dollar}\n' \
               f'курс евро: {self.course_euro}\n' \
               f'курс али: {self.course_ali}'


all_course_class = Clipboard()


async def send_course():
    """Отправка курсов."""
    await all_course_class.get_all_course()
    mess = await all_course_class.get()
    await bot.send_message(chat_id=my, text=mess)
    await bot.send_message(chat_id=sibintek, text=mess)


async def check_apartment():
    """Напоминание по подаче данных."""
    if datetime.now().day == 19:
        await bot.send_message(chat_id=my, text='Подать данные по коммуналке')


async def birthday():
    """Поздравление с днем рождения."""
    dates = str(datetime.now().day).rjust(2, '0') + '.' + str(
        datetime.now().month).rjust(2, '0')
    async with db.async_session() as session:
        query = select(db.Birthday.name).filter_by(date=dates)
        res = await session.execute(query)
        res = res.all()
        if res:
            await bot.send_message(chat_id=my,
                                   text=f'Сегодня свой день рождение '
                                        f'празднует {res[0][0]}! '
                                        f'Давайте все вместе поздравим его!')


async def send_weather():
    """Запрос и отправка погоды."""
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
    """Приветствие бота с утра."""
    await bot.send_message(chat_id=sibintek, text='Доброе утро, 36.6')


async def check_out_boys():
    """Раз в неделю запуск скрипта с количеством слов."""
    async with db.async_session() as session:
        query = select(db.Boys.nick, func.max(db.Boys.count))
        res = await session.execute(query)
        answer_database = res.all()
        await bot.send_message(chat_id=sibintek,
                               text=f'Больше всего сообщений с '
                                    f'нецензурными словами за последние '
                                    f'семь дней '
                                    f'поступило от {answer_database[0][0]} в '
                                    f'количестве {answer_database[0][1]}')
        query = select(db.Boys.nick, db.Boys.count).order_by(
            db.Boys.count.desc())
        res = await session.execute(query)
        answer_database = res.all()
        text = '\n'.join([f'{i[0]} : {i[1]}' for i in answer_database])
        await bot.send_message(chat_id=sibintek,
                               text=f'Общая статистика: \n{text}')
        smtp = update(db.Boys).values(count=0)
        await session.execute(smtp)
        await session.commit()


async def holiday_send():
    """Выборка из бд по дню с праздниками за сегодня."""
    day_now = datetime.today().day
    month_now = datetime.today().month
    for_select = str(month_now).rjust(2, '0') + '-' + str(day_now).rjust(2,
                                                                         '0')
    async with db.async_session() as session:
        query = select(db.Holidays.celebrate).filter_by(date=for_select)
        result = await session.execute(query)
        res = result.all()
    answer_database = '\n'.join([i[0] for i in res])
    await bot.send_message(chat_id=my, text=answer_database,
                           parse_mode='html')
    answer_for_user = f'<b>Какой сегодня праздник</b>\n{answer_database}'
    await bot.send_message(chat_id=sibintek, text=answer_for_user,
                           parse_mode='html')


async def scheduler():
    """Запуск скриптов по времени в бесконечном цикле времени."""
    aioschedule.every().day.at('09:00').do(greeting)
    aioschedule.every().day.at('09:03').do(birthday)
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
    """Запуск обработчика времени и логов."""
    asyncio.create_task(scheduler())
    asyncio.create_task(loggingg())


# запуск бота и времени
if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=False, on_startup=on_startup)
