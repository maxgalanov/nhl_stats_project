import datetime
from dateutil.relativedelta import relativedelta
import asyncio

import telebot
import pandas as pd
import numpy as np
import psycopg2

from telebot.types import ReplyKeyboardMarkup, KeyboardButton
from telebot import asyncio_filters
from telebot.async_telebot import AsyncTeleBot
from telebot.asyncio_storage import StateMemoryStorage
from telebot.asyncio_handler_backends import State, StatesGroup


token='5793916142:AAEULZu3GI9DiznLRL9DoH0djJy8zDFqWKI'
bot = AsyncTeleBot(token, state_storage=StateMemoryStorage())


class PlayerStates(StatesGroup):
    player = State()
    name = State()
    datalens = State()
    

def gen_df(table):
    conn = psycopg2.connect("""
    host=rc1b-diwt576i60sxiqt8.mdb.yandexcloud.net
    port=6432
    sslmode=verify-full
    dbname=hse_db
    user=zendeer
    password=hse_12345
    target_session_attrs=read-write
""")
    df = pd.read_sql(f'SELECT * FROM {table}', conn)
    conn.close()
    return df


@bot.message_handler(commands=['start'])
async def start_message(message):
    await bot.send_message(message.chat.id,"""Привет!
Я могу показать результаты игр за неделю с помощью команды /results
Информация по игрокам: /player_stats
Дэшборды в DataLens: /datalens""")
    
    
@bot.message_handler(commands=['results'])
async def get_results(message):
    results = gen_df('shedule')
    ans = "Вот матчи, которые прошли на этой неделе: \n"
    prev_date = results['date_play'][0]
    
    for ind in results.index:
        cur_date = results['date_play'][ind]
        cur = ''
        cur += '\n' if cur_date != prev_date else ''
        cur += results['date_play'][ind] + ' '
        cur += results['away_teams'][ind] 
        cur += ' ' + str(results['away_result'][ind])
        cur += ':'
        cur += str(results['home_result'][ind]) + ' '
        cur += results['home_teams'][ind] 
        cur += '.\n'
        ans += cur
        prev_date = cur_date
        
    await bot.send_message(message.chat.id, ans)
    

@bot.message_handler(commands=['player_stats'])
async def get_players(message):
    keys = ["Вратарь", "Полевой игрок"]
    markup = ReplyKeyboardMarkup(resize_keyboard = True)
    row = [KeyboardButton(x) for x in keys]
    markup.add(*row)
    
    await bot.set_state(message.from_user.id, PlayerStates.player, message.chat.id)    
    await bot.send_message(message.chat.id, 'Вратарь или полевой игрок?', reply_markup=markup)
    

        
@bot.message_handler(state=PlayerStates.player)
async def get_name(message):
    player_type = {}
    player_type['Вратарь'] = 'goalies_agg'
    player_type['Полевой игрок'] = 'skaters_agg'
    
    markup = telebot.types.ReplyKeyboardRemove()

    await bot.set_state(message.from_user.id, PlayerStates.name, message.chat.id)    
    await bot.send_message(message.chat.id, 'Введите имя', reply_markup=markup)
    
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        data['player'] = player_type[message.text]
    

@bot.message_handler(state=PlayerStates.name)
async def get_stats(message):
    
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        data['name'] = message.text

    markup = telebot.types.ReplyKeyboardRemove()
    
    grp_cols = ['playerId', 'playersFIO', 'triCodeCurrent', 'currentTeamFullName', 'headshot',
       'teamAbbrev', 'teamFullName', 'birthDate', 'birthCity', 'shootsCatches',
       'birthCountry', 'birthStateProvince', 'positionCode']
    
    res = None
    
    async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        
        df = gen_df(data['player'])
        df = df.drop(columns='homeRoadFlag').groupby(grp_cols, as_index=False).sum()
        
        if data['player'] == 'goalies_agg':
            df['perc_goals'] = round(df['shotsAgainst']/(df['shotsAgainst'] + df['goalsAgainst']) * 100, 2)
        
        res = df[df['playersFIO'] == data['name']].iloc[0].to_dict()
        start_date = datetime.datetime(*[int(i) for i in res['birthDate'].split('-')])
        end_date =  datetime.date.today()
        res['difference_in_years'] = str(relativedelta(end_date, start_date).years)
        
    img_url = res['headshot']
    
    await bot.send_photo(message.chat.id, img_url, reply_markup=markup)
    
    ans = 'Дата рождения: ' + res['birthDate'] + '\n'
    ans += 'Возраст: ' + res['difference_in_years'] + '\n'
    ans += 'Страна: ' + res['birthCountry'] + '\n'
    ans += 'Город: ' + res['birthCity'] + '\n'
    ans += 'Команда: ' + res['teamFullName'] + ' (' + res['teamAbbrev'] + ')' + '\n'
    ans += 'Игр: ' + str(int(res['gamesCNT'])) + '\n'
    ans += 'Игр в стартовом составе: ' + str(int(res['gamesStarted'])) + '\n' if data['player'] == 'goalies_agg' else ''
    ans += 'Игр на ноль: ' + str(int(res['shutouts'])) + '\n' if data['player'] == 'goalies_agg' else ''
    ans += 'Процент отраженных бросков: ' + str(res['perc_goals']) + '\n' if data['player'] == 'goalies_agg' else ''
    ans += 'Среднее время на льду: ' + str(int(res['toi'] / res['gamesCNT']) // 60) + ' минут, ' + str(int(res['toi']) % 60) + ' секунд' + '\n'
    
    ## TODO
    ## add for all types
    ans += 'Очков: ' + str(int(res['points'])) + '\n' if data['player'] == 'skaters_agg' else ''
    ans += 'Голов: ' + str(int(res['goals'])) + '\n'
    ans += 'Показатель полезности: ' + str(int(res['plusMinus'])) + '\n' if data['player'] == 'skaters_agg' else ''
    ans += 'Штрафных минут за сезон: ' + str(int(res['pim'])) + '\n'
    
    await bot.send_message(message.chat.id, ans, reply_markup=markup)
    await bot.send_message(message.chat.id, f'''Более подробную информация можете посмотреть в нашем [дашборде по игрокам](https://datalens.yandex/xqnhz02g6x6ml?tab=lD&playersfio_d7s6={data['name'].split(' ')[0]}%20{data['name'].split(' ')[1]})''', parse_mode='MarkdownV2')
    await bot.delete_state(message.from_user.id, message.chat.id)
        

@bot.message_handler(state="*", commands=['cancel'])
async def cancel(message):
    await bot.send_message(message.chat.id, "Your state was cancelled.")
    await bot.delete_state(message.from_user.id, message.chat.id)
    
    
@bot.message_handler(commands=['datalens'])
async def get_datalens(message):
    keys = ["Игроки на карте", "Форварды и защитники", "Вратари", "Статистика игрока"]
    markup = ReplyKeyboardMarkup(resize_keyboard = True)
    row = [KeyboardButton(x) for x in keys]
    markup.add(*row)
    
    await bot.set_state(message.from_user.id, PlayerStates.datalens, message.chat.id)    
    await bot.send_message(message.chat.id, 'Выберите вкладку дэшборда', reply_markup=markup)
    

@bot.message_handler(state=PlayerStates.datalens)
async def get_datalens(message):
    datalens_type = {}
    datalens_type['Игроки на карте'] = '7pV'
    datalens_type['Форварды и защитники'] = 'LD'
    datalens_type['Вратари'] = 'Re'
    datalens_type['Статистика игрока'] = 'lD'
    
    res = 'https://datalens.yandex/xqnhz02g6x6ml?tab='
    res += datalens_type[message.text]
    markup = telebot.types.ReplyKeyboardRemove()
    
    await bot.set_state(message.from_user.id, PlayerStates.datalens, message.chat.id)
    await bot.send_message(message.chat.id, f'[Дашборд в DataLens]({res})', parse_mode='MarkdownV2', reply_markup=markup)
    
    
bot.add_custom_filter(asyncio_filters.StateFilter(bot))

bot.set_my_commands([
    telebot.types.BotCommand("/start", "Главное меню"),
    telebot.types.BotCommand("/results", "Результаты игр за неделю"),
    telebot.types.BotCommand("/player_stats", "Статистика по игроку"),
    telebot.types.BotCommand("/datalens", "Дэшборд"),
    telebot.types.BotCommand("/cancel", "Отмена")
])

asyncio.run(bot.polling())
