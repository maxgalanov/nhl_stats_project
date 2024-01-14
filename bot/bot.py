import telebot
import pandas as pd
import numpy as np
import psycopg2

from telebot import asyncio_filters
from telebot.async_telebot import AsyncTeleBot

# list of storages, you can use any storage
from telebot.asyncio_storage import StateMemoryStorage

# new feature for states.
from telebot.asyncio_handler_backends import State, StatesGroup

import asyncio


token='6960440682:AAFatXmmDB7JgspcC4b1n-W78H5VkgiOQ3Y'
#bot=telebot.TeleBot(token)


bot = AsyncTeleBot(token, state_storage=StateMemoryStorage())


class MyStates(StatesGroup):
    # Just name variables differently
    goalie = State() # creating instances of State class is enough from now
    skater = State()
    fav_goalie = State()
    fav_skater = State()
    


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
Я могу показать результаты матча за неделю с помощью команды /stats
Информация по вратарям: /goalies
Информация по основным игрокам: /skaters""")
    
    
@bot.message_handler(commands=['stats'])
async def get_stats(message):
    stats = pd.read_csv('data/stats.csv')
    print(stats)
    ans = "Вот матчи, которые прошли на этой неделе: \n"
    prev_date = stats['date_play'][0]
    for ind in stats.index:
        cur_date = stats['date_play'][ind]
        cur = ''
        if cur_date != prev_date:
            cur += '\n'
        cur += stats['date_play'][ind] + ' '
        cur += stats['away_teams'][ind] 
        cur += ' ' + str(stats['away_result'][ind])
        cur += ':'
        cur += str(stats['home_result'][ind]) + ' '
        cur += stats['home_teams'][ind] 
        cur += '.\n'
        ans += cur
        prev_date = cur_date
        
    await bot.send_message(message.chat.id, ans)
    
    
# @bot.message_handler(commands=['fav_goalie'])
# async def get_goalies(message):

#     await bot.set_state(message.from_user.id, MyStates.fav_goalie, message.chat.id)    
#     await bot.send_message(message.chat.id, 'Введите имя Вашего любимого вратаря')
    
    
# @bot.message_handler(state=MyStates.fav_goalie)
# async def get_goalies(message):
    
#     await bot.set_state(message.from_user.id, MyStates.fav_goalie, message.chat.id)    
#     await bot.send_message(message.chat.id, 'Спасибо, принято!')
#     async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
#         data['fav_goalie'] = message.text
    

# @bot.message_handler(commands=['fav_goalie_stats'])
# async def get_fav_goalie_stats(message):
    
#     with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
        
#         bot.send_message(message.chat.id, 'Вы ввели ' + data['fav_goalie'] + '.\n')

#         df = gen_df('goalies_agg')

#         res = df[df['playersFIO'] == data['fav_goalie']].iloc[1].to_dict()

#         ans = 'Дата рождения: ' + res['birthDate'] + '\n'
#         ans += 'Страна: ' + res['birthCountry'] + '\n'
#         ans += 'Город: ' + res['birthCity'] + '\n'
#         ans += 'Игр: ' + str(res['gamesCNT']) + '\n'
#         ans += 'Голов: ' + str(res['goals']) + '\n'

#         bot.send_message(message.chat.id, ans)



@bot.message_handler(commands=['goalies'])
async def get_goalies(message):

    await bot.set_state(message.from_user.id, MyStates.goalie, message.chat.id)    
    await bot.send_message(message.chat.id, 'Введите имя вратаря')

@bot.message_handler(state=MyStates.goalie)
async def name_get(message):
    
    df = gen_df('goalies_agg')
    res = df[df['playersFIO'] == message.text].iloc[1].to_dict()
    ans = 'Дата рождения: ' + res['birthDate'] + '\n'
    ans += 'Страна: ' + res['birthCountry'] + '\n'
    ans += 'Город: ' + res['birthCity'] + '\n'
    ans += 'Игр: ' + str(res['gamesCNT']) + '\n'
    ans += 'Голов: ' + str(res['goals']) + '\n'
    
    await bot.send_message(message.chat.id, ans)
    await bot.delete_state(message.from_user.id, message.chat.id)


@bot.message_handler(commands=['skaters'])
async def get_skater(message):

    await bot.set_state(message.from_user.id, MyStates.skater, message.chat.id)    
    await bot.send_message(message.chat.id, 'Введите имя игрока')


@bot.message_handler(state=MyStates.skater)
async def name_get(message):
    
    df = gen_df('skaters_agg')
    res = df[df['playersFIO'] == message.text].iloc[1].to_dict()
    ans = 'Дата рождения: ' + res['birthDate'] + '\n'
    ans += 'Страна: ' + res['birthCountry'] + '\n'
    ans += 'Город: ' + res['birthCity'] + '\n'
    ans += 'Игр: ' + str(res['gamesCNT']) + '\n'
    ans += 'Голов: ' + str(res['goals']) + '\n'
    
    await bot.send_message(message.chat.id, ans)
    await bot.delete_state(message.from_user.id, message.chat.id)
    

@bot.message_handler(state="*", commands=['cancel'])
async def any_state(message):
    await bot.send_message(message.chat.id, "Your state was cancelled.")
    await bot.delete_state(message.from_user.id, message.chat.id)
    
bot.add_custom_filter(asyncio_filters.StateFilter(bot))

bot.set_my_commands([
    telebot.types.BotCommand("/start", "main menu"),
    telebot.types.BotCommand("/stats", "get stats"),
    telebot.types.BotCommand("/goalies", "get goalies"),
    telebot.types.BotCommand("/skaters", "get skaters"),
    telebot.types.BotCommand("/cancel", "cancel"),
    # telebot.types.BotCommand("/fav_goalie", "pick fav goalie"),
    # telebot.types.BotCommand("/fav_goalie_stats", "stats fav goalie")
])


asyncio.run(bot.polling())

# Расписание
# Любимая команда
# К ней последние игры 
# К ней расписание на ближайшую неделю
# Выбрать любимого игрока
# Показать стату игрока в даталенс(?)
# Параметры для игрока 
# Ссылку на дашборды
# Свежую статистику по новым матчам
