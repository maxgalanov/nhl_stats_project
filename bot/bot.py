import telebot
import pandas as pd
import numpy as np

token='6960440682:AAFatXmmDB7JgspcC4b1n-W78H5VkgiOQ3Y'
bot=telebot.TeleBot(token)

@bot.message_handler(commands=['start'])
def start_message(message):
    bot.send_message(message.chat.id,"""Привет!
Я могу показать результаты матча за неделю с помощью команды /stats""")
    
    
@bot.message_handler(commands=['stats'])
def start_message(message):
    stats = pd.read_csv('data/stats.csv')
    print(stats)
    ans = "Вот матчи, которые прошли на этой неделе: \n"
    for ind in stats.index:
        cur = stats['date_play'][ind] + ' '
        cur += stats['away_teams'][ind] 
        cur += ' ' + str(stats['away_result'][ind])
        cur += ':'
        cur += str(stats['home_result'][ind]) + ' '
        cur += stats['home_teams'][ind] 
        cur += '.\n'
        ans += cur
        
    bot.send_message(message.chat.id, ans)
    
    
bot.infinity_polling()



# Расписание
# Любимая команда
# К ней последние игры 
# К ней расписание на ближайшую неделю
# Выбрать любимого игрока
# Показать стату игрока в даталенс(?)
# Параметры для игрока 
# Ссылку на дашборды
# Свежую статистику по новым матчам
