import pandas as pd
import config
import db_access
from datetime import date, timedelta
from loguru import logger
from sqlalchemy import create_engine
import babel.dates

logger.add("send_reminders.log", format="{time} {level} {message}", level="DEBUG", rotation="3 MB", compression="zip")

ENGINE = create_engine(f'postgresql://{config.db_user}:{config.db_password}@{config.db_hostname}:{config.db_port}/{config.db_name}?sslmode=require')

import telebot
bot = telebot.TeleBot(config.bot_token)


def get_users_for_reminder(days_until_reminder):
    df = db_access.get_last_visits(ENGINE)
    to_remind = df[df['max_date'] < date.today() - timedelta(days=days_until_reminder)]
    return to_remind


def process_reminders(to_remind_df, allowed_usernames):
    logger.info('Started processing reminders')
    sent_to = []
    for i, row in to_remind_df.iterrows():
        id_hg = row['id_hg']
        leader = row['leader']
        leader_username = row['leader_username']
        max_date = row['max_date']
        logger.info(f'Processing {id_hg} (leader_username = {leader_username}, max date = {max_date})')
        user_data = db_access.get_user_data(leader_username, ENGINE)
        if len(user_data) > 0 and f'@{leader_username}' in allowed_usernames:
            telegram_uid = user_data[0][1]
            send_message(telegram_uid, id_hg, max_date)
            sent_to.append(f'{leader} (@{leader_username})')
    return sent_to


def send_message(telegram_uid, id_hg, max_date):
    logger.info(f'Sending reminder to {telegram_uid}')
    date_text = format_date(max_date) if max_date is not None else ''
    bot.send_message(telegram_uid, f'Привет! Отчет по группе {id_hg} уже давно не заполнялся — в последний раз это было {date_text} \n\n'
                                   f'Пожалуйста, заполни отчеты по прошедшим за это время группам, а в следующий раз не забывай присылать отчет вовремя 😉')


def format_date(date):
    return babel.dates.format_date(date, 'd MMMM yyyy г.', 'ru')
