import pandas as pd
import config
import db_access
from datetime import date, timedelta
from loguru import logger
from sqlalchemy import create_engine
import babel.dates
import random
import telebot

ENGINE = create_engine(
    f'postgresql://{config.db_user}:{config.db_password}@{config.db_hostname}:{config.db_port}/{config.db_name}?sslmode=require')

bot = telebot.TeleBot(config.bot_token)


def get_actual_master_data():
    df = db_access.get_master_data_for_today(ENGINE)
    return df


def get_users_for_reminder():
    df_last_visits = db_access.get_last_visits(ENGINE)
    df_master_data = get_actual_master_data()
    df_all = pd.merge(df_last_visits, df_master_data, on='id_hg')
    to_remind = df_all[df_all['max_date'] < date.today() - timedelta(days=date.today().weekday() + 7)]
    return to_remind


def process_reminders(to_remind_df):
    logger.info('Started processing reminders')
    sent_to = []
    reminder_message_templates = db_access.get_multi_key_value('reminder_template', ENGINE)
    reminder_message_template = random.choice(reminder_message_templates)

    for i, row in to_remind_df.iterrows():
        id_hg = row['id_hg']
        leader = row['leader']
        leader_username = row['leader_username']
        max_date = row['max_date']
        date_text = format_date(max_date) if max_date is not None else ''
        logger.info(f'Processing {id_hg} (leader_username = {leader_username}, max date = {max_date})')

        reminder_message = reminder_message_template.format(id_hg=id_hg, date_text=date_text)
        user_data = db_access.get_user_data(leader_username, ENGINE)

        if len(user_data) > 0:
            telegram_uid = user_data[0][1]
            try:
                send_message(telegram_uid, reminder_message)
                sent_to.append(f'{leader} (@{leader_username})')
            except Exception as e:
                logger.exception(e)
    logger.info('Finished processing reminders')
    return sent_to


def send_reminder_before_hg(id_hg, time_of_hg):
    message_templates = db_access.get_multi_key_value('reminder_before_hg_template', ENGINE)
    message_template = random.choice(message_templates)
    message = message_template.format(time_of_hg=time_of_hg)
    send_reminder_before_after_hg(id_hg, message)


def send_reminder_after_hg(id_hg):
    message_templates = db_access.get_multi_key_value('reminder_after_hg_template', ENGINE)
    message_template = random.choice(message_templates)
    send_reminder_before_after_hg(id_hg, message_template)


def send_reminder_before_after_hg(id_hg, message):
    leader_username = db_access.get_leader_username_for_hg(id_hg, ENGINE)
    user_data = db_access.get_user_data(leader_username, ENGINE)

    if leader_username is not None and len(user_data) > 0:
        telegram_uid = user_data[0][1]
        try:
            send_message(telegram_uid, message)
        except Exception as e:
            logger.exception(e)


def send_message(telegram_uid, reminder_message):
    logger.info(f'Sending reminder to {telegram_uid}: {reminder_message}')
    bot.send_message(telegram_uid, reminder_message)


def send_message_fake(telegram_uid, reminder_message):
    logger.info(f'Sending fake reminder to {telegram_uid}: {reminder_message}')


def format_date(date):
    return babel.dates.format_date(date, 'd MMMM yyyy г.', 'ru')
