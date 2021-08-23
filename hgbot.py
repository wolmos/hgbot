import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
import config
import db_access
from loguru import logger
from sqlalchemy import create_engine

logger.add("debug.log", format="{time} {level} {message}", level="INFO", rotation="3 MB", compression="zip")

# States from certain range. States are kept in memory, lost if bot if restarted!
USER_STATES = defaultdict(int)
SELECT_GROUP, DATE, MARK_VISITORS, GUESTS, TESTIMONIES, PREACHER = range(6)

# For each user, the current group he is working on (one user can edit different groups)
USER_CURRENT_GROUPS = defaultdict(int)

MEMBERS = defaultdict(dict)
VISITORS = defaultdict(dict)
GUEST_VISITORS = defaultdict(list)
ACTIVE_REASONS = defaultdict(None)
DATES = defaultdict(None)


ENGINE = create_engine(f'postgresql://{config.db_user}:{config.db_password}@{config.db_hostname}:{config.db_port}/{config.db_name}?sslmode=require')

import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove

bot = telebot.TeleBot(config.bot_token)

import sentry_sdk
sentry_sdk.init(config.sentry_url)
from sentry_sdk import capture_exception


# {username: {'group_id': , 'leader': , 'username': 'uid': (after first reaction from tg)}}
USERS = db_access.select_leader_usernames(ENGINE)
USER_ID_MAP = {} # user_id: username


#================HELPER METHODS================

def update_user_id(username, user_id):
    USERS[username]['user_id'] = user_id
    USER_ID_MAP[user_id] = username


def update_user_current_group(username, group_id):
    USER_CURRENT_GROUPS[username] = group_id


def check_user_group(message):
    source_username = message.from_user.username
    for username, user_info in USERS.items():
        if source_username == username:
            return user_info
    return False


def get_leader_members(username):
    group_id = USER_CURRENT_GROUPS[username]
    if group_id in MEMBERS:
        return MEMBERS[group_id]
    members = db_access.select_group_members(group_id, ENGINE)
    MEMBERS[group_id] = members
    return members


def get_members(group_id):
    if group_id in MEMBERS:
        return MEMBERS[group_id]
    members = db_access.select_group_members(group_id, ENGINE)
    MEMBERS[group_id] = members
    return members


def get_current_group_id(user_id):
    username = USER_ID_MAP[user_id]
    return USER_CURRENT_GROUPS[username]


def parse_date(text):
    if text in DATES:
        return DATES[text]()
    else:
        try:
            visit_date = datetime.strptime(text, "%d/%m")
            visit_date = visit_date.replace(year=2021)
            return visit_date
        except Exception as e:
            return None
            logger.error(e);


def get_user_mode(user_id):
    return USER_STATES[user_id]


def set_user_mode(user_id, mode):
    USER_STATES[user_id] = mode


#================MENUS================

def get_visit_markup(members):
    markup = InlineKeyboardMarkup()
    for member in members:
        markup.row(InlineKeyboardButton(member,  callback_data='TITLE'))
        markup.row(InlineKeyboardButton("✅", callback_data="{}: +".format(member)),
                   InlineKeyboardButton("🚫", callback_data="{}: -".format(member)))
    markup.row(InlineKeyboardButton('Подтвердить отметки', callback_data='REVIEW'))
#     markup.row(InlineKeyboardButton('Добавить гостя', callback_data='ADD_GUEST'))
    return markup


def get_guests_markup(guests):
    markup = InlineKeyboardMarkup()
    for guest in guests:
        markup.row(InlineKeyboardButton(guest,  callback_data='TITLE'),
                   InlineKeyboardButton("✅", callback_data=f"{guest}"))
    markup.row(InlineKeyboardButton('Завершить добавление гостей', callback_data='FINISH_GUESTS'))
    return markup


def get_review_markup():
    markup = InlineKeyboardMarkup()
    markup.row(InlineKeyboardButton('Всё верно', callback_data='COMPLETE_VISITORS'))
    return markup
# def get_guests_markup(members):
#     markup = InlineKeyboardMarkup()
#     for member in members:
#         markup.row(InlineKeyboardButton(member,  callback_data='TITLE'))
#         markup.row(InlineKeyboardButton("✅", callback_data=f"{member}: +"),
#                    InlineKeyboardButton("🚫", callback_data=f"{member}: -"),
#                    InlineKeyboardButton("Удалить", callback_data=f"{member}: remove"))
#     return markup

def get_dates_markup():
    dates_menu = ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    dates_menu.row('Позавчера')
    dates_menu.row('Вчера')
    dates_menu.row('Сегодня')
    return dates_menu


def get_groups_markup(group_ids):
    menu = ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    for group_id in group_ids:
        menu.add(KeyboardButton(f'Группа: {group_id}'))
    return menu


def get_reasons_markup():
    reasons_menu = ReplyKeyboardMarkup(one_time_keyboard=True)
    [reasons_menu.row(r) for r in REASONS]
    return reasons_menu


DATES = {'Сегодня': (lambda : datetime.now().date()),
         'Вчера': (lambda : datetime.now().date() - timedelta(days=1)),
         'Позавчера': (lambda : datetime.now().date() - timedelta(days=2))}


REASONS = ['Работа / Учеба',
            'Семейные обстоятельства',
            'Болезнь',
            'Встреча по служению в церкви / Был на другой ДГ',
            'Отпуск / Был в другом городе',
            'Не захотел прийти/Забыл',
            'Удалить человека']


def get_visitors_df(user_id):
    df = pd.DataFrame([{
                        'name_leader': values['leader'],
                        'id_hg': get_current_group_id(user_id),
                        'name': name,
                        'status': values['status'],
                        'type_person': 'Член',
                        'reason': values.get('reason', None) }
                       for name, values in VISITORS[user_id].items()])
    df['date'] = DATES[user_id]
    df['date_processed'] = datetime.now()
    return df


def get_guests_df(user_id):
    df = pd.DataFrame([{
                        'name_leader': guest['leader'],
                        'id_hg': get_current_group_id(user_id),
                        'name': guest['name'],
                        'status': guest['status'],
                        'type_person': 'Гость' }
                       for guest in GUEST_VISITORS[user_id]])
    df['date'] = DATES[user_id]
    df['date_processed'] = datetime.now()
    return df


def add_guest_vist(user_id, leader, guest):
    GUEST_VISITORS[user_id].append({'status': '+', 'leader': leader, 'guest': True, 'name': guest})


def group_members_checked(user_id):
    group_members = get_leader_members(USER_ID_MAP[user_id])
    return len(VISITORS[user_id]) == len(group_members)


def get_missing_group_members(user_id):
    group_members = get_leader_members(USER_ID_MAP[user_id])
    return [m for m in group_members if m not in VISITORS[user_id]]


def cleanup(user_id):
    VISITORS[user_id] = {}
    GUEST_VISITORS[user_id] = []
    ACTIVE_REASONS[user_id] = None
    DATES[user_id] = None
    USER_CURRENT_GROUPS[user_id] = None
    set_user_mode(user_id, DATE)


#================WORKING WITH BOT================

def respond_review(bot, leader, user_id, call_id):
    if group_members_checked(user_id):
        df = get_visitors_df(user_id)
        review_text = '\n'.join([f'{row["name"]}: {"✅" if row["status"] == "+" else "🚫"}'
                                 for i, row in df.iterrows()])
        bot.send_message(user_id,
                         f'Все члены отмечены, но ещё есть возможность изменить ответы:\n\n{review_text}',
                         reply_markup=get_review_markup())
    # bot.send_document(user_id, df)
    else:
        missing = get_missing_group_members(user_id)
        bot.answer_callback_query(call_id, 'Ещё не все члены отмечены:\n' + "\n".join(missing))


def respond_complete(bot, group_id, user_id, call_id):
    logger.info('Getting the DF')
    df = get_visitors_df(user_id)
    logger.info('Saving the DF')
    db_access.save_visitors_to_db(df, ENGINE)
    #     cleanup(user_id)
    logger.info('SAVED!')
    bot.answer_callback_query(call_id, 'Все члены отмечены!')
    set_user_mode(user_id, GUESTS)
    guests = db_access.get_group_guests(group_id, ENGINE)
    guests_markup = get_guests_markup(guests)
    bot.send_message(user_id,
                     'Переходим к добавлению гостей. Отправь в отдельных сообщениях имена новых гостей или выбери повторно посетивших из списка.',
                     reply_markup=guests_markup)


def respond_visitor_selection(bot, leader, user_id, call_id, call_data):
    name = call_data.split(':')[0]
    logger.info(f'Got them {name}')
    if ': -' in call_data:
        VISITORS[user_id][name] = {'status': '-', 'leader': leader}
        bot.answer_callback_query(call_id, 'Укажи причину отсутсвия')
        reasons_menu = get_reasons_markup()
        ACTIVE_REASONS[user_id] = name
        bot.send_message(user_id, f'Укажи причину отсутсвия {name}',
                         reply_markup=reasons_menu)
    else:
        bot.answer_callback_query(call_id, call_data)
        VISITORS[user_id][name] = {'status': '+', 'leader': leader}


@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    try:
        user_id = call.from_user.id
        logger.info(f'[User {user_id}] Button Click: {call.data}, user mode {get_user_mode(user_id)}')
        user_info = check_user_group(call)
        group_id = get_current_group_id(user_id)

        hg_info = list(filter(lambda cur_hg_info: cur_hg_info['group_id'] == group_id, user_info['hgs']))[0]
        leader = hg_info['leader']

        if get_user_mode(user_id) == GUESTS:
            if call.data == 'FINISH_GUESTS':
                guests_df = get_guests_df(user_id)
                db_access.save_visitors_to_db(guests_df, ENGINE)
                guests_text = '\n'.join([row['name'] for i, row in guests_df.iterrows()])
                bot.answer_callback_query(call.id, 'Гости добавлены')
                bot.send_message(user_id, f'Гости добавлены:\n\n{guests_text}',
                                 reply_markup=ReplyKeyboardRemove())
                cleanup(user_id)
            elif call.data != "TITLE":
                logger.info(f'Guest added: {call.data}')
                bot.answer_callback_query(call.id, call.data)
                add_guest_vist(user_id, leader, call.data)
        else:
            if call.data == 'REVIEW':
                # bot.edit_message(user_id, reply_markup=ReplyKeyboardRemove())
                respond_review(bot, leader, user_id, call.id)
            elif call.data == 'COMPLETE_VISITORS':
                respond_complete(bot, group_id, user_id, call.id)
            elif call.data != "TITLE":
                respond_visitor_selection(bot, leader, user_id, call.id, call.data)
    except Exception as e:
        capture_exception(e)
        logger.error(e);


# Starting point of bot
@bot.message_handler(func=check_user_group, commands=['add'])
def select_group(message):
    try:
        logger.info('Select Group')
        user_id = message.from_user.id
        cleanup(user_id)
        user_info = check_user_group(message)
        username = user_info['username']
        logger.info(user_info)
        group_ids = map(lambda x: x['group_id'], user_info['hgs'])
        groups_menu = get_groups_markup(group_ids)
        set_user_mode(message.from_user.id, SELECT_GROUP)
        update_user_id(username, user_id)
        bot.send_message(message.from_user.id, 'Привет! Выбери, пожалуйста, группу.', reply_markup=groups_menu)
    except Exception as e:
        capture_exception(e)
        logger.error(e);


@bot.message_handler(func=check_user_group, regexp='^Группа: ')
def select_date(message):
    try:
        logger.info('Select Date')
        group_id = message.text.replace('Группа: ', '')
        print(f'Requested to work with group id {group_id}')
        user_id = message.from_user.id
        user_info = check_user_group(message)
        username = user_info['username']
        update_user_current_group(username, group_id)
        update_user_id(username, user_id)
        hg_info = list(filter(lambda cur_hg_info: cur_hg_info['group_id'] == group_id, user_info['hgs']))[0]
        #bot.reply_to(message, f'Привет! Ты — {hg_info["leader"]}, лидер группы {hg_info["group_id"]}.')
        dates_menu = get_dates_markup()
        set_user_mode(message.from_user.id, DATE)
        bot.send_message(message.from_user.id,
                     f'Привет! Ты — {hg_info["leader"]}, лидер группы {hg_info["group_id"]}. '\
                     'Выбери дату из списка или отправь дату в формате ДД/ММ (27/12)', reply_markup=dates_menu)
    except Exception as e:
        capture_exception(e)
        logger.error(e);


@bot.message_handler(func=check_user_group)
def mark_visits(message):
    try:
        logger.info('Mark Visitors')
        user_id = message.from_user.id
        user_info = check_user_group(message)
        username = user_info['username']
        group_id = get_current_group_id(user_id)
        hg_info = list(filter(lambda cur_hg_info: cur_hg_info['group_id'] == group_id, user_info['hgs']))[0]
        leader = hg_info['leader']

        update_user_id(username, user_id)
        visit_date = parse_date(message.text)

        if get_user_mode(user_id) == DATE:
            if visit_date:
                group_members = get_members(group_id)
                bot.send_message(user_id, f'Выбранная дата: {visit_date}', reply_markup=ReplyKeyboardRemove())
                visit_menu = get_visit_markup(group_members)
                bot.send_message(user_id, f'Отметь посещение за {visit_date}', reply_markup=visit_menu)
                set_user_mode(user_id, MARK_VISITORS)
                DATES[user_id] = visit_date
            else:
                select_date(message)

        elif get_user_mode(user_id) == MARK_VISITORS:
            VISITORS[user_id][ACTIVE_REASONS[user_id]]['reason'] = message.text
        elif get_user_mode(user_id) == GUESTS:
            bot.send_message(user_id, f'Добавлен гость {message.text}')
            add_guest_vist(user_id, leader, message.text)
    except Exception as e:
        capture_exception(e)
        logger.error(e);


bot.polling()
