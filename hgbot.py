import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
import config
import db_access
import send_reminders
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
import babel.dates
import random

logger.add("debug.log", format="{time} {level} {message}", level="DEBUG", rotation="3 MB", compression="zip")

# States from certain range. States are kept in memory, lost if bot if restarted!
USER_STATES = defaultdict(int)
SELECT_GROUP, DATE, MARK_VISITORS, GROUP_DID_NOT_GATHER_CONFIRM, GUESTS, HG_SUMMARY, HG_SUMMARY_CONFIRM, \
TESTIMONIES, TESTIMONIES_INPUT, TESTIMONIES_CONFIRM, PREACHER, \
DISTRIBUTED_PEOPLE, DISTRIBUTED_PEOPLE_INPUT, DISTRIBUTED_PEOPLE_CONFIRM, \
PERSONAL_MEETING, PERSONAL_MEETING_INPUT, PERSONAL_MEETING_CONFIRM, FINISH_ALL = range(18)

# For each user, the current group he is working on (one user can edit different groups)
USER_CURRENT_GROUPS = defaultdict(int)

MEMBERS = defaultdict(dict)
VISITORS = defaultdict(dict)
GUEST_VISITORS = defaultdict(list)
ACTIVE_REASONS = defaultdict(None)
DATES = defaultdict(None)
SUMMARY = defaultdict(None)
DISTRIBUTED_PEOPLE_FEEDBACK = defaultdict(None)
TESTIMONY = defaultdict(None)
PERSONAL_MEETINGS_FEEDBACK = defaultdict(None)

THANK_YOU_MESSAGES = ['Спасибо тебе!']  # just in case if nothing found in the DB
FEEDBACK_MESSAGE = ''

ENGINE = create_engine(f'postgresql://{config.db_user}:{config.db_password}@{config.db_hostname}:{config.db_port}/{config.db_name}?sslmode=require')

import telebot
from telebot.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove

bot = telebot.TeleBot(config.bot_token)

import sentry_sdk

sentry_sdk.init(config.sentry_url)
from sentry_sdk import capture_exception

# {username: {'group_id': , 'leader': , 'username': 'uid': (after first reaction from tg)}}
USERS = {}
USER_ID_MAP = {}  # user_id: username
GROUP_ICONS = ['🍏', '🍒', '🍉', '🍍', '🥥', '🍑', '🍇', '🫑', '🥝', '🍋']


# ================INITIALIZATION================

def init():
    global USERS, THANK_YOU_MESSAGES, FEEDBACK_MESSAGE

    logger.info('Init started')
    USERS = db_access.select_leader_usernames(ENGINE)
    logger.debug(f"Got {len(USERS)} users from DB")
    THANK_YOU_MESSAGES = db_access.get_multi_key_value('thank_you_message', ENGINE)
    FEEDBACK_MESSAGE = db_access.get_single_key_value('feedback_message', ENGINE)
    logger.info('Init finished')


# ================HELPER METHODS================

def update_user_id(username, user_id):
    USERS[username]['user_id'] = user_id
    USER_ID_MAP[user_id] = username
    db_access.save_user_data(username, user_id, ENGINE)


def update_user_current_group(username, group_id):
    USER_CURRENT_GROUPS[username] = group_id


def check_user_group(message):
    source_username = message.from_user.username
    for username, user_info in USERS.items():
        if source_username == username:
            return user_info
    bot.send_message(message.from_user.id, 'Привет! К сожалению, у тебя нет доступа')
    logger.warning(f'The user @{source_username} does not have access')
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


def get_group_info(user_info, group_id):
    return list(filter(lambda cur_info: cur_info['group_id'] == group_id, user_info['hgs']))[0]


def parse_date(text):
    if text in DATES:
        return DATES[text]()
    else:
        try:
            visit_date = datetime.strptime(text, '%d/%m/%y').date()
            return visit_date
        except Exception as e:
            logger.error(e)
            return None

def format_date(date):
    return babel.dates.format_date(date, 'd MMMM yyyy г.', 'ru')


def get_user_mode(user_id):
    return USER_STATES[user_id]


def set_user_mode(user_id, mode):
    USER_STATES[user_id] = mode


# ================MENUS================

def get_visit_markup(members):
    markup = InlineKeyboardMarkup()
    for member in members:
        markup.row(InlineKeyboardButton(member, callback_data='TITLE'))
        markup.row(InlineKeyboardButton(f"✅", callback_data="{}: +".format(member)),
                   InlineKeyboardButton(f"🚫", callback_data="{}: -".format(member)))
    markup.row(InlineKeyboardButton('Подтвердить отметки', callback_data='REVIEW'))
    markup.row(InlineKeyboardButton('Группа не прошла', callback_data='GROUP_DID_NOT_GATHER'))
    #     markup.row(InlineKeyboardButton('Добавить гостя', callback_data='ADD_GUEST'))
    return markup


def get_guests_markup(guests):
    markup = InlineKeyboardMarkup()
    for guest in guests:
        markup.row(InlineKeyboardButton(guest, callback_data='TITLE'),
                   InlineKeyboardButton("✅", callback_data=f"{guest}"))
    markup.row(InlineKeyboardButton('Завершить добавление гостей', callback_data='FINISH_GUESTS'))
    return markup


def get_review_markup():
    markup = InlineKeyboardMarkup()
    markup.row(InlineKeyboardButton('Все верно', callback_data='COMPLETE_VISITORS'))
    return markup


def get_dates_markup():
    dates_menu = ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)
    dates_menu.row('⏪ Позавчера')
    dates_menu.row('◀️ Вчера')
    dates_menu.row('✔️ Сегодня')
    return dates_menu


def get_groups_markup(group_ids):
    menu = ReplyKeyboardMarkup(one_time_keyboard=True, resize_keyboard=True)

    # assign quasi-random fruit to each group
    for group_id in group_ids:
        icon = GROUP_ICONS[ord(group_id[-1]) % 10]
        menu.add(KeyboardButton(f'{icon} Группа: {group_id}'))
    return menu


def get_reasons_markup():
    reasons_menu = ReplyKeyboardMarkup(one_time_keyboard=True)

    reasons_menu.row(REASONS['work'][1], REASONS['family'][1])
    reasons_menu.row(REASONS['illness'][1], REASONS['church'][1])
    reasons_menu.row(REASONS['vacation'][1], REASONS['forgot'][1])
    reasons_menu.row(REASONS['unknown'][1], REASONS['delete'][1])

    return reasons_menu


def get_confirm_yes_no_markup():
    markup = InlineKeyboardMarkup()
    markup.row(InlineKeyboardButton('Да, все верно', callback_data='YES'),
               InlineKeyboardButton('Нет, хочу исправить', callback_data='NO'))
    return markup


def get_distributed_people_markup():
    markup = InlineKeyboardMarkup()
    markup.row(InlineKeyboardButton('Рассказать', callback_data='YES'),
               InlineKeyboardButton('Нет', callback_data='NO'))
    return markup


DATES = {'✔️ Сегодня': (lambda: datetime.now().date()),
         '◀️ Вчера': (lambda: datetime.now().date() - timedelta(days=1)),
         '⏪ Позавчера': (lambda: datetime.now().date() - timedelta(days=2))}

# key = some code, value = (message for DB, display message)
REASONS = {'work': ('Работа / Учеба', '💼 Работа / Учеба'),
           'family': ('Семейные обстоятельства', '🚼 Семейные обстоятельства'),
           'illness': ('Болезнь', '🩺 Болезнь'),
           'church': ('Служение / Другая ДГ', '🦸 Служение / Другая ДГ'),
           'vacation': ('Отпуск / Отсутствие в городе', '✈️ Отпуск / Отсутствие в городе'),
           'forgot': ('Не захотел прийти / Забыл', '🙈 Не захотел прийти / Забыл'),
           'unknown': ('Не удалось выяснить причину', '🤷 Не удалось выяснить причину'),
           'delete': ('Удалить человека', '🚫 Удалить человека')
           }


def get_visitors_df(user_id):
    df = pd.DataFrame([{
        'name_leader': values['leader'],
        'id_hg': get_current_group_id(user_id),
        'name': name,
        'status': values['status'],
        'type_person': 'Член',
        'reason': values.get('reason', None)}
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
        'type_person': 'Гость'}
        for guest in GUEST_VISITORS[user_id]])
    df['date'] = DATES[user_id]
    df['date_processed'] = datetime.now()
    return df


def get_questions_df(user_id):
    username = USER_ID_MAP[user_id]
    user_info = USERS[username]
    group_id = USER_CURRENT_GROUPS[username]
    group_info = get_group_info(user_info, group_id)

    df = pd.DataFrame([{
        'name_leader': group_info['leader'],
        'id_hg': group_id[:7],
        'date': DATES[user_id],
        'summary': SUMMARY[user_id],
        'distributed_people_feedback': DISTRIBUTED_PEOPLE_FEEDBACK[user_id],
        'testimony': TESTIMONY[user_id],
        'personal_meeting': PERSONAL_MEETINGS_FEEDBACK[user_id]
    }])
    return df


def add_guest_vist(user_id, leader, guest):
    existing_guest_names = set(map(lambda x: x['name'], GUEST_VISITORS[user_id]))
    if guest in existing_guest_names:
        logger.info(f'Guest {guest} has been already added')
    else:
        GUEST_VISITORS[user_id].append({'status': '+', 'leader': leader, 'guest': True, 'name': guest})


def add_summary(user_id, group_info, summary):
    SUMMARY[user_id] = summary


def add_distributed_people(user_id, feedback):
    DISTRIBUTED_PEOPLE_FEEDBACK[user_id] = feedback


def add_testimonies(user_id, testimony):
    TESTIMONY[user_id] = testimony


def add_personal_meetings_feedback(user_id, personal_meetings_feedback):
    PERSONAL_MEETINGS_FEEDBACK[user_id] = personal_meetings_feedback


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
    SUMMARY[user_id] = None
    DISTRIBUTED_PEOPLE_FEEDBACK[user_id] = None
    TESTIMONY[user_id] = None
    PERSONAL_MEETINGS_FEEDBACK[user_id] = None
    set_user_mode(user_id, DATE)


# ================WORKING WITH BOT================

def respond_select_date(bot, user_id, username, group_id):
    update_user_current_group(username, group_id)
    user_info = USERS[username]
    group_info = get_group_info(user_info, group_id)
    # bot.reply_to(message, f'Привет! Ты — {group_info["leader"]}, лидер группы {group_info["group_id"]}.')

    dates_menu = get_dates_markup()
    set_user_mode(user_id, DATE)
    bot.send_message(user_id,
                     f'Привет! Ты — {group_info["leader"]}, лидер группы {group_info["group_id"]}. '
                     'Выбери дату из списка или отправь дату в формате ДД/ММ/ГГ (03/09/21)', reply_markup=dates_menu)


def respond_invalid_date_format(message):
    bot.reply_to(message, 'Я не понимаю, что это за дата 🤷\nПопробуй еще раз. Дата должна быть в формате ДД/ММ/ГГ (03/09/21)')


def respond_date_is_in_future(message):
    bot.reply_to(message, 'Нельзя указать дату в будущем, попробуй ввести еще раз')


def respond_mark_visits(user_id, visit_date, group_members):
    visit_menu = get_visit_markup(group_members)
    bot.send_message(user_id, f'Отметь посещения за {format_date(visit_date)} (при присутствии нажми ✅, при отсутствии нажми 🚫 и выбери причину отсутствия). Если группа не состоялась, нажми «Группа не прошла».', reply_markup=visit_menu)
    set_user_mode(user_id, MARK_VISITORS)


def respond_review(bot, leader, user_id, call_id):
    if group_members_checked(user_id):
        df = get_visitors_df(user_id)
        review_text = '\n'.join([f'{row["name"]}: {"✅" if row["status"] == "+" else "🚫"}'
                                 for i, row in df.iterrows()])
        bot.send_message(user_id,
                         f'Все члены отмечены, но ещё есть возможность изменить ответы:\n\n{review_text}',
                         reply_markup=get_review_markup())
        bot.answer_callback_query(call_id)
    # bot.send_document(user_id, df)
    else:
        missing = get_missing_group_members(user_id)
        bot.answer_callback_query(call_id, 'Ещё не все члены отмечены: ' + ", ".join(missing))


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
                     'Переходим к добавлению гостей. Отправь в отдельных сообщениях имена новых гостей или выбери повторно посетивших из списка. Затем нажми «Завершить добавление гостей»',
                     reply_markup=guests_markup)
    bot.answer_callback_query(call_id)


def respond_visitor_selection(bot, leader, user_id, call_id, call_data):
    name = call_data.split(':')[0]
    logger.info(f'Got them {name}')
    if ': -' in call_data:
        VISITORS[user_id][name] = {'status': '-', 'leader': leader}
        bot.answer_callback_query(call_id, 'Укажи причину отсутствия')
        reasons_menu = get_reasons_markup()
        ACTIVE_REASONS[user_id] = name
        bot.send_message(user_id, f'Укажи причину отсутствия {name}',
                         reply_markup=reasons_menu)
    else:
        bot.answer_callback_query(call_id, call_data)
        VISITORS[user_id][name] = {'status': '+', 'leader': leader}
        if group_members_checked(user_id):
            bot.send_message(user_id, f'Отлично! Теперь нажми «Подтвердить отметки»')
        else:
            bot.send_message(user_id, f'{name}: ✅\nПродолжай отмечать дальше.')


def respond_confirm_did_not_gather(user_id):
    set_user_mode(user_id, GROUP_DID_NOT_GATHER_CONFIRM)
    confirm_markup = get_confirm_yes_no_markup()
    bot.send_message(user_id, 'Группа действительно не прошла на этой неделе?',
                     reply_markup=confirm_markup)


def respond_hg_summary(user_id, call_id):
    set_user_mode(user_id, HG_SUMMARY)
    bot.send_message(user_id, 'Опиши, о чем была духовная часть (3–4 тезиса)', reply_markup=ReplyKeyboardRemove())
    bot.answer_callback_query(call_id)


def respond_confirm_hg_summary(user_id, call_id=None):
    set_user_mode(user_id, HG_SUMMARY_CONFIRM)
    confirm_hg_summary_markup = get_confirm_yes_no_markup()
    bot.send_message(user_id, 'Духовная часть указана правильно?', reply_markup=confirm_hg_summary_markup)
    if call_id is not None:
        bot.answer_callback_query(call_id)


def respond_distributed_people(user_id):
    set_user_mode(user_id, DISTRIBUTED_PEOPLE)
    distributed_people_markup = get_distributed_people_markup()
    bot.send_message(user_id,
                     'Есть ли у тебя на домашней группе люди, которых тебе передали в течение последнего месяца? Если есть, рассажи немного о коммуникации с ними.',
                     reply_markup=distributed_people_markup)


def respond_input_distributed_people(user_id):
    set_user_mode(user_id, DISTRIBUTED_PEOPLE_INPUT)
    bot.send_message(user_id, 'Опиши, пожалуйста, ситуацию вкратце')


def respond_confirm_distributed_people(user_id):
    set_user_mode(user_id, DISTRIBUTED_PEOPLE_CONFIRM)
    confirm_distributed_people_markup = get_confirm_yes_no_markup()
    bot.send_message(user_id, 'Ситуация с распределенными людьми описана правильно?',
                     reply_markup=confirm_distributed_people_markup)


def respond_testimonies(user_id):
    set_user_mode(user_id, TESTIMONIES)
    testimonies_markup = get_distributed_people_markup()
    bot.send_message(user_id, 'Были ли какие-нибудь свидетельства?', reply_markup=testimonies_markup)


def respond_input_testimonies(user_id):
    set_user_mode(user_id, TESTIMONIES_INPUT)
    bot.send_message(user_id, 'Опиши, какие были свидетельства (например, Иванов Иван: Молились за исцеление...)')


def respond_confirm_testimonies(user_id):
    set_user_mode(user_id, TESTIMONIES_CONFIRM)
    confirm_testimonies_markup = get_confirm_yes_no_markup()
    bot.send_message(user_id, 'Свидетельства описаны правильно?',
                     reply_markup=confirm_testimonies_markup)


def respond_personal_meetings_feedback(user_id):
    set_user_mode(user_id, PERSONAL_MEETING)
    personal_meeting_markup = get_distributed_people_markup()
    bot.send_message(user_id, 'Были ли личные встречи?', reply_markup=personal_meeting_markup)

def respond_input_personal_meetings_feedback(user_id):
    set_user_mode(user_id, PERSONAL_MEETING_INPUT)
    bot.send_message(user_id, 'Расскажи, с кем были встречи')


def respond_confirm_personal_meetings_feedback(user_id):
    set_user_mode(user_id, PERSONAL_MEETING_CONFIRM)
    confirm_personal_meetings_markup = get_confirm_yes_no_markup()
    bot.send_message(user_id, 'Про встречи написано правильно?',
                     reply_markup=confirm_personal_meetings_markup)


def get_thank_you_message():
    return random.choice(THANK_YOU_MESSAGES) + '\n\n' + FEEDBACK_MESSAGE


def respond_finish(user_id):
    set_user_mode(user_id, FINISH_ALL)
    bot.send_message(user_id, get_thank_you_message(), reply_markup=ReplyKeyboardRemove())


# Handles all clicks on inline buttons
@bot.callback_query_handler(func=lambda call: True)
def callback_query(call):
    try:
        user_id = call.from_user.id
        user_info = check_user_group(call)
        user_mode = get_user_mode(user_id)
        logger.info(f'[User {user_id} (@{user_info["username"]})] Button Click: {call.data}, user mode {user_mode}')

        group_id = get_current_group_id(user_id)
        group_info = get_group_info(user_info, group_id)
        leader = group_info['leader']

        if user_mode == GUESTS:
            if call.data == 'FINISH_GUESTS':
                guests_df = get_guests_df(user_id)
                db_access.save_visitors_to_db(guests_df, ENGINE)
                guests_text = '\n'.join([row['name'] for i, row in guests_df.iterrows()])
                if guests_text != '':
                    bot.answer_callback_query(call.id)
                    bot.send_message(user_id, f'Гости добавлены:\n\n{guests_text}',
                                    reply_markup=ReplyKeyboardRemove())
                else:
                    bot.answer_callback_query(call.id)
                    bot.send_message(user_id, f'Гостей не было',
                                    reply_markup=ReplyKeyboardRemove())
                respond_hg_summary(user_id, call.id)
                # cleanup(user_id)
            elif call.data != "TITLE":
                logger.info(f'Guest added: {call.data}')
                bot.answer_callback_query(call.id, call.data)
                add_guest_vist(user_id, leader, call.data)
        # elif user_mode == HG_SUMMARY:
        # skip summary button click
        elif user_mode == HG_SUMMARY_CONFIRM:
            if call.data == 'YES':
                logger.info(f'Confirmed hg summary: {SUMMARY[user_id]}')
                bot.answer_callback_query(call.id)
                respond_testimonies(user_id)
            elif call.data == 'NO':
                respond_hg_summary(user_id, call.id)
        elif user_mode == TESTIMONIES:
            if call.data == 'YES':
                bot.answer_callback_query(call.id)
                respond_input_testimonies(user_id)
            elif call.data == 'NO':
                bot.answer_callback_query(call.id)
                respond_personal_meetings_feedback(user_id)
        elif user_mode == TESTIMONIES_CONFIRM:
            if call.data == 'YES':
                bot.answer_callback_query(call.id)
                respond_personal_meetings_feedback(user_id)
            elif call.data == 'NO':
                bot.answer_callback_query(call.id)
                respond_input_testimonies(user_id)
        elif user_mode == PERSONAL_MEETING:
            if call.data == 'YES':
                bot.answer_callback_query(call.id)
                respond_input_personal_meetings_feedback(user_id)
            elif call.data == 'NO':
                bot.answer_callback_query(call.id)
                respond_distributed_people(user_id)
        elif user_mode == PERSONAL_MEETING_CONFIRM:
            if call.data == 'YES':
                bot.answer_callback_query(call.id)
                respond_distributed_people(user_id)
            elif call.data == 'NO':
                bot.answer_callback_query(call.id)
                respond_input_personal_meetings_feedback(user_id)
        elif user_mode == DISTRIBUTED_PEOPLE:
            if call.data == 'YES':
                bot.answer_callback_query(call.id)
                respond_input_distributed_people(user_id)
            elif call.data == 'NO':
                questions_df = get_questions_df(user_id)
                db_access.save_questions_to_db(questions_df, ENGINE)
                logger.info(f'Saved questions df: {questions_df}')
                bot.answer_callback_query(call.id)
                respond_finish(user_id)
        elif user_mode == DISTRIBUTED_PEOPLE_CONFIRM:
            if call.data == 'YES':
                questions_df = get_questions_df(user_id)
                db_access.save_questions_to_db(questions_df, ENGINE)
                logger.info(f'Saved questions df: {questions_df}')
                bot.answer_callback_query(call.id)
                respond_finish(user_id)
            elif call.data == 'NO':
                bot.answer_callback_query(call.id)
                respond_input_distributed_people(user_id)
        elif user_mode == GROUP_DID_NOT_GATHER_CONFIRM:
            group_members = get_members(group_id)
            if call.data == 'YES':
                bot.answer_callback_query(call.id)
                for group_member in group_members:
                    VISITORS[user_id][group_member] = {'status': '-', 'leader': leader, 'reason': 'Группа не прошла'}
                df = get_visitors_df(user_id)
                logger.info(f'Saving the DF with {len(group_members)} size')
                db_access.save_visitors_to_db(df, ENGINE)
                respond_finish(user_id)
            elif call.data == 'NO':
                bot.answer_callback_query(call.id)
                respond_mark_visits(user_id, DATES[user_id], group_members)
        else:
            if call.data == 'REVIEW':
                # bot.edit_message(user_id, reply_markup=ReplyKeyboardRemove())
                respond_review(bot, leader, user_id, call.id)
            elif call.data == 'COMPLETE_VISITORS':
                respond_complete(bot, group_id, user_id, call.id)
            elif call.data == 'GROUP_DID_NOT_GATHER':
                respond_confirm_did_not_gather(user_id)
            # should not fall here if wrong user mode
            elif call.data != "TITLE":
                respond_visitor_selection(bot, leader, user_id, call.id, call.data)
    except IntegrityError as e:
        logger.error(e)
        bot.answer_callback_query(call.id, 'Произошла ошибка. Нам очень жаль 😔')
        bot.send_message(user_id, f'👺 Данные для группы {group_id} за дату {format_date(DATES[user_id])} уже были внесены',
                         reply_markup=ReplyKeyboardRemove())
    except Exception as e:
        capture_exception(e)
        logger.exception(e)
        bot.answer_callback_query(call.id, 'Произошла ошибка. Нам очень жаль 😔')


# Starting point of bot
@bot.message_handler(func=check_user_group, commands=['add', 'start'])
def select_group(message):
    try:
        user_id = message.from_user.id
        cleanup(user_id)
        user_info = check_user_group(message)
        username = user_info['username']
        logger.info(f'[User {user_id} (@{username})] Select Group')

        update_user_id(username, user_id)
        logger.info(f'User groups: {user_info["hgs"]}')
        group_ids = list(map(lambda x: x['group_id'], user_info['hgs']))
        if len(group_ids) > 1:
            set_user_mode(user_id, SELECT_GROUP)
            groups_menu = get_groups_markup(group_ids)
            bot.send_message(user_id, 'Привет! Выбери, пожалуйста, группу.', reply_markup=groups_menu)
        else:
            # if only one group is present, select it and go to select date
            respond_select_date(bot, user_id, username, group_ids[0])
    except Exception as e:
        capture_exception(e)
        logger.exception(e)


@bot.message_handler(func=check_user_group, regexp='Группа: ')
def select_date(message):
    try:
        user_id = message.from_user.id
        user_info = check_user_group(message)
        username = user_info['username']
        logger.info(f'[User {user_id} (@{username})] Select Date')
        update_user_id(username, user_id)
        group_id = message.text.replace('Группа: ', '')
        if group_id[0] in GROUP_ICONS and group_id[1] == ' ':
            group_id = group_id[2:]  # remove emoji
        group_ids = map(lambda x: x['group_id'], user_info['hgs'])
        if not group_id in group_ids:
            bot.reply_to(message, f'Ошибка в номере группы {group_id}, попробуй ввести еще раз')
            return
        logger.info(f'Requested to work with group id {group_id}')
        respond_select_date(bot, user_id, username, group_id)

    except Exception as e:
        capture_exception(e)
        logger.exception(e)


@bot.message_handler(func=check_user_group, regexp='Разослать напоминания')
def process_reminders(message):
    try:
        logger.info('Starting reminders...')
        allowed_usernames = [e[0] for e in db_access.get_allowed_reminder_usernames(ENGINE)]
        logger.info('Allowed usernames: ' + ','.join(allowed_usernames))
        df = send_reminders.get_users_for_reminder(config.min_age_to_send_reminder_in_days)
        sent_to = send_reminders.process_reminders(df, allowed_usernames)
        bot.reply_to(message, f'Разосланы напоминания лидерам: ' + ', '.join(sent_to))
    except Exception as e:
        logger.exception(e)


@bot.message_handler(func=check_user_group)
def handle_generic_messages(message):
    try:
        user_id = message.from_user.id
        user_info = check_user_group(message)
        username = user_info['username']
        user_mode = get_user_mode(user_id)
        logger.info(f'[User {user_id} (@{username})] Handling inbound message. State = {user_mode}')
        logger.debug(f'Inbound message: {message.text}')

        group_id = get_current_group_id(user_id)
        group_info = get_group_info(user_info, group_id)
        leader = group_info['leader']

        update_user_id(username, user_id)

        if user_mode == DATE:
            visit_date = parse_date(message.text)
            if visit_date:
                if visit_date > datetime.now().date():
                    respond_date_is_in_future(message)
                else:
                    group_members = get_members(group_id)
                    bot.send_message(user_id, f'Выбранная дата: {format_date(visit_date)}', reply_markup=ReplyKeyboardRemove())
                    DATES[user_id] = visit_date
                    respond_mark_visits(user_id, visit_date, group_members)
            else:
                respond_invalid_date_format(message)
                #select_date(message)

        elif user_mode == MARK_VISITORS:
            reason_for_db = list(filter(lambda reason: reason[1] == message.text, REASONS.values()))[0][0]
            VISITORS[user_id][ACTIVE_REASONS[user_id]]['reason'] = reason_for_db
            if group_members_checked(user_id):
                bot.send_message(user_id, f'Отлично! Теперь нажми «Подтвердить отметки»')
            else:
                bot.send_message(user_id, f'{ACTIVE_REASONS[user_id]}: {reason_for_db}\nПродолжай отмечать дальше.')
        elif user_mode == GUESTS:
            bot.send_message(user_id, f'Добавлен гость {message.text}')
            add_guest_vist(user_id, leader, message.text)
        elif user_mode == HG_SUMMARY:
            add_summary(user_id, group_info, message.text)
            respond_confirm_hg_summary(user_id)
        elif user_mode == TESTIMONIES_INPUT:
            add_testimonies(user_id, message.text)
            respond_confirm_testimonies(user_id)
        elif user_mode == PERSONAL_MEETING_INPUT:
            add_personal_meetings_feedback(user_id, message.text)
            respond_confirm_personal_meetings_feedback(user_id)
        elif user_mode == DISTRIBUTED_PEOPLE_INPUT:
            add_distributed_people(user_id, message.text)
            respond_confirm_distributed_people(user_id)
    except Exception as e:
        capture_exception(e)
        logger.exception(e)


init()
bot.polling()
