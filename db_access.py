import pandas as pd

VISITORS_TABLE = 'data_for_bot_visitors'
USERNAMES_TABLE = 'data_for_bot_usernames'
VISITS_TABLE = 'data_from_bot_visitors'
QUESTIONS_TABLE = 'data_from_bot_questions'
USERS_TABLE = 'data_from_bot_users'
ALLOWED_REMINDER_USERNAMES_TABLE = 'allowed_reminder_usernames'
MASTER_DATA_HISTORY_TABLE = 'master_data_history'
KEY_VALUE_TABLE = 'key_value_storage'


def select_leader_usernames(engine):
    usernames_df = pd.read_sql(f'select * from {USERNAMES_TABLE}', engine)
    users = {}
    for i, group in usernames_df.iterrows():
        for username in group.usernames.split(','):
            username_no_handle = username.replace('@', '')
            if username_no_handle:
                user_info = users.get(username_no_handle, {'user_id': None, 'username': username_no_handle, 'hgs': []})
                user_info['hgs'].append({'group_id': group['id_hg'], 'leader': group['leader']})
                users[username_no_handle] = user_info

    return users


def select_group_members(group_id, engine):
    members_df = pd.read_sql(f"select name from {VISITORS_TABLE} where id_hg = '{group_id}'", engine)
    members = members_df['name'].tolist()
    return members


def save_visitors_to_db(df, engine):
    df.to_sql(VISITS_TABLE, engine, if_exists='append', index=None)


def save_questions_to_db(df, engine):
    df.to_sql(QUESTIONS_TABLE, engine, if_exists='append', index=None)


def get_leader_guests(leader, engine):
    return [m[0] for m in list(engine.execute(
        f"SELECT distinct(name) FROM {VISITS_TABLE} WHERE type_person='Гость' AND name_leader='{leader}'"))]


def get_group_guests(group_id, engine):
    return [m[0] for m in list(engine.execute(
        f"SELECT distinct(name) FROM {VISITS_TABLE} WHERE type_person='Гость' AND id_hg='{group_id}' AND date > current_date - interval '60 day'"))]


def get_user_data(username, engine):
    return list(engine.execute(
        f"SELECT telegram_username, telegram_uid, user_state FROM {USERS_TABLE} WHERE telegram_username='{username}'"))


def save_user_data(telegram_username, telegram_uid, engine):
    engine.execute(f"INSERT INTO {USERS_TABLE} (telegram_username, telegram_uid) VALUES ('{telegram_username}', {telegram_uid}) ON CONFLICT (telegram_username) DO UPDATE SET telegram_uid = {telegram_uid}, updated_ts = now()")


def get_last_visits(engine):
    sql = "select n.id_hg as id_hg, max(leader) as leader, max(v.date) as max_date, " \
          "replace(split_part(max(n.usernames), ',', 1), '@', '') as leader_username " \
          f"from {USERNAMES_TABLE} n " \
          f"left join {VISITS_TABLE} v on n.id_hg = v.id_hg " \
          "group by n.id_hg"
    return pd.read_sql(sql, engine)


def get_allowed_reminder_usernames(engine):
    return list(engine.execute(f"SELECT telegram_username FROM {ALLOWED_REMINDER_USERNAMES_TABLE}"))


def get_master_data_for_today(engine):
    sql = "select g.id_hg as id_hg, max(m.status_of_hg) as status, max(m.type_age) as type_age, max(m.weekday) as weekday, max(m.time_of_hg) as time_of_hg " \
          f"from {USERNAMES_TABLE} g " \
          f"left join {MASTER_DATA_HISTORY_TABLE} m on g.id_hg = m.id_hg " \
          "and m.valid_from <= now() and m.valid_to >= now() " \
          "group by g.id_hg"
    return pd.read_sql(sql, engine)


def get_multi_key_value(key, engine):
    sql = f"select value from {KEY_VALUE_TABLE} where key = '{key}' and is_enabled order by multivalue_seq_number"
    return list(map(lambda x: x[0].replace('\\n', '\n'), engine.execute(sql)))


def get_single_key_value(key, engine):
    multi_key_value = get_multi_key_value(key, engine)
    if len(multi_key_value) != 1:
        raise ValueError(f"{len(multi_key_value)} enabled values found for key {key} (expected 1)")
    return multi_key_value[0]


