import pandas as pd

VISITORS_TABLE = 'data_for_bot_visitors'
USERNAMES_TABLE = 'data_for_bot_usernames'
VISITS_TABLE = 'data_from_bot_visitors'
QUESTIONS_TABLE = 'data_from_bot_questions'


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
        f"SELECT distinct(name) FROM {VISITS_TABLE} WHERE type_person='Гость' AND id_hg='{group_id}'"))]