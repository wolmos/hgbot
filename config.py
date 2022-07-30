import os

# Config file with secrets.
# DO NOT PUSH SECRETS TO GIT!

# PostgreSQL
db_user = os.environ['DB_USER']
db_password = os.environ['DB_PASSWORD']
db_hostname = os.environ['DB_HOSTNAME']
db_port = 6432
db_name = os.environ['DB_NAME']

# Telegram bot
bot_token = os.environ['BOT_TOKEN']

# Sentry
sentry_url = os.environ['SENTRY_URL']

# Other
min_age_to_send_reminder_in_days = 7

# List of admins
admins = os.environ['ADMINS']
