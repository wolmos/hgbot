import threading
from loguru import logger
import schedule
import time
import send_reminders


class ReminderThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.name = 'ReminderThread'
        self.daemon = True

    def run(self):
        logger.info('Reminder thread started')
        self.init_jobs()

        while True:
            schedule.run_pending()
            time.sleep(1)

    def init_jobs(self):
        schedule.every().monday.at('11:00').do(self.send_reminders_for_old_hgs).tag('send_reminders_for_old_hgs')

    def send_reminders_for_old_hgs(self):
        logger.info(f'Executing check_old_hgs. All jobs: {schedule.get_jobs()}')
        df = send_reminders.get_users_for_reminder()
        sent_to = send_reminders.process_reminders(df)

