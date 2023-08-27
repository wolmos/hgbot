import threading
from loguru import logger
import schedule
import time
import send_reminders
import datetime_helper


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
        # Init reminders for old hgs
        schedule.every().monday.at('11:00').do(self.send_reminders_for_old_hgs).tag('send_reminders_for_old_hgs')

        # Init reminders for today's hgs
        master_data_df = send_reminders.get_actual_master_data()
        for i, row in master_data_df.iterrows():
            weekday = row['weekday']
            time_of_hg = row['time_of_hg']
            id_hg = row['id_hg']
            self.try_init_reminder_for_today(weekday, time_of_hg, id_hg)

    def try_init_reminder_for_today(self, weekday, time_of_hg, id_hg):
        logger.info(f'Init reminders for hg {id_hg}: {weekday} {time_of_hg}')
        if time_of_hg is None or weekday is None:
            return

        time_before_hg = datetime_helper.get_reminder_before_hg_time_utc(time_of_hg)
        time_after_hg = datetime_helper.get_reminder_after_hg_time_utc(time_of_hg)

        if time_before_hg is None or time_after_hg is None:
            return

        logger.info(f'Calculated times in UTC: {time_before_hg} {time_after_hg}')

        if weekday.lower() == 'понедельник':
            schedule.every().sunday.at(time_before_hg).do(self.send_reminders_before_hg, id_hg=id_hg,
                                                          time_of_hg=time_of_hg).tag('send_reminders_before_hg')
            schedule.every().monday.at(time_after_hg).do(self.send_reminders_after_hg, id_hg=id_hg).tag(
                'send_reminders_after_hg')
        elif weekday.lower() == 'вторник':
            schedule.every().monday.at(time_before_hg).do(self.send_reminders_before_hg, id_hg=id_hg,
                                                          time_of_hg=time_of_hg).tag('send_reminders_before_hg')
            schedule.every().tuesday.at(time_after_hg).do(self.send_reminders_after_hg, id_hg=id_hg).tag(
                'send_reminders_after_hg')
        elif weekday.lower() == 'среда':
            schedule.every().tuesday.at(time_before_hg).do(self.send_reminders_before_hg, id_hg=id_hg,
                                                           time_of_hg=time_of_hg).tag('send_reminders_before_hg')
            schedule.every().wednesday.at(time_after_hg).do(self.send_reminders_after_hg, id_hg=id_hg).tag(
                'send_reminders_after_hg')
        elif weekday.lower() == 'четверг':
            schedule.every().wednesday.at(time_before_hg).do(self.send_reminders_before_hg, id_hg=id_hg,
                                                             time_of_hg=time_of_hg).tag('send_reminders_before_hg')
            schedule.every().thursday.at(time_after_hg).do(self.send_reminders_after_hg, id_hg=id_hg).tag(
                'send_reminders_after_hg')
        elif weekday.lower() == 'пятница':
            schedule.every().thursday.at(time_before_hg).do(self.send_reminders_before_hg, id_hg=id_hg,
                                                            time_of_hg=time_of_hg).tag('send_reminders_before_hg')
            schedule.every().friday.at(time_after_hg).do(self.send_reminders_after_hg, id_hg=id_hg).tag(
                'send_reminders_after_hg')
        elif weekday.lower() == 'суббота':
            schedule.every().friday.at(time_before_hg).do(self.send_reminders_before_hg, id_hg=id_hg,
                                                          time_of_hg=time_of_hg).tag('send_reminders_before_hg')
            schedule.every().saturday.at(time_after_hg).do(self.send_reminders_after_hg, id_hg=id_hg).tag(
                'send_reminders_after_hg')
        elif weekday.lower() == 'воскресенье':
            schedule.every().saturday.at(time_before_hg).do(self.send_reminders_before_hg, id_hg=id_hg,
                                                            time_of_hg=time_of_hg).tag('send_reminders_before_hg')
            schedule.every().sunday.at(time_after_hg).do(self.send_reminders_after_hg, id_hg=id_hg).tag(
                'send_reminders_after_hg')
        else:
            logger.warning(f'Unexpected weekday: {weekday}')

    def send_reminders_for_old_hgs(self):
        try:
            logger.info(f'Executing check_old_hgs')
            df = send_reminders.get_users_for_reminder()
            send_reminders.process_reminders(df)
        except Exception as e:
            logger.exception(e)

    def send_reminders_before_hg(self, id_hg, time_of_hg):
        try:
            logger.info(f'Reminder 2 hours before hg: {id_hg}')
            send_reminders.send_reminder_before_hg(id_hg, time_of_hg)
        except Exception as e:
            logger.exception(e)

    def send_reminders_after_hg(self, id_hg):
        try:
            logger.info(f'Reminder 3 hours after hg: {id_hg}')
            send_reminders.send_reminder_after_hg(id_hg)
        except Exception as e:
            logger.exception(e)
