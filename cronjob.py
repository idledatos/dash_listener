# Package Scheduler.
from apscheduler.schedulers.blocking import BlockingScheduler

# Main cronjob function.
from listener import twitter_listener

# Create an instance of scheduler and add function.
scheduler = BlockingScheduler()
scheduler.add_job(twitter_listener, "interval", hours=1)

scheduler.start()