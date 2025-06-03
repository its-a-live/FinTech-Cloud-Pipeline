import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from app_config import AppConfig
from stg_loader.stg_message_processor_job import StgMessageProcessor
from stg_loader.repository.stg_repository import StgRepository

app = Flask(__name__)



@app.get('/health')
def health():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    config = AppConfig()
    pg_repository = StgRepository(config.pg_warehouse_db())

    proc = StgMessageProcessor(config.kafka_consumer(),
                               config.kafka_producer(),
                               config.redis_client(),
                               pg_repository,
                               config.batch_size, app.logger)

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=config.DEFAULT_JOB_INTERVAL)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
