import logging

from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask
from cdm_loader.repository.cdm_repository import CdmRepository
from app_config import AppConfig
from cdm_loader.cdm_message_processor_job import CdmMessageProcessor


app = Flask(__name__)


@app.get('/health')
def hello_world():
    return 'healthy'


if __name__ == '__main__':
    app.logger.setLevel(logging.DEBUG)

    config = AppConfig()
    pg_repository = CdmRepository(config.pg_warehouse_db())

    proc = CdmMessageProcessor(
        config.kafka_consumer(),
        pg_repository,
        app.logger
    )

    scheduler = BackgroundScheduler()
    scheduler.add_job(func=proc.run, trigger="interval", seconds=25)
    scheduler.start()

    app.run(debug=True, host='0.0.0.0', use_reloader=False)
