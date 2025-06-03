from datetime import datetime
from logging import Logger
from uuid import UUID
from lib.kafka_connect import KafkaConsumer
from cdm_loader.repository.cdm_repository import CdmRepository
from cdm_loader.repository.cdm_model import *

class CdmMessageProcessor:
    def __init__(self,
                 consumer: KafkaConsumer,
                 repository: CdmRepository,
                 logger: Logger,
                 ) -> None:

        self._consumer = consumer
        self._cdm_repository = repository
        self._logger = logger
        self._batch_size = 100

    def run(self) -> None:
        self._logger.info(f"{datetime.utcnow()}: START")

        for _ in range(self._batch_size):
            msg = self._consumer.consume()
            if not msg:
                break

            user_products_data = msg.get("message")
            user_products = [ProductCounters(**x) for x in user_products_data]
            for product in user_products:
                self._cdm_repository.user_product_counters_insert(product)

            user_id = msg.get("user_id")
            self._cdm_repository.user_category_counters_insert(user_id)

        self._logger.info(f"{datetime.utcnow()}: FINISH")
