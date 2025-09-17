from config.config import CONFIG
from config.logger import setup_logger
from services.penerbitan import Penerbitan

class Service:
    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)
        self.service = None

        service_name = CONFIG['service']

        if service_name.lower() == 'penerbitan':
            self.service = Penerbitan()
        else:
            self.logger.error("Service tidak terdaftar, tersedia 'penerbitan'.")

    def start(self):
        if self.service:
            print(self.service)
            self.service.run_service()
        else:
            self.logger.error("Service belum diinisialisasi.")