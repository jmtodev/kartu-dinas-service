from config.config import CONFIG
from config.logger import setup_logger
from services.penerbitan import Penerbitan
from services.whitelist import Whitelist
from services.blacklist import Blacklist

class Service:
    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)
        self.service = None

        service_name = CONFIG['service']

        if service_name.lower() == 'penerbitan':
            self.service = Penerbitan()
        elif service_name.lower() == 'whitelist':
            self.service = Whitelist()
        elif service_name.lower() == 'blacklist':
            self.service = Blacklist()
        else:
            self.logger.error("Service tidak terdaftar, tersedia 'penerbitan','whitelist','blacklist'.")

    def start(self):
        if self.service:
            print(self.service)
            self.service.run_service()
        else:
            self.logger.error("Service belum diinisialisasi.")