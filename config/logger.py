import logging
import os
from datetime import datetime
from colorama import Fore, Style, init

# Inisialisasi colorama agar warna otomatis reset
init(autoreset=True)

class ColoredFormatter(logging.Formatter):
    COLORS = {
        logging.DEBUG: Fore.LIGHTBLACK_EX,
        logging.INFO: Fore.CYAN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.LIGHTRED_EX,
    }

    def format(self, record):
        # Warna untuk level
        level_color = self.COLORS.get(record.levelno, Fore.WHITE)
        record.levelname = f"{level_color}{record.levelname}{Style.RESET_ALL}"

        # Tambahkan nama hanya jika ERROR ke atas
        if record.levelno >= logging.ERROR:
            record.msg = f"[{record.name}] - {record.msg}"

        # Buat bold
        record.msg = f"{Style.BRIGHT}{record.msg}{Style.RESET_ALL}"

        return super().format(record)

def setup_logger(name: str, level=logging.INFO):
    log_dir = "logs"
    os.makedirs(log_dir, exist_ok=True)

    log_filename = os.path.join(log_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    # Format default tanpa name
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    log_datefmt = '%Y-%m-%d %H:%M:%S'

    # File handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(logging.Formatter(log_format, datefmt=log_datefmt))
    logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(ColoredFormatter(log_format, datefmt=log_datefmt))
    logger.addHandler(console_handler)

    return logger
