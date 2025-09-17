import psycopg2
import psycopg2.extras
from database.base import BaseDatabaseConnector
from config.logger import setup_logger


class PGSQLConnector(BaseDatabaseConnector):
    def __init__(self, config):
        self.config = config
        self.conn = None
        self.cursor = None
        self.logger = setup_logger(self.__class__.__name__)

    def __ensure_params(self, value):
        """Pastikan nilai untuk parameter query adalah tuple."""
        if isinstance(value, (tuple, list)):
            return tuple(value)
        return (value,)

    def connect(self):
        self.logger.info(
            f"Menghubungkan ke database PostgreSQL: {self.config['host']}:{self.config['port']}"
        )
        try:
            self.conn = psycopg2.connect(
                host=self.config["host"],
                user=self.config["user"],
                port=self.config["port"],
                password=self.config["password"],
                dbname=self.config["database"],
                cursor_factory=psycopg2.extras.RealDictCursor,  # hasil query dict
            )
            self.cursor = self.conn.cursor()
            self.logger.info("Koneksi PostgreSQL berhasil!")
            return self.conn
        except psycopg2.Error as err:
            self.logger.error(f"Gagal terhubung ke database PostgreSQL: {err}")
            raise

    def fetch(self, query: str, params: tuple = ()):
        params = self.__ensure_params(params)
        try:
            self.cursor.execute(query, params)
            result = self.cursor.fetchall()
            return result
        except psycopg2.Error as err:
            self.logger.error(f"Error saat menjalankan query: {err}")
            raise

    def execute(self, query: str, params: tuple = ()):
        params = self.__ensure_params(params)
        try:
            self.cursor.execute(query, params)
            self.logger.info("Query berhasil dieksekusi.")
        except psycopg2.Error as err:
            self.logger.error(f"Error saat menjalankan query: {err}")
            raise

    def commit(self):
        self.logger.info("Menyimpan perubahan ke database...")
        try:
            self.conn.commit()
            self.logger.info("Perubahan berhasil disimpan.")
        except psycopg2.Error as err:
            self.logger.error(f"Gagal menyimpan perubahan: {err}")
            raise

    def close(self):
        if self.cursor:
            self.logger.info("Menutup cursor...")
            self.cursor.close()
        if self.conn:
            self.logger.info("Menutup koneksi ke database...")
            self.conn.close()
