from services.http import Http
from config.logger import setup_logger
from database.mysql.connector import MySQLConnector
from config.config import CONFIG


class Blacklist:
    def __init__(self):
        self.db = MySQLConnector(CONFIG['mysql'])
        self.logger = setup_logger(self.__class__.__name__)

    def run_service(self):
        self.logger.info("Service blacklist running...")
        try:
            self.db.connect()
            rows = self._fetch_from_api()

            data = rows['data']['data']

            if not data:
                self.logger.info("Tidak ada data untuk diproses.")
                return

            self.logger.info(f"Ditemukan {len(data)} data.")

            mapped_data = [self._map_data(item) for item in data]

            # save ke db
            self._save_to_db(mapped_data)

        except Exception as e:
            self.logger.error(f"Terjadi error saat menjalankan service: {e}")
        finally:
            try:
                self.db.close()
            except Exception as e:
                self.logger.warning(f"Gagal menutup koneksi: {e}")


    def _fetch_from_api(self):
        try:
            headers = {
                "x-api-key": CONFIG["xapikey"],
            }

            return Http.http_get(
                f"{CONFIG['endpoint_url']}/api/v1/distribution/data/blacklist",
                headers=headers,
            )
        except Exception as e:
            self.logger.error(f"Error saat request: {e}")
            return None
        
    def _map_data(self, item):
        data = {
            "uid": item.get("uid"),
            "alasan": item.get("alasan"),
            "no_kartu": item.get("no_kartu"),
            "penerbitan_id": item.get("penerbitan_id"),
            "tgl_kadaluwarsa": item.get("tgl_kadaluwarsa"),
            "no_blacklist": item.get("no_blacklist"),
        }

        return data

    def _save_to_db(self, mapped_data):
        if not mapped_data:
            return

        columns = list(mapped_data[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join([f"%({c})s" for c in columns])

        query = f"""
            INSERT INTO tbl_blacklist_kartu ({col_names})
            VALUES ({placeholders})
        """

        try:
            with self.db.conn.cursor() as cur:
                cur.executemany(query, mapped_data)
            
            # commit supaya data benar-benar tersimpan
            self.db.conn.commit()

            self.logger.info(f"{len(mapped_data)} data berhasil disimpan ke DB.")

        except Exception as e:
            self.db.conn.rollback()
            self.logger.error(f"Gagal simpan ke DB, simpan dibatalkan: {e}")
            raise