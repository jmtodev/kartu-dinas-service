from services.http import Http
from config.logger import setup_logger
from database.mysql.connector import MySQLConnector
from config.config import CONFIG
import json


class Blacklist:
    def __init__(self):
        self.db = MySQLConnector(CONFIG["mysql"])
        self.logger = setup_logger(self.__class__.__name__)

    def run_service(self):
        self.logger.info("Service blacklist running...")
        try:
            self.db.connect()
            rows = self._fetch_from_api()
            if not rows or "data" not in rows or "data" not in rows["data"]:
                self.logger.info("Tidak ada data valid dari API.")
                return

            data = rows["data"]["data"]

            if not data:
                self.logger.info("Tidak ada data untuk diproses.")
                return

            self.logger.info(f"Ditemukan {len(data)} data.")

            mapped_data = [self._map_data(item) for item in data]

            if self._save_to_db(mapped_data):
                ids = [str(item.get("id")) for item in data if item.get("id")]
                ids_str = ",".join(ids)
                self._flag_data(ids_str)

        except Exception as e:
            self.logger.error(f"Terjadi error saat menjalankan service: {e}")
        finally:
            try:
                self.db.close()
            except Exception as e:
                self.logger.warning(f"Gagal menutup koneksi: {e}")

    def _fetch_from_api(self):
        try:
            headers = {"x-api-key": CONFIG["xapikey"]}
            return Http.http_get(
                f"{CONFIG['endpoint_url']}/api/v1/distribution/data/blacklist",
                headers=headers,
            )
        except Exception as e:
            self.logger.error(f"Error saat request: {e}")
            return None

    def _map_data(self, item):
        return {
            "uuid": item.get("uid"),
            "no_registrasi": item.get("no_blacklist"),
            "info": item.get("alasan"),
            "jenis_ktp": item.get("jenis_kartu_id"),
            "tick": None,
            "penempatan_gerbang": None,
        }
        

    def _save_to_db(self, mapped_data):        
        if not mapped_data:
            return False

        columns = list(mapped_data[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join([f"%({c})s" for c in columns])

        # Tentukan kolom unik / primary key yang TIDAK boleh diupdate
        unique_keys = {"uid", "no_blacklist", "penerbitan_id"}  

        # Bangun update clause hanya untuk kolom non-key
        update_cols = [c for c in columns if c not in unique_keys]
        update_clause = ", ".join([f"{c}=VALUES({c})" for c in update_cols])

        query = f"""
            INSERT INTO tbl_blacklist ({col_names})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """

        try:
            self.db.executemany(query, mapped_data)
            self.db.commit()
            self.logger.info(f"{len(mapped_data)} data berhasil disimpan/diupdate ke DB.")
            return True
        except Exception as e:
            self.db.rollback()
            self.logger.error(f"Gagal simpan ke DB, simpan dibatalkan: {e}")
            return False


    def _flag_data(self, ids: str):
        try:
            headers = {"x-api-key": CONFIG["xapikey"]}
            payload = {"blacklist_ids": ids}

            self.logger.info("Memulai flagging data...")

            return Http.http_patch(
                f"{CONFIG['endpoint_url']}/api/v1/distribution/data/blacklist",
                payload=payload,
                headers=headers,
            )
        except Exception as e:
            self.logger.error(f"Error saat request flag data: {e}")
            return None