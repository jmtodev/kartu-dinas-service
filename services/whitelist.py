import os
from services.http import Http
from config.logger import setup_logger
from database.mysql.connector import MySQLConnector
from config.config import CONFIG


class Whitelist:
    def __init__(self):
        self.db = MySQLConnector(CONFIG["mysql"])
        self.logger = setup_logger(self.__class__.__name__)

    def run_service(self):
        self.logger.info("Service whitelist running...")
        try:
            self.db.connect()
            
            ruas_id = os.getenv("IDRUAS");
            gerbang_id = os.getenv("IDGERBANG")
            
            rows = self._fetch_from_api(ruas_id, gerbang_id)
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

    def _fetch_from_api(self, ruas_id=None, gerbang_id=None):
        try:
            headers = {"x-api-key": CONFIG["xapikey"]}

            # Bangun URL dengan parameter opsional
            base_url = f"{CONFIG['endpoint_url']}/api/v1/distribution/data/whitelist"
            params = []

            if ruas_id is not None:
                params.append(f"ruas_id={ruas_id}")
            if gerbang_id is not None:
                params.append(f"gerbang_id={gerbang_id}")

            if params:
                url = f"{base_url}?{'&'.join(params)}"
            else:
                url = base_url

            return Http.http_get(url, headers=headers)

        except Exception as e:
            self.logger.error(f"Error saat request: {e}")
            return None


    def _map_data(self, item):
        mapping = {
            "1": "1",
            "3": "0",
        }

        return {
            "ktp_id": item.get("uid"),
            "no_registrasi": item.get("no_registrasi"),
            "tgl_terbit": item.get("tgl_terbit"),
            "signature_key": item.get("signature_key"),
            "tgl_kadaluarsa": item.get("tgl_kadaluwarsa"),
            "nama": item.get("nama_pengguna"),
            "ruas": item.get("ruas"),
            "penempatan_gerbang": item.get("penempatan_gerbang"),
            "status": mapping.get(item.get("status_kartu"), "0"),
            "isdeleted": "0",
            "datetimeint": item.get("datetimeint"),
        }

    def _save_to_db(self, mapped_data):        
        if not mapped_data:
            return False

        columns = list(mapped_data[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join([f"%({c})s" for c in columns])

        # Kolom unik / primary key jangan ikut di-update
        # unique_keys = {"ktp_id", "no_registrasi"}

        # update_cols = [c for c in columns if c not in unique_keys]
        # update_clause = ", ".join([f"{c}=VALUES({c})" for c in update_cols])

        # query = f"""
        #     INSERT INTO tbl_penerbitan_kartu_whitelist ({col_names})
        #     VALUES ({placeholders})
        #     ON DUPLICATE KEY UPDATE {update_clause}
        # """
        
        query = f"""
            INSERT INTO tbl_penerbitan_kartu_whitelist ({col_names})
            VALUES ({placeholders})
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
            payload = {"whitelist_ids": ids}

            self.logger.info("Memulai flagging data...")

            return Http.http_patch(
                f"{CONFIG['endpoint_url']}/api/v1/distribution/data/whitelist",
                payload=payload,
                headers=headers,
            )
        except Exception as e:
            self.logger.error(f"Error saat request flag data: {e}")
            return None
