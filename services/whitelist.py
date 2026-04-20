import os
from services.http import Http
from config.logger import setup_logger
from database.mysql.connector import MySQLConnector
from config.config import CONFIG


class Whitelist:
    def __init__(self):
        self.db = MySQLConnector(CONFIG["mysql"])
        self.logger = setup_logger(self.__class__.__name__)
        
        # Kolom yang menjadi UNIQUE KEY di DB
        self.unique_keys = ["ktp_id", "ruas", "penempatan_gerbang"]

    def run_service(self):
        self.logger.info("Service whitelist running...")

        try:
            self.db.connect()

            ruas_id = os.getenv("IDRUAS")
            gerbang_id = os.getenv("IDGERBANG")

            rows = self._fetch_from_api(ruas_id, gerbang_id)

            if not rows or "data" not in rows or "data" not in rows["data"]:
                self.logger.warning("Tidak ada data valid dari API.")
                return

            data = rows["data"]["data"]

            if not data:
                self.logger.info("Tidak ada data untuk diproses.")
                return

            self.logger.info(f"Ditemukan {len(data)} data dari API.")

            mapped_data = [self._map_data(item) for item in data]

            # Debug UNIQUE KEY
            for d in mapped_data:
                self.logger.debug(
                    f"UNIQUE CHECK → ktp_id={d['ktp_id']} | ruas={d['ruas']} | penempatan={d['penempatan_gerbang']}"
                )
            
            #inser db
            self._save_to_db(mapped_data)

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

            base_url = f"{CONFIG['endpoint_url']}/api/v1/distribution/data/whitelist"
            params = []

            if ruas_id:
                params.append(f"ruas_id={ruas_id}")

            if gerbang_id:
                params.append(f"gerbang_id={gerbang_id}")

            url = f"{base_url}?{'&'.join(params)}" if params else base_url

            self.logger.info(f"Request API URL: {url}")

            return Http.http_get(url, headers=headers)

        except Exception as e:
            self.logger.error(f"Error saat request API: {e}")
            return None

    def _map_data(self, item):
        return {
            "ktp_id": item.get("uid"),
            "no_registrasi": item.get("no_registrasi"),
            "tgl_terbit": item.get("tgl_terbit"),
            "signature_key": item.get("signature_key"),
            "tgl_kadaluarsa": item.get("tgl_kadaluwarsa"),
            "nama": item.get("nama_pengguna") or "",
            "ruas": str(item.get("ruas")),                     # pastikan string
            "penempatan_gerbang": str(item.get("penempatan_gerbang")),  # pastikan string
            "status": "1" if item.get("status_kartu") == "1" else "0",
            "isdeleted": "0",
            "datetimeint": item.get("datetimeint"),
        }

    def _save_to_db(self, mapped_data):
        if not mapped_data:
            self.logger.warning("mapped_data kosong, tidak ada yang disimpan.")
            return False

        columns = list(mapped_data[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join([f"%({c})s" for c in columns])

        # Kolom yang boleh update selain UNIQUE KEY
        update_cols = [c for c in columns if c not in self.unique_keys]
        update_clause = ", ".join([f"{c}=VALUES({c})" for c in update_cols])

        query = f"""
            INSERT INTO tbl_penerbitan_kartu_whitelist ({col_names})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """

        try:
            self.db.executemany(query, mapped_data)
            self.db.commit()
            self.logger.info(f"{len(mapped_data)} data berhasil disimpan/diupdate ke DB.")
            return True

        except Exception as e:
            self.logger.error(f"ERROR DB: {e}")
            self.logger.error("Query gagal! Ini biasanya terjadi jika UNIQUE KEY tidak cocok.")
            self.db.rollback()
            return False