from services.http import Http
from config.logger import setup_logger
from database.mysql.connector import MySQLConnector
from config.config import CONFIG


class Blacklist:
    api_token = None

    def __init__(self):
        self.db = MySQLConnector(CONFIG["mysql"])
        self.logger = setup_logger(self.__class__.__name__)

        self.unique_keys = ["uuid"]

    @staticmethod
    def _hex_to_decimal_little_endian(hex_str: str) -> int:
        if not hex_str:
            return 0

        bytes_reversed = "".join(
            [hex_str[i:i + 2] for i in range(0, len(hex_str), 2)][::-1]
        )

        return int(bytes_reversed, 16)

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

            self._save_to_db(mapped_data)

        except Exception as e:
            self.logger.error(f"Terjadi error saat menjalankan service: {e}")

        finally:
            try:
                self.db.close()
            except Exception as e:
                self.logger.warning(f"Gagal menutup koneksi: {e}")

    def _request_api_token(self):
        try:
            url = f"{CONFIG['endpoint_url']}/api/v1/auth/client/requestToken"

            body = {
                "nama": CONFIG["username"],
                "password": CONFIG["password"],
            }

            self.logger.info("Request token API...")

            response = Http.http_post(url, json=body)

            if not response or response.get("success") is not True:
                self.logger.error(f"Gagal request token: {response}")
                return None

            token = response.get("data", {}).get("client", {}).get("token")

            if not token:
                self.logger.error(f"Token tidak ditemukan di response: {response}")
                return None

            Blacklist.api_token = token
            self.logger.info("Token API berhasil didapatkan.")
            return token

        except Exception as e:
            self.logger.error(f"Error saat request token API: {e}")
            return None

    def _fetch_from_api(self):
        try:
            if not Blacklist.api_token:
                self.logger.info("Token belum ada, request token baru...")

                token = self._request_api_token()

                if not token:
                    self.logger.error("Tidak bisa request API karena token kosong.")
                    return None
            else:
                self.logger.info("Token sudah ada, langsung fetch data.")

            response = self._fetch_blacklist()

            if self._is_unauthorized(response):
                self.logger.warning(
                    "Token unauthorized/expired. Request token baru lalu retry fetch data..."
                )

                Blacklist.api_token = None

                token = self._request_api_token()

                if not token:
                    self.logger.error("Gagal request token baru.")
                    return None

                response = self._fetch_blacklist()

            return response

        except Exception as e:
            self.logger.error(f"Error saat request API: {e}")
            return None

    def _fetch_blacklist(self):
        headers = {
            "Authorization": f"Bearer {Blacklist.api_token}"
        }

        url = f"{CONFIG['endpoint_url']}/api/v1/distribution/data/blacklist"

        self.logger.info(f"Request API URL: {url}")

        return Http.http_get(url, headers=headers)

    def _is_unauthorized(self, response):
        if not response:
            return False

        if not isinstance(response, dict):
            return False

        status_code = response.get("status_code") or response.get("code")
        message = str(response.get("message", "")).lower()
        error = str(response.get("error", "")).lower()

        return (
            status_code == 401
            or message == "unauthorized"
            or "unauthorized" in message
            or "unauthorised" in message
            or "expired" in message
            or "kadaluarsa" in message
            or "kadaluwarsa" in message
            or "unauthorized" in error
            or "expired" in error
        )

    def _map_data(self, item):
        return {
            "uuid_origin": item.get("uid"),
            "uuid": Blacklist._hex_to_decimal_little_endian(item.get("uid")),
            "no_registrasi": item.get("no_blacklist"),
            "info": item.get("alasan"),
            "jenis_ktp": item.get("jenis_kartu_id"),
            "tick": item.get("datetimeint"),
            "penempatan_gerbang": None,
        }

    def _save_to_db(self, mapped_data):
        if not mapped_data:
            self.logger.warning("mapped_data kosong, tidak ada yang disimpan.")
            return False

        columns = list(mapped_data[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join([f"%({c})s" for c in columns])

        update_cols = [c for c in columns if c not in self.unique_keys]
        update_clause = ", ".join([f"{c}=VALUES({c})" for c in update_cols])

        query = f"""
            INSERT INTO tbl_blacklist ({col_names})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
        """

        try:
            self.db.executemany(query, mapped_data)
            self.db.commit()
            self.logger.info(f"{len(mapped_data)} data berhasil disimpan/diupdate.")
            return True

        except Exception as e:
            self.logger.error(f"Query gagal: {e}")
            self.logger.warning("Query gagal, tetapi tidak dilakukan rollback.")
            return False