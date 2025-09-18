from services.http import Http
from config.logger import setup_logger
from database.mysql.connector import MySQLConnector
from config.config import CONFIG


class Whitelist:
    def __init__(self):
        self.db = MySQLConnector(CONFIG['mysql'])
        self.logger = setup_logger(self.__class__.__name__)

    def run_service(self):
        self.logger.info("Service whitelist running...")
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
                f"{CONFIG['endpoint_url']}/api/v1/distribution/data/whitelist",
                headers=headers,
            )
        except Exception as e:
            self.logger.error(f"Error saat request: {e}")
            return None
        
    def _map_data(self, item):
        # Ambil field sederhana langsung
        data = {
            "identitas": item.get("identitas"),
            "no_registrasi": item.get("no_registrasi"),
            "status_pengajuan": item.get("status_pengajuan"),
            "status_kartu": item.get("status_kartu"),
            "uid": item.get("uid"),
            "tgl_terbit": item.get("tgl_terbit"),
            "penerbit": item.get("penerbit"),
            "organisasi_instansi": item.get("organisasi_instansi"),
            "unit_kerja": item.get("unit_kerja"),
        }

        # Flatten regional list → simpan sebagai string dipisahkan koma
        regional_list = item.get("regional") or []
        data["regional_ids"] = ",".join([r.get("id") for r in regional_list if r.get("id")])
        data["regional_namas"] = ",".join([r.get("nama") for r in regional_list if r.get("nama")])

        # Flatten ruas list → simpan sebagai string dipisahkan koma
        ruas_list = item.get("ruas") or []
        data["ruas_ids"] = ",".join([r.get("id") for r in ruas_list if r.get("id")])
        data["ruas_namas"] = ",".join([r.get("nama") for r in ruas_list if r.get("nama")])
        
        # Flatten gerbang list → simpan sebagai string dipisahkan koma
        gerbang_list = item.get("gerbang") or []
        data["gerbang_ids"] = ",".join([r.get("id") for r in gerbang_list if r.get("id")])
        data["gerbang_namas"] = ",".join([r.get("nama") for r in gerbang_list if r.get("nama")])

        return data



    def _save_to_db(self, mapped_data):
        if not mapped_data:
            return

        columns = list(mapped_data[0].keys())
        col_names = ", ".join(columns)
        placeholders = ", ".join([f"%({c})s" for c in columns])

        query = f"""
            INSERT INTO tbl_penerbitan_kartu_eksternal ({col_names})
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