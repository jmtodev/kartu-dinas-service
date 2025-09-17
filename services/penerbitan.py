from services.http import Http
from config.logger import setup_logger
from database.pgsql.connector import PGSQLConnector
from config.config import CONFIG


class Penerbitan:
    def __init__(self):
        self.db = PGSQLConnector(CONFIG['pgsql'])
        self.logger = setup_logger(self.__class__.__name__)

    def run_service(self):
        self.logger.info("Service penerbitan running...")
        try:
            self.db.connect()
            rows = self._fetch_from_api()

            data = rows['data']['data']

            if not data:
                self.logger.info("Tidak ada data untuk diproses.")
                return

            self.logger.info(f"Ditemukan {len(data)} data.")
            for item in data:
                self._process_row(item)

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
                f"{CONFIG['endpoint_url']}/api/v1/distribution/data/penerbitan",
                headers=headers,
            )
        except Exception as e:
            self.logger.error(f"Error saat request: {e}")
            return None
        
    def _process_row(self, item):
        mapped_data = self._map_data(item)
        
        return mapped_data
        
    def _map_data(self, item):

        mapping = {
            "penerbit": "penerbit",
            "regional_id": "regional_id",
            "ruas_id": "ruas_id",
            "gerbang_id": "gerbang_id",
            "organisasi_instansi_nama": "organisasi_instansi_nama",
            "unit_kerja_nama": "unit_kerja_nama",
            "identitas": "identitas",
            "surat_pengajuan": "surat_pengajuan",
            "no_registrasi": "no_registrasi",
            "no_blacklist": "no_blacklist",
            "alasan_blacklist": "alasan_blacklist",
            "status_pengajuan": "status_pengajuan",
            "status_kartu": "status_kartu",
            "tgl_terbit": "tgl_terbit",
            "uid": "uid",
        }

        data = {to: item[from_] for from_, to in mapping.items()}

        return data


    # def _fetch_data(self):
    #     query = """
    #         SELECT id, noresi, ts, person, pay_total, vehicle,
    #             payment, pay_person, pay_vehicle,
    #             exit_barcode, gate_id,
	# 			JSON_UNQUOTE(JSON_EXTRACT(ticket_data, '$[0].barcode')) AS barcode
    #         FROM tbl_transaksi
    #         WHERE (flag_trx_ancol = 0 
    #         and payment NOT IN ('OPR'))
    #         LIMIT 1
    #     """
    #     return self.db.fetch(query)

    # def _flag_data(self, item_id):
    #     try:
    #         self.logger.info(f"Flagging data ID {item_id}...")
    #         query = """
    #             UPDATE tbl_transaksi 
    #             SET flag_trx_ancol = %s
    #             WHERE id = %s
    #         """
    #         self.db.execute(query, (1, item_id))
    #         self.db.commit()
    #     except Exception as e:
    #         self.logger.error(f"Gagal update flag untuk ID {item_id}: {e}")
