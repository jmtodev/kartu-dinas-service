import time
from apscheduler.schedulers.background import BackgroundScheduler
from config.logger import setup_logger
from config.config import CONFIG
from services.service import Service

# Inisialisasi logger (hanya sekali saat aplikasi dijalankan)
logger = setup_logger(__name__)

# Fungsi utama yang dipanggil oleh scheduler
def main():
    try:
        # start service
        service = Service()
        service.start()

    except Exception as e:
        logger.error(f"Error: {e}")

# Fungsi untuk menjalankan scheduler
def run_scheduler():
    interval = CONFIG.get("schedule_interval", 5)  # Ambil interval dari konfigurasi (.env), default 5 menit

    scheduler = BackgroundScheduler()

    scheduler.add_job(
        main,
        'interval',
        minutes=interval,
        max_instances=1,           # Pastikan hanya satu instance job yang berjalan
        misfire_grace_time=10      # Job masih bisa dijalankan hingga 10 detik setelah jadwal
    )

    logger.info(f"Scheduler started. Running every {interval} minutes...")

    scheduler.start()

    # Menjaga thread utama tetap hidup
    try:
        while True:
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()  # Berhentikan scheduler saat aplikasi ditutup

if __name__ == "__main__":
    main()
    run_scheduler()