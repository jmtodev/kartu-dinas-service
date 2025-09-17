import os

CONFIG = {
    "service": os.getenv("SERVICE"),
    "endpoint_url": os.getenv("ENDPOINT_URL", "").rstrip("/"),
    "schedule_interval": int(os.getenv("SCHEDULE_INTERVAL", 5)),
    "xapikey": os.getenv("XAPIKEY", None),
    "pgsql": {
        "host": os.getenv("DB_HOST", "localhost"),
        "user": os.getenv("DB_USERNAME", "postgres"),
        "port": int(os.getenv("DB_PORT", 5432)),
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_DATABASE", ""),
    },
}
