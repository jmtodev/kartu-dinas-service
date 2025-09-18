import os

CONFIG = {
    "service": os.getenv("SERVICE"),
    "endpoint_url": os.getenv("ENDPOINT_URL", "").rstrip("/"),
    "schedule_interval": int(os.getenv("SCHEDULE_INTERVAL", 5)),
    "xapikey": os.getenv("XAPIKEY", None),
    "mysql": {
        "host": os.getenv("DB_HOST", "localhost"),
        "user": os.getenv("DB_USERNAME", "root"),   # default mysql user
        "port": int(os.getenv("DB_PORT", 3306)),    # default mysql port
        "password": os.getenv("DB_PASSWORD", ""),
        "database": os.getenv("DB_DATABASE", ""),
    },
}