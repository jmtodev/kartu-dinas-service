from database import PGSQLConnector

def get_connector(db_type: str):
    if db_type == "pgsql":
        return PGSQLConnector
    else:
        raise ValueError(f"Database type '{db_type}' not supported")
