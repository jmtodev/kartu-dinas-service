from database.mysql.connector import MySQLConnector

def get_connector(db_type: str):
    if db_type == "mysql":
        return MySQLConnector
    else:
        raise ValueError(f"Database type '{db_type}' not supported")