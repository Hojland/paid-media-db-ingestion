import sqlalchemy


def create_engine(db_config: dict, db_name: str=None, db_type: str='postgres'):
    """Creates a sqlalchemy engine, with specified connection information

    Arguments
    ---------
    db_config: a dictionary with configurations for a resource
    db_name: Overwrite the database from the config
    db_type: a string with the database type for prepending the connection string

    Returns
    -------
    engine: sqlalchemy.Engine
    """
    if db_type == 'postgres':
       prepend = 'postgresql+psycopg2'
    elif db_type == 'mssql':
       prepend = 'mssql+pyodbc'
    elif db_type == 'mysql' or db_type == 'mariadb':
       prepend = 'mysql+pymysql'

    uid, psw, host, port, db = db_config.values()
    if db_name:
       db = db_name
    conn_string = f"{db_type}://{uid}:{psw}@{host}:{port}/{db}"
    engine = sqlalchemy.create_engine(conn_string)
    return engine


def get_latest_date_in_table(db_engine: sqlalchemy.engine, table_name: str):
    latest_date = db_engine.execute(f'SELECT MAX(date) FROM output.{table_name}').scalar()
    if not latest_date:
        raise IndexError("No data in variable 'date' in table")
    return latest_date


def delete_date_entries_in_table(db_engine: sqlalchemy.engine, min_date: str, table_name: str):
    db_engine.execute(f'DELETE FROM output.{table_name} WHERE date>="{min_date}";')
