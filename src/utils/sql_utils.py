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
