import sqlalchemy
import pandas as pd

from utils import utils


def create_engine(db_config: dict, db_name: str = None, db_type: str = "postgres"):
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
    if db_type == "postgres":
        prepend = "postgresql+psycopg2"
    elif db_type == "mssql":
        prepend = "mssql+pyodbc"
    elif db_type == "mysql" or db_type == "mariadb":
        prepend = "mysql+pymysql"

    uid, psw, host, port, db = db_config.values()
    if db_name:
        db = db_name
    conn_string = f"{db_type}://{uid}:{psw}@{host}:{port}/{db}"
    engine = sqlalchemy.create_engine(conn_string)
    return engine


def get_latest_date_in_table(db_engine: sqlalchemy.engine, table_name: str):
    latest_date = db_engine.execute(
        f"SELECT MAX(date) FROM output.{table_name}"
    ).scalar()
    if not latest_date:
        raise IndexError("No data in variable 'date' in table")
    return latest_date


def delete_date_entries_in_table(
    db_engine: sqlalchemy.engine, min_date: str, table_name: str
):
    db_engine.execute(f'DELETE FROM output.{table_name} WHERE date>="{min_date}";')


def delete_table(db_engine: sqlalchemy.engine, table: str):
    db_engine.execute(f"DROP TABLE {table}")


def truncate_table(db_engine: sqlalchemy.engine, table: str):
    db_engine.execute(f"TRUNCATE TABLE {table}")


def table_exists(db_engine: sqlalchemy.engine, schema_name: str, table_name: str):
    exists_num = db_engine.execute(
        f"""
    SELECT EXISTS (SELECT * 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_SCHEMA = '{schema_name}' 
        AND  TABLE_NAME = '{table_name}')
    """
    ).scalar()
    if exists_num == 0:
        exists = False
    elif exists_num == 1:
        exists = True
    return exists


def table_empty(db_engine: sqlalchemy.engine, table_name: str):
    empty_num = db_engine.execute(
        f"""
      SELECT EXISTS(SELECT 1 FROM {table_name})
   """
    ).scalar()
    if empty_num == 0:
        empty = False
    elif empty_num == 1:
        empty = True
    return empty


def table_exists_notempty(
    db_engine: sqlalchemy.engine, schema_name: str, table_name: str
):
    exists = table_exists(db_engine, schema_name, table_name)
    if exists:
        empty = table_empty(db_engine, f"{schema_name}.{table_name}")
        if empty:
            both = True
        else:
            both = False
    else:
        both = False
    return both


def table_index_exists(
    db_engine: sqlalchemy.engine, schema: str, table: str, index_name: str = None
):
    sql_query = f"""
    SELECT COUNT(1) as IndexIsThere FROM INFORMATION_SCHEMA.STATISTICS
    WHERE table_schema='{schema}' AND table_name='{table}'
    """
    if index_name:
        sql_query = sql_query + f" AND index_name='{index_name}'"

    index_exists_num = db_engine.execute(sql_query).scalar()
    if index_exists_num == 0:
        index_exists = False
    elif index_exists_num > 0:
        index_exists = True
    else:
        index_exists = False
    return index_exists


def table_col_names(db_engine: sqlalchemy.engine, schema_name: str, table_name: str):
    column_names = []
    with db_engine.connect() as con:
        rows = con.execute(
            f"select column_name from information_schema.columns where table_schema = '{schema_name}' and table_name='{table_name}'"
        )
        column_names = [row[0] for row in rows]
    return column_names


"""
ALTER TABLE table
ADD [COLUMN] column_name_1 column_1_definition [FIRST|AFTER existing_column],
ADD [COLUMN] column_name_2 column_2_definition [FIRST|AFTER existing_column],
...;"""


def add_columns_to_table(
    db_engine: sqlalchemy.engine, table_name: str, col_datatype_dct: dict
):
    add_col_definition_str = ", ".join(
        [f"ADD {k} {v}" for k, v in col_datatype_dct.items()]
    )

    sql_query = f"""
   ALTER TABLE {table_name}
   {add_col_definition_str}
   """

    db_engine.execute(sql_query)


def get_dtype_trans_mysql(df: pd.DataFrame, str_len: int = 150):
    obj_vars = [colname for colname in list(df) if df[colname].dtype == "object"]
    int_vars = [colname for colname in list(df) if df[colname].dtype == "int64"]
    float_vars = [colname for colname in list(df) if df[colname].dtype == "float64"]
    datetime_vars = [
        colname for colname in list(df) if df[colname].dtype == "datetime64[ns]"
    ]

    dtype_trans = {obj_var: f"VARCHAR({str_len})" for obj_var in obj_vars}
    dtype_trans.update({int_var: "INT" for int_var in int_vars})
    dtype_trans.update({float_var: "FLOAT(14, 5)" for float_var in float_vars})
    dtype_trans.update({datetime_var: "DATE" for datetime_var in datetime_vars})
    return dtype_trans


def df_to_sql_split(
    db_engine: sqlalchemy.engine, df: pd.DataFrame, table_name: str, chunksize: int = 50
):
    for i in range(0, len(df), chunksize):
        df_to_sql(db_engine, df.iloc[i : i + chunksize], table_name)


def df_to_sql(db_engine: sqlalchemy.engine, df: pd.DataFrame, table_name: str):
    def delete_quotation(str: str):
        return str.replace("'", "").replace('"', "")

    df = df.astype(str)
    df_values = [[delete_quotation(value) for value in values] for values in df.values]
    sql_query_start = f"INSERT INTO {table_name}"
    column_str = ",".join(list(df))
    values_str = ",".join([f"""('{"','".join(values)}')""" for values in df_values])
    values_str = utils.multiple_replace({"'nan'": "NULL", "'<NA>'": "NULL"}, values_str)

    sql_query = f"{sql_query_start} ({column_str}) VALUES {values_str}"
    db_engine.execute(sql_query)


def create_table(
    db_engine: sqlalchemy.engine,
    table_name: str,
    col_datatype_dct: dict,
    primary_key: str = None,
    index_lst: list = None,
    foreignkey_ref_dct: dict = None,
):
    # primary_key = "id INT AUTO_INCREMENT PRIMARY KEY"
    def_strings = []
    col_definition_str = ", ".join([f"{k} {v}" for k, v in col_datatype_dct.items()])
    if primary_key:
        col_definition_str = primary_key + ", " + col_definition_str
    def_strings.append(col_definition_str)
    if foreignkey_ref_dct:
        foreign_key_strs = [
            f"FOREIGN KEY ({k}) REFERENCES {v}" for k, v in foreignkey_ref_dct.items()
        ]
        foreign_str = ", ".join(foreign_key_strs)
        def_strings.append(foreign_str)
    if index_lst:
        index_str = ", ".join([f"INDEX ({index})" for index in index_lst])
        def_strings.append(index_str)

    create_table_query = (
        f"""CREATE TABLE IF NOT EXISTS {table_name} ({','.join(def_strings)});"""
    )

    db_engine.execute(create_table_query)

