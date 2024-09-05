import requests
import json
import pandas as pd
import sqlite3 as sql

def get_data(url: str) -> dict:
    try:
        response = requests.get(url)
    except requests.exceptions.ConnectionError:
        print('Connection error, trying again...')
        response = requests.get(url)

    if response.status_code == 200:
        data_json = json.loads(response.content.decode('utf-8'))

    elif response.status_code == 404:
        raise NameError(response.reason)

    return data_json


def round_na(num:int, decimals:int = 2) -> float:
    if not pd.isnull(num):
        return round(num, decimals)
    else:
        return None


def connect_database(db_name: str='db_NationalWater') -> sql.Connection:
    try:
        return sql.connect(db_name)
    except sql.Error as error:
        print(error)


def check_update(df: pd.DataFrame, conn: sql.Connection, table_name: str, id_col: str) -> pd.DataFrame:
    """
        Returns a DataFrame that contains the values to update
        This values have to be passed to Delete and Insert
    """
    conn = connect_database()
    df_db = pd.read_sql(f'SELECT * FROM {table_name}', con=conn)
    
    conn.close()

    intersection = pd.Series(list(set(df_db[id_col]) & set(df[id_col])))

    if intersection.size > 0:
        df_db = df_db[df_db[id_col].isin(intersection)]
        df = df[df[id_col].isin(intersection)]

        cols_to_check = list(df_db.columns)
        cols_to_check.remove(id_col)
        df_db['check_update'] = df_db[cols_to_check].astype(str).values.sum(axis=1)
        df['check_update'] = df[cols_to_check].astype(str).values.sum(axis=1)

        different_rows = pd.Series(list(set(df_db.check_update) ^ set(df.check_update)))

        df = df[df['check_update'].isin(different_rows)].drop('check_update', axis=1)

        return df

    else:
        return pd.DataFrame()


def check_insert(df: pd.DataFrame, conn: sql.Connection, table_name: str, id_col: str) -> pd.DataFrame:
    """
        Returns a DataFrame that contains the values to insert
    """
    conn = connect_database()
    df_db = pd.read_sql(f'SELECT * FROM {table_name}', con=conn)
    
    conn.close()

    difference = pd.Series(list(set(df_db[id_col]) ^ set(df[id_col])))

    if difference.size > 0:
        df = df[df[id_col].isin(difference)]

        return df

    else:
        return pd.DataFrame()


def sql_delete(table_name: str, id_col: str, to_delete: pd.DataFrame) -> None:
    """
        Delete coincidences to update later
    """
    conn = connect_database()
    curs = conn.cursor()

    to_delete = to_delete[id_col]
    in_sentence = ','.join([f"'{delete}'" for delete in to_delete])
    query = f'DELETE FROM {table_name} WHERE {id_col} IN ({in_sentence})'
    query = query.replace('[', '').replace(']', '')

    curs.execute(query)
    conn.commit()
    conn.close()


def sql_upsert(table_name: str, id_col: str, to_delete: pd.DataFrame=pd.DataFrame()) -> None:
    if to_delete.size > 0:
        sql_delete(table_name=table_name, id_col=id_col, to_delete=to_delete)


def check_table_exist(table_name: str, conn: sql.Connection):
    """
        Return False if table doesn't exists
    """
    conn = connect_database()
    query = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'"

    df = pd.read_sql(query, con=conn)

    if df.size == 0:
        return False
    else:
        return True


def sql_save(table_name: str, id_col: str, df: pd.DataFrame=pd.DataFrame()) -> bool:
    """
        Save table to DataBase
    """

    if df.size <= 0:
        raise ValueError('Need data to insert')
    
    conn = connect_database()

    table_exists = check_table_exist(table_name=table_name, conn=conn)

    saved_data = True
    if table_exists:
        to_update = check_update(df, conn=conn, table_name=table_name, id_col=id_col)
        to_insert = check_insert(df, conn=conn, table_name=table_name, id_col=id_col)

        if to_update.size > 0:
            sql_delete(table_name=table_name, id_col=id_col, to_delete=to_update)
            to_update.to_sql(table_name, conn, index=False, if_exists='append')
            to_insert.to_sql(table_name, conn, index=False, if_exists='append')
        elif to_insert.size > 0:
            to_insert.to_sql(table_name, conn, index=False, if_exists='append')
        else:
            saved_data = False
    else:
        df.to_sql(table_name, conn, index=False, if_exists='append')

    conn.close()
    return saved_data
