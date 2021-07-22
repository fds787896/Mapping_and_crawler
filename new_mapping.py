import pandas as pd
import datetime as dt
from sqlalchemy import create_engine
import json


def db_info():
    with open("config.json", mode="r") as file:
        info = json.load(file)
    return info


def connection(user, pwd, host, port, db):
    engine = create_engine(
        'mssql+pyodbc://{user}:{pwd}@{host}:{port}/{db}?driver=SQL+Server'.format(user=user, pwd=pwd, host=host,
                                                                                  port=port, db=db))
    con = engine.raw_connection()
    cursor = con.cursor()
    return engine, con, cursor


def month_lst():
    today = dt.datetime.today()
    first = today.replace(day=1)
    previous_month = first - dt.timedelta(days=1)
    month = first.strftime("%Y-%m")
    lastmonth = previous_month.strftime("%Y-%m")
    return [month, lastmonth]


class sql_mapping:
    def __init__(self):
        self.dir_tb_lst = ["employee_mapping.xlsx", "三方與銀行名稱.xlsx", "划分號_new.xlsx", "會科Comparison_1.xlsx"]
        self.sql_tb_lst = ["employee_mapping", "third_mapping", "co_mapping", "subject_mapping"]

    def truncate_table(self):
        for table in self.sql_tb_lst:
            try:
                cursor.execute("truncate table {table}".format(table=table))
                con.commit()
            except Exception as ex:
                print(ex)

    def insert_into_sql(self):
        for file, tbname in zip(self.dir_tb_lst, self.sql_tb_lst):
            try:
                df = pd.read_excel(r"Z:\09-tableau-data\mapping表\{month}\{file}".format(month=month_lst[0], file=file))
                df = df.mask(pd.isnull(df), None)
                df.to_sql(tbname, con=engine, if_exists="append", index=False, chunksize=1000)
            except Exception as ex:
                print(ex)
                mapping_fail.append(file)
            finally:
                con.close()


def main():
    global db_info, engine, con, cursor, month_lst, mapping_fail
    db_info = db_info()
    engine, con, cursor = connection(db_info["user"], db_info["password"], "localhost", 1433, "testdb")
    month_lst = month_lst()
    mapping_fail = []
    obj = sql_mapping()
    obj.truncate_table()
    obj.insert_into_sql()


if __name__ == "__main__":
    main()
