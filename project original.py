import numpy as np
import pandas as pd

from db_conn import *


def read_excel_into_mysql():
    excel_file = "movie_list.xls"

    conn, cur = open_db()

    df = pd.read_excel(excel_file, skiprows=4,
                       converters={'영화명': str, '영화명(영문)': str, '제작연도': int,
                                   '제작국가': str, '유형': str, '장르': str,
                                   '제작상태': str, '감독': str, '제작사': str})

    print(df.head())

    movie_table = "final_project.movie"

    create_sql = f"""
        drop table if exists {movie_table} ;

        create table {movie_table} (
            id int auto_increment primary key,
            title varchar(500),
            eng_title varchar(500),
            year int,
            country varchar(100),
            m_type varchar(10),
            genre varchar(100),
            status varchar(30),
            director varchar(250),
            company varchar(100),
            enter_date datetime default now()
        ); """

    cur.execute(create_sql)
    conn.commit()

    insert_sql = f"""insert into {movie_table} (title, eng_title, year, country, m_type, genre, status, director, company)
                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s);"""

    df = df.replace({np.nan: None})

    for i, r in df.iterrows():
        row = tuple(r)

        try:
            cur.execute(insert_sql, row)
            if (i + 1) % 1000 == 0:
                print(f"{i} rows")
        except Exception as e:
            pass
            print(e)
            print(row)

    conn.commit()

    close_db(conn, cur)


if __name__ == '__main__':
    read_excel_into_mysql()
