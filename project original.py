import numpy as np
import pandas as pd

from db_conn import *

movie_table = "final_project.movie"
genre_table = "final_project.genre"
director_table = "final_project.director"
movie_director_table = "final_project.movie_director"

insert_movie_sql = f"""insert into {movie_table} (title, eng_title, year, country, m_type, status, company)
                    values(%s,%s,%s,%s,%s,%s,%s);"""
insert_genre_sql = f"""insert into {genre_table} (movie_id, genre) values(%s,%s);"""
insert_director_sql = f"""insert into {director_table} (name) values(%s);"""
insert_movie_director_sql = f"""insert into {movie_director_table} (movie_id, director_id) values(%s,%s);"""
select_director_id_sql = f"""select id from {director_table} where name=%s;"""


def process_movie_row(row, cur):
    title = row[0]
    eng_title = row[1]
    year = row[2]
    country = row[3]
    m_type = row[4]
    genre = row[5]
    status = row[6]
    directors = row[7]
    company = row[8]

    try:
        cur.execute(insert_movie_sql, (title, eng_title, year, country, m_type, status, company))
        movie_id = cur.lastrowid

        cur.execute(insert_genre_sql, (movie_id, genre))

        if directors is None:
            return

        director_ids = []
        for director in directors.split(","):
            cur.execute(select_director_id_sql, director.strip())
            director_id = cur.fetchone()
            if director_id:
                director_ids.append(director_id['id'])
            else:
                cur.execute(insert_director_sql, director.strip())
                director_id = cur.lastrowid
                director_ids.append(director_id)

        for director_id in director_ids:
            cur.execute(insert_movie_director_sql, (movie_id, director_id))

    except Exception as e:
        pass
        print(e)
        print(row)


def read_excel_into_mysql():
    excel_file = "movie_list.xls"

    conn, cur = open_db()

    df1 = pd.read_excel(excel_file, sheet_name="영화정보 리스트", skiprows=4)
    df2 = pd.read_excel(excel_file, sheet_name="영화정보 리스트_2", header=None)

    create_sql = f"""
        drop table if exists {movie_director_table};
        drop table if exists {director_table};
        drop table if exists {genre_table};
        drop table if exists {movie_table} ;

        create table {movie_table} (
            id int auto_increment primary key,
            title varchar(500),
            eng_title varchar(500),
            year int,
            country varchar(100),
            m_type varchar(10),
            status varchar(30),
            company varchar(200)
        ); 
        
        create table {genre_table} (
            movie_id int,
            genre varchar(100)
        );
        
        create table {director_table} (
            id int auto_increment primary key,
            name varchar(250)
        );
        
        create table {movie_director_table} (
            movie_id int,
            director_id int
        );
        
        create index idx_movie_id on {genre_table}(movie_id);
        create index idx_year on {movie_table}(year);
        create fulltext index idx_title on {movie_table}(title);
        create fulltext index idx_name on {director_table}(name);
        """

    cur.execute(create_sql)
    conn.commit()

    df1 = df1.replace({np.nan: None})
    df2 = df2.replace({np.nan: None})

    for i, r in df1.iterrows():
        process_movie_row(tuple(r), cur)
        if (i + 1) % 1000 == 0:
            print(f"{i} rows")

    for i, r in df2.iterrows():
        process_movie_row(tuple(r), cur)
        if (i + 1) % 1000 == 0:
            print(f"{i} rows")

    conn.commit()

    close_db(conn, cur)


if __name__ == '__main__':
    read_excel_into_mysql()
