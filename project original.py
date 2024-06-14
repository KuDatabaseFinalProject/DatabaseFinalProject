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

select_last_movie_id_sql = f"""select max(id) as id from {movie_table};"""
select_last_director_id_sql = f"""select max(id) as id from {director_table};"""


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
            foreign key (movie_id) references {movie_table}(id),
            genre varchar(100)
        );
        
        create table {director_table} (
            id int auto_increment primary key,
            name varchar(250)
        );
        
        create table {movie_director_table} (
            movie_id int,
            director_id int,
            foreign key (movie_id) references {movie_table}(id),
            foreign key (director_id) references {director_table}(id)
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
    dfs = [df1, df2]

    movie_rows = []
    director_rows = []
    genre_rows = []
    for df in dfs:
        for movie_id, r in df.iterrows():
            row = tuple(r)
            title = row[0]
            eng_title = row[1]
            year = row[2]
            country = row[3]
            m_type = row[4]
            genre = row[5]
            status = row[6]
            directors = row[7]
            company = row[8]

            movie_rows.append(
                (title, eng_title, year, country, m_type, status, company))
            director_rows.append(directors)
            genre_rows.append(genre)

    cur.executemany(insert_movie_sql, movie_rows)
    print("inserted movies")

    cur.execute(select_last_movie_id_sql)
    last_movie_id = cur.fetchone()["id"]
    first_movie_id = last_movie_id - len(movie_rows) + 1

    for movie_id in range(first_movie_id, last_movie_id + 1):
        row_index = movie_id - first_movie_id
        genre_rows[row_index] = (movie_id, genre_rows[row_index])

    genre_rows = [x for x in genre_rows if x is not None]
    cur.executemany(insert_genre_sql, genre_rows)
    print("inserted genres")

    unique_directors = set()
    for directors in director_rows:
        if directors is None:
            continue
        for director in directors.split(","):
            unique_directors.add(director.strip())

    unique_directors = list(unique_directors)
    cur.executemany(insert_director_sql, unique_directors)
    conn.commit()
    print("inserted directors")

    cur.execute(select_last_director_id_sql)
    last_director_id = cur.fetchone()["id"]
    first_director_id = last_director_id - len(unique_directors) + 1

    director_id_map = {}
    for director_id in range(first_director_id, last_director_id + 1):
        row_index = director_id - first_director_id
        director_id_map[unique_directors[row_index]] = director_id

    movie_director_rows = []
    for movie_id in range(first_movie_id, last_movie_id + 1):
        row_index = movie_id - first_movie_id
        if director_rows[row_index] is None:
            continue
        for director in director_rows[row_index].split(","):
            movie_director_rows.append(
                (movie_id, director_id_map[director.strip()]))
    cur.executemany(insert_movie_director_sql, movie_director_rows)
    print("inserted movie_directors")

    conn.commit()
    close_db(conn, cur)


if __name__ == '__main__':
    read_excel_into_mysql()
