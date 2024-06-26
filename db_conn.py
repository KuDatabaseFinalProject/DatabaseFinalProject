import pymysql


from pymysql.constants.CLIENT import MULTI_STATEMENTS

def open_db(dbname='final_project'):
    conn = pymysql.connect(host='localhost',
                           port=3333,
                           user='root',
                           passwd='1111',
                           db=dbname,
                           client_flag=MULTI_STATEMENTS)
    cur = conn.cursor(pymysql.cursors.DictCursor)

    return conn, cur

def close_db(conn, cur):
    cur.close()
    conn.close()