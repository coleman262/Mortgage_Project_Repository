import psycopg2
import pandas as pd


#AWS Pycharm Postgres Connection Query for ETL

hostname = 'fnmaloanhist.ccsgwcueebaq.us-east-2.rds.amazonaws.com'
database = 'postgres'
username = 'XXXX'
pwd = 'XXXX'



conn = psycopg2.connect(
    host = hostname,
    database = database,
    user = username,
    password = pwd
    )

cur = conn.cursor()
conn.autocommit = True


data = pd.read_csv('C:/Users/colem/PycharmProjects/pythonProject2/pmms.csv')

def create_staging_table(cursor):
    cursor.execute("""
    DROP TABLE IF EXISTS pmms_staging CASCADE;
    CREATE UNLOGGED TABLE pmms_staging(
    period numeric,
    pmms_30yr decimal(4,2),
    pmms_15yr decimal(4,2)
    );""")

with conn.cursor() as cursor:
    create_staging_table(cursor)

def send_csv_to_psql(connection,csv,table_):
    sql = "COPY %s FROM STDIN WITH CSV HEADER DELIMITER AS ','"
    file = open(csv, "r")
    table = table_
    with connection.cursor() as cur:
        cur.execute("truncate " + table + ";")   #to eliminate duplicates
        cur.copy_expert(sql=sql % table, file=file)
        connection.commit()

    return connection.commit()

send_csv_to_psql(conn,'C:/Users/colem/PycharmProjects/pythonProject2/pmms.csv', 'pmms_staging')

sql_ = "Select count (*) from pmms_staging"
cur.execute(sql_)
cur.fetchall()

def data_transform_load(cursor):
    cursor.execute("""
    INSERT INTO econo_data (period, pmms_30yr, pmms_15yr)
   
    (Select
    period,
    pmms_30yr/100 as pmms_30yr,
    pmms_15yr/100 as pmms_15yr
    from pmms_staging);""")

with conn.cursor() as cursor:
    data_transform_load(cursor)


def drop_staging_table(cursor):
    cursor.execute("""
    DROP TABLE IF EXISTS pmms_staging CASCADE;""")

with conn.cursor() as cursor:
    drop_staging_table(cursor)


cur.close()
conn.close()



