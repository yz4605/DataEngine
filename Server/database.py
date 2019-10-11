import psycopg2
import configparser

conn = psycopg2.connect("dbname=app user=postgres")
cur = conn.cursor()

# buffer = "CREATE TABLE buffer (id SERIAL PRIMARY KEY, msg text);"
# cur.execute(buffer)
# conn.commit()

config = configparser.ConfigParser()
config.read('config.ini')
sectors = config['sector_stock']
for i in sectors:
    query = "CREATE TABLE %s (time TIMESTAMP NOT NULL, percent DOUBLE PRECISION NOT NULL );" %i
    cur.execute(query)
    query = "SELECT create_hypertable('%s','time');" %i
    cur.execute(query)
    f = open(i+'.csv')
    query = "COPY %s(time,percent) FROM STDIN DELIMITER ',' CSV HEADER;" %i
    cur.copy_expert(query,f)
    conn.commit()


for i in sectors:
    query = "CREATE TABLE %s_top (time TIMESTAMP NOT NULL,percent DOUBLE PRECISION NOT NULL,top TEXT NOT NULL);" %i
    cur.execute(query)
    query = "SELECT create_hypertable('%s_top','time');" %i
    cur.execute(query)
    f = open(i+'.csv')
    query = "COPY %s_top(time,percent,top) FROM STDIN DELIMITER ',' CSV HEADER;" %i
    cur.copy_expert(query,f)
    conn.commit()

