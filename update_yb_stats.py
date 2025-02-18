from sqlalchemy import text, create_engine
import requests
import json
import pika
import argparse
import time
import os

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', heartbeat=1000))
channel = connection.channel()



channel.queue_declare('new_stats')
config = json.load(open('config.json', 'r'))
parser = argparse.ArgumentParser()
parser.add_argument('-p', '--primary', default=os.environ['PRIMARY'])
parser.add_argument('-d', '--primarydb', default=config.get('primarydb'))
parser.add_argument('-u', '--primaryuser', default=os.environ['PRIMARY_USER'])
parser.add_argument('-r', '--readcopy', default=os.environ['READ_COPY'])
parser.add_argument('-D', '--readdb', default=config.get('readdb'))
parser.add_argument('-U', '--readuser', default=os.environ['READ_USER'])
parser.add_argument('-c', '--cdchost', default='localhost')

args = parser.parse_args()

primary1 = os.environ['PRIMARY']
#primary2, primary3 = os.environ['PRIMARY2'],os.environ['PRIMARY3']
ybengine1 = create_engine(f'postgresql+psycopg2://{args.primaryuser}@{primary1}/{args.primarydb}', echo=True)
#ybengine2 = create_engine(f'postgresql+psycopg2://{args.primaryuser}@{primary2}/{args.primarydb}', echo=True)
#ybengine3 = create_engine(f'postgresql+psycopg2://{args.primaryuser}@{primary3}/{args.primarydb}', echo=True)

pg_engine = create_engine(f'postgresql+psycopg2://{args.readuser}@{args.readcopy}/{args.readdb}')

def update_yb_stat(colstatstmts, tabstats: dict, engine):
    with engine.connect() as conn:
        with conn.begin() as tn:
            conn.execute(text('SET yb_non_ddl_txn_for_sys_tables_allowed = ON'))

            # update table stats for the relation which has new stats
            
            conn.execute(text(f'''
                UPDATE pg_class SET reltuples = {tabstats[0]['reltuples']}, relpages = {tabstats[0]['relpages']}, relallvisible = {tabstats[0]['relallvisible']} 
                WHERE relname = '{tabstats[0]['relname']}'
            '''))

            conn.execute(text(f"DELETE FROM pg_statistic WHERE starelid = '{tabstats[0]['relid']}'"))

            # update indexrowstats  for the indexes of the relation
            for indexrow in tabstats:
                conn.execute(text(f'''
                UPDATE pg_class SET reltuples = {indexrow['ireltuples']}, relpages = {indexrow['irelpages']}, relallvisible = {indexrow['irelallvisible']} 
                WHERE relname = '{indexrow['irelname']}'
                '''))
                conn.execute(text(f"DELETE FROM pg_statistic WHERE starelid = '{indexrow['irelid']}'"))

            # update column stats of indexes and relations
            for colstatstmt in colstatstmts:
                stmt = dict(colstatstmt)
                conn.execute(text(stmt['dump_statistic']))
            tn.commit()



def on_new_stats(ch, method, properties, body):

    ob = json.loads(body)

    tablename = ob['tablename']
    print(f"Change in stats for {tablename}")
    with pg_engine.connect() as conn:      
        res = conn.execute(text(f"select pgc1.oid relid, pgc1.relname relname, pgc1.relpages relpages, pgc1.reltuples reltuples, pgc1.relallvisible relallvisible, pgc2.oid irelid, pgc2.relname irelname, pgc2.relpages irelpages, pgc2.reltuples ireltuples, pgc2.relallvisible irelallvisible from pg_class pgc1, pg_index pgi, pg_class pgc2 where pgc1.oid = pgi.indrelid and pgc2.oid = pgi.indexrelid and pgc1.oid = {ob['relid']} order by relid, relname;"))
        tabstats = res.fetchall()
        tabstats = list(tabstats)
        
        colstats = []
        # get stats for each index of the table
        for r in tabstats:
            res = conn.execute(text(f"SELECT dump_statistic({r['irelid']})"))
            colstats.extend(res.fetchall())

        # get stats for the table itself
        res = conn.execute(text(f"SELECT dump_statistic({tabstats[0]['relid']})"))
        colstats.extend(res.fetchall())
        

        print(len(colstats))
        print(tablename)
        print(tabstats)
        
            
        
        update_yb_stat(colstats, tabstats, ybengine1)
#        update_yb_stat(colstats, tabstats, ybengine2)
#       update_yb_stat(colstats, tabstats, ybengine3)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        # print(tabstats)
    

        
def on_new_stats_callback(ch, method, properties, body):
    try:
        on_new_stats(ch, method, properties, body)
    except Exception as e:
        print(e)


channel.basic_consume(queue='new_stats', on_message_callback=on_new_stats_callback)

def connect_with_dbs():
    with pg_engine.connect() as conn:
        with conn.begin() as tn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS dump_stat"))
            tn.commit()


    with ybengine1.connect() as conn:
        with conn.begin() as tn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS dump_stat"))
            tn.commit()

while True:
    try:
        print('Attempting to connect to database servers... ')
        connect_with_dbs()
        break
    except Exception as e:
        print(e)

print("Waiting for messages... ")
channel.start_consuming()
