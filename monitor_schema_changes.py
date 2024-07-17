import pika
from sqlalchemy import text, create_engine
import argparse
import json
from time import sleep
import requests
import os

'''

    How this program works:
    Takes two schemas._
    Diffs those two schemas.
    Applies the relevant DDLs.

'''

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', heartbeat=1000))
channel = connection.channel()

channel.queue_declare('new_ddls')
# channel.queue_declare('new_tables')

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

print(args.primary)
print(args.readcopy)
primary_host, primary_port = args.primary.split(':')
readcopy_host, readcopy_port = args.readcopy.split(':')
engine = create_engine(f'postgresql+psycopg2://{args.primaryuser}@{args.primary}/{args.primarydb}', connect_args={"connect_timeout": 10})
pg_engine = create_engine(f'postgresql+psycopg2://{args.readuser}@{args.readcopy}/{args.readdb}', connect_args={"connect_timeout": 10})

def send_create_index(index):
    b = dict(index)
    b['ddl_type'] = 'create_index'
    channel.basic_publish(exchange='', routing_key='new_ddls', body=json.dumps(b))

        
def send_deploy_sink(table):
    b = dict(table)
    b['ddl_type'] = 'deploy_sink'
    tablename = f'{b["schemaname"]}.{b["relname"]}'
    resp = requests.get(f"http://{args.cdchost}:8083/connectors?expand=status")
    stat = resp.json()
    # print(stat)
    if stat.get(f'sink_{tablename}') is None:
        channel.basic_publish(exchange='', routing_key='new_ddls', body=json.dumps(b))
    else:
        if stat[f'sink_{tablename}']['status']['connector']['state'] != "RUNNING":
            print("Connector there but not running")

def tableExists(index):
    with pg_engine.connect() as conn:
        res = conn.execute(text(f'''
            SELECT oid from pg_class where relname = '{index['relname']}'
        '''))
        tbls = res.fetchall()
        print("Table exists", index['relname'], len(tbls))
        return len(tbls) > 0
    

def indexExists(index):
    # pg_engine = create_engine(f'postgresql+psycopg2://{args.readuser}@{args.readcopy}/{args.readdb}')
    with pg_engine.connect() as conn:
        res = conn.execute(text(f'''
            SELECT oid from pg_class where relname = '{index['indexname']}'
        '''))
        indcs = res.fetchall()
        print("Index exists", index['indexname'], len(indcs))
        return len(indcs) > 0
            
    
def check_for_new_ddls():
    with engine.connect() as conn:
        # res = conn.execute(text(
        # '''
        #     select T.indrelid relid, T.indexrelid index_id, schemaname, pgsut.relname relname, array_agg(attname) atts, T.indnkeyatts, T.indisunique, T.indisprimary, pgc.relname indexrelname 
        #     from 
        #     (
        #         select unnest(indkey[0:indnatts]) indattid, indexrelid, indrelid, indnkeyatts, indisunique, indisprimary  
        #         from pg_index 
        #     ) T 
        #     JOIN 
        #     pg_attribute 
        #     on 
        #     (T.indattid = pg_attribute.attnum and T.indrelid = pg_attribute.attrelid)
        #     JOIN pg_stat_user_tables pgsut
        #     on 
        #     T.indrelid = pgsut.relid  
        #     JOIN pg_class pgc 
        #     on 
        #     pgc.oid = T.indexrelid 
        #     group by 
        #     T.indrelid, T.indexrelid, schemaname, pgc.relname, pgsut.relname, indisprimary, indisunique, indnkeyatts
        #     order by 
        #     indisprimary DESC
        # '''))
        res = conn.execute(text(
        '''
            SELECT pgnsp.nspname schemaname,
       pgc0.relname indexname,
       pgi.indisprimary,
       pg_indexes.indexdef,
       pgc.relname
FROM pg_indexes
INNER JOIN pg_class pgc0 ON pgc0.relname=pg_indexes.indexname
INNER JOIN pg_index pgi ON pgc0.oid = pgi.indexrelid
RIGHT OUTER JOIN pg_class pgc ON pgi.indrelid = pgc.oid
INNER JOIN pg_namespace pgnsp ON pgnsp.oid = pgc.relnamespace
WHERE pgc.relname in
    (SELECT relname
     FROM pg_stat_user_tables) ;
        '''))
        indices = res.fetchall()

        # res = conn.execute(text("SELECT relname from pg_stat_user_tables"))
        # tables = res.fetchall()
        
        for index in indices:
            # Since we have ordered by indisprimary,  first all primary key indexes will come followed by other indexes
            doesTableIndexExists = tableExists(index)

            if not doesTableIndexExists:
                send_deploy_sink(index)

            if index.indexname is not None:
                if doesTableIndexExists and not indexExists(index):
                    print("Index does not exist")
                    send_create_index(index)
                
            
                
                # pass
                
            

            
            
            

def main():
    print("Listening for schema changes in Primary Cluster")
    while True:
        try:
            check_for_new_ddls()
            sleep(5)
        except Exception as e:
            print(e)

if __name__ == '__main__':
    main()


# channel.basic_publish(exchange='', routing_key='new_ddls', body=json.dumps())

