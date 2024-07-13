import requests
import json
import pika
import argparse
from sqlalchemy import create_engine, text
import subprocess
import re
import os

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', heartbeat=15))
channel = connection.channel()

channel.queue_declare('new_ddls')



parser = argparse.ArgumentParser()
parser.add_argument('-p', '--primary', default=os.environ['PRIMARY'])
parser.add_argument('-d', '--primarydb', default='latency_testing')
parser.add_argument('-u', '--primaryuser', default=os.environ['PRIMARY_USER'])
parser.add_argument('-r', '--readcopy', default=os.environ['READCOPY'])
parser.add_argument('-D', '--readdb', default='postgres')
parser.add_argument('-U', '--readuser', default=os.environ['READ_USER'])
parser.add_argument('-c', '--cdchost', default='localhost')
# parser.add_argument('streamid')

args = parser.parse_args()
primary_host, primary_port = args.primary.split(':')
readcopy_host, readcopy_port = args.readcopy.split(':')
readcopy_engine = create_engine(f"postgresql+psycopg2://{args.readuser}@{args.readcopy}/{args.readdb}", echo=True)
# def create_index(req, ch, method):
#     print("METHOD", method)
#     readcopy_engine = create_engine(f"postgresql+psycopg2://{args.readuser}@{args.readcopy}/{args.readdb}", echo=True)
#     unique = ''
#     if req['indisunique']:
#         unique = 'UNIQUE'
    
#     index_name = req['indexrelname']
#     tablename = f'{req["schemaname"]}.{req["relname"]}'
    
#     index_method = 'btree'
#     colnames = ','.join([att for att in req['atts'][:req['indnkeyatts']]])
#     include_cols = ','.join([att for att in req['atts'][req['indnkeyatts']: ]])
#     print("Sairam", req)
#     with readcopy_engine.connect() as conn:
#        with conn.begin() as tn:
#         if len(include_cols) > 0:
#             stmt = f"CREATE  {unique} INDEX IF NOT EXISTS {index_name} ON {tablename}({colnames})  INCLUDE ({include_cols})"
#         else:
#             stmt = f"CREATE  {unique} INDEX IF NOT EXISTS {index_name} ON {tablename}({colnames})"
#         conn.execute(text(stmt))
#         tn.commit()
    
#     ch.basic_ack(delivery_tag=method.delivery_tag)
    

def create_index(req, ch, method):
    print("METHOD", req)
    create_index_stmts = []
    # check if index is primary
    if not req['indisprimary']:
        create_index_stmt = req['indexref']
        create_index_stmt = create_index_stmt.strip().replace('CREATE INDEX', 'CREATE INDEX IF NOT EXISTS').replace('HASH', 'ASC').replace('lsm', 'btree')
        create_index_stmts.append(create_index_stmt)
    else:
        # For primary key just generate the statement using pg_dump(for reliable results). However, is time consuming.
        out = subprocess.run(f'pg_dump -h {primary_host} -p {primary_port} -U {args.primaryuser} -s -t {req["relname"]} {args.primarydb}'.split(), capture_output=True)
        out = out.stdout.decode()
        tmp = list(filter(lambda x: not x.startswith('--'), out.splitlines()))
        tmp = [l.strip() for l in out.splitlines() if (not l.strip().startswith('--') and len(l.strip()) > 0)]
        tmp2 = ' '.join(tmp)
        stmts = tmp2.split(';')
        # print(tmp)
        
        for stmt in stmts:
            match = re.search(r'(ALTER TABLE ONLY .*? ADD CONSTRAINT .*? PRIMARY KEY .*?)$', stmt.strip())
            # print(stmt)
            if  match is not None:
                create_index_stmts.append(match[0])
            else:
                # print("Could not find")
                pass
        

          
    
    print(create_index_stmts)

    

    # print("Sairam", req)
    with readcopy_engine.connect() as conn:
        for stmt in create_index_stmts:
            try:
                with conn.begin() as tn:
                    conn.execute(text(stmt))
                    # tn.commit()
            except Exception as e:
                print(f"Failed to create index: {stmt}", e)
    
    
    ch.basic_ack(delivery_tag=method.delivery_tag)
     


# def create_table(req):
#     readcopy_engine = create_engine(f"postgresql+psycopg2://{args.primaryuser}@{args.readcopy}/{args.readdb}")
#     unique = ''
#     if req['indisunique']:
#         unique = 'UNIQUE'
    
#     index_name = req['indexrelname']
#     tablename = f'{req["schemaname"]}.{req["relname"]}'
    
#     method = 'btree'
#     pkey_colnames = ','.join([att for att in req['atts'][:req['indnkeyatts']]])
   

#     with readcopy_engine.connect() as conn:


def tableExists(index):
    with readcopy_engine.connect() as conn:
        res = conn.execute(f'''
            SELECT oid from pg_class where relname = '{index['relname']}'
        ''')
        tbls = res.fetchall()
        print("Table exists", index['relname'], len(tbls))
        return len(tbls) > 0

def deploy_sink(req, ch, method):
    # key_list = req['atts'][:req['indnkeyatts']]
    # print("METHOD", method)
    tablename = f'{req["schemaname"]}.{req["relname"]}'
    resp = requests.get(f"http://{args.cdchost}:8083/connectors/sink_common_{args.primarydb}/config")
    config_edited = resp.json()
    topiclist = [topic.strip() for topic in config_edited['topics'].split(',')]
    print(topiclist)
    if f"{args.primarydb}.{tablename}" in topiclist:
        print("Topic", f"{args.primarydb}.{tablename}", "exists. Removing message...")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return


    # print("DEPLOYING SINK", f'{args.primarydb}.{tablename}')
    pgdstmt = f'pg_dump -h {primary_host} -p {primary_port} -U {args.primaryuser} -s -t "{req["relname"]}" {args.primarydb}'
    print(pgdstmt)
    out = subprocess.run(pgdstmt.split(), capture_output=True)
    out = out.stdout.decode()
    # out = out.stdout.decode()
    # tmp = list(filter(lambda x: not x.startswith('--'), out.splitlines()))
    tmp = [l.strip() for l in out.splitlines() if (not l.strip().startswith('--') and len(l.strip()) > 0)]
    tmp2 = ' '.join(tmp)
    stmts = tmp2.split(';')
    stmts = [stmt for stmt in stmts if not re.search(r'.*?FOREIGN KEY.*?', stmt)]
    sqlstatements = ';'.join(stmts)
    print("SQL statements", ';'.join(stmts))
    sqlstatements = sqlstatements.replace('lsm', 'btree').replace('HASH', 'ASC')
    sqlstatements = re.sub(r'ALTER (.*?) OWNER TO (.*?);',rf"ALTER \1 OWNER TO SESSION_USER;", sqlstatements)

    with readcopy_engine.connect() as conn:
        with conn.begin() as tn:
            conn.execute(text(sqlstatements))

    resp = requests.get(f"http://{args.cdchost}:8083/connectors/sink_common_{args.primarydb}/config")
    config_edited = resp.json()
    print(config_edited)
    config_edited['topics'] = f"{config_edited['topics']},{args.primarydb}.{tablename}"
    
    print("Deploying...", config_edited)
    resp = requests.put(f"http://{args.cdchost}:8083/connectors/sink_common_{args.primarydb}/config", json=config_edited, headers={"Accept": "application/json"})
    s = resp.json()
    print("Sairam", s)
    ## Need to add a condition to check for http request wrong which needs the message to be unacknowledged

    ch.basic_ack(delivery_tag=method.delivery_tag)

    
    # resp = requests.post(f"http://{args.cdchost}:8083/connectors", json=sink_connect, headers={"Accept": "application/json"})
    # #resp.raise_for_status()
    # a = resp.json()
    # print(json.dumps(a, indent=4))
    


def on_new_ddl_callback(ch, method, properties, body):
    # pass
    reqs = body.decode()
    
    req = json.loads(reqs)
    if req['ddl_type'] == 'create_index':
        create_index(req, ch, method)
        
    elif req['ddl_type'] == 'deploy_sink':
        deploy_sink(req, ch, method)


channel.basic_consume(queue='new_ddls', on_message_callback=on_new_ddl_callback)

print("Waiting for messages... ")

channel.start_consuming()
