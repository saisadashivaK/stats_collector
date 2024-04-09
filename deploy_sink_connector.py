import requests
import argparse
from sqlalchemy import text, create_engine
import subprocess
import re

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--primary', default='10.110.11.24:5433')
parser.add_argument('-d', '--primarydb', default='dryrun1')
parser.add_argument('-r', '--readcopy', default='10.110.21.59:5434')
parser.add_argument('-D', '--readdb', default='dryrun1')
parser.add_argument('-u', '--primaryuser', default='yugabyte')
parser.add_argument('-U', '--readuser', default='yb-testing-2')

parser.add_argument('-c', '--cdchost', default='localhost')
args = parser.parse_args()
primary_host, primary_port = args.primary.split(':')
readcopy_host, readcopy_port = args.readcopy.split(':')
yb_engine = create_engine(f"postgresql+psycopg2://{args.primaryuser}@{args.primary}/{args.primarydb}")
pg_engine = create_engine(f'postgresql+psycopg2://{args.readuser}@{args.readcopy}/{args.readdb}', echo=True)

def deploy_sink_connector():
    tables = []
    with yb_engine.connect() as conn:
        res = conn.execute("SELECT relname FROM pg_stat_user_tables")
        tabs = res.fetchall()
        for t in tabs:
            tables.append(f"{args.primarydb}.public.{t['relname']}")
    
    out = subprocess.run(f'pg_dump -h {primary_host} -p {primary_port} -U {args.primaryuser} -s {args.primarydb}'.split(), capture_output=True)
    out = out.stdout.decode()
    sqlstatements = out.replace('lsm', 'btree').replace('HASH', 'ASC')
    sqlstatements = re.sub(r'ALTER (.*?) OWNER TO (.*?);',rf"ALTER \1 OWNER TO SESSION_USER;", sqlstatements)
    with pg_engine.connect() as conn:
        with conn.begin() as tn:
            conn.execute(text(sqlstatements))
            # tn.commit()

    
    topiclist = ','.join(tables)

    sink_connect = {
              "name": f"sink_common_{args.primarydb}",
              "config": {
                    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
                    "transforms": "unwrap",
                    "tasks.max": "3",
                    "topics": topiclist, 
                    "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",
                    "transforms.unwrap.drop.tombstones": "false",
                    "connection.url": f"jdbc:postgresql://{args.readcopy}/{args.readdb}?user={args.readuser}",
                    "connection.user": args.readuser,
                    "connection.password": "Sairam@123",
                    "dialect.name": "PostgreSqlDatabaseDialect",
                    "insert.mode": "upsert",
                    "delete.enabled": "true",
                    "table.name.format": "${topic}",
                    "pk.mode": "record_key",
                    "auto.create": "false",
                    "auto.evolve": "true",
                    "value.deserializer": "io.confluent.kafka.serializers.KafkaJsonDeserializer"
              }
        }

    
   
    print("Deploying...")
    resp = requests.post(f"http://{args.cdchost}:8083/connectors", json=sink_connect, headers={"Accept": "application/json"})
    s = resp.json()
    print("Sairam", s)

def main():

    deploy_sink_connector()

if __name__ == '__main__':
    main()