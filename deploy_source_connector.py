from sqlalchemy import create_engine, text
from time import sleep
import requests
import json
import argparse
import os

'''
    Purpose of this code:
    ---------------------

    There are two purposes:
    - To deploy the source connector
    This code is defined for a particular Yb primary cluster, postgres read copy, CDC connector host.
    
'''


parser = argparse.ArgumentParser()
parser.add_argument('-p', '--primary', default=os.environ['PRIMARY'])
parser.add_argument('-d', '--primarydb', default='tpch_cdc')
parser.add_argument('-u', '--primaryuser', default=os.environ['PRIMARY_USER'])

parser.add_argument('-m', '--masters', default=os.environ['MASTERS'])
parser.add_argument('-c', '--cdchost', default='localhost')
parser.add_argument('streamid', default=os.environ['STREAM_ID'])

args = parser.parse_args()

# current count of how many analyzes performed - if incremented from current value then we can trigger copy of stats
# curr_ancount = 0
# curr_aucount = 0

primary_host, primary_port = args.primary.split(':')
# readcopy_host, readcopy_port = args.readcopy.split(':')


def deploy_source_connector():

    
    print("DEPLOYING SOURCE")
    source_connect = {
       "name": f"{args.primarydb}_source",
       "config": {
        "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBConnector",
        "tasks.max": "3",
        "database.server.name": args.primarydb,
        "database.hostname": primary_host,
        "database.port": primary_port,
        "database.user": args.primaryuser,
        "database.password": "Yugabyte@123",
        "database.dbname": args.primarydb,
        "database.master.addresses": args.masters,
        "decimal.handling.mode": "double", 
        "database.streamid": args.streamid,
        "snapshot.mode": "always",
        "table.include.list": f".*",
        "value.serializer": "io.confluent.kafka.serializers.KafkaJsonSerializer"
       }
    }
    
    print("Deploying... ")
    # First check for whether connector exists already and it is running
    resp = requests.get(f"http://{args.cdchost}:8083/connectors?expand=status")
    resp.raise_for_status()
    stat = resp.json()
    print(stat)
    if stat.get(f'{args.primarydb}_source') is not None:
        if stat[f'{args.primarydb}_source']['status']['connector']['state'] == "RUNNING":
            print("Deleting existing connector... ")
            r = requests.delete(f"http://{args.cdchost}:8083/connectors/{args.primarydb}_source")
            r.raise_for_status()
        
        resp = requests.post(f"http://{args.cdchost}:8083/connectors", json=source_connect, headers={"Accept": "application/json"})
        resp.raise_for_status()
        s = resp.json()
        print("Sairam", s)
    else:
        resp = requests.post(f"http://{args.cdchost}:8083/connectors", json=source_connect, headers={"Accept": "application/json"})
        resp.raise_for_status()
        s = resp.json()
        print("Sairam", s)

    #resp.raise_for_status()



def main():
    while True:
        print("Attempting to deploy CDC source connector for primary db as {args.primarydb}....")
        try:
            deploy_source_connector()
            print("Deployed connector successfully")
            break
        except requests.exceptions.HTTPError as e:
            print(e)






if __name__ == '__main__':
    main()
    