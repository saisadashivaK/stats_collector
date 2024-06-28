from sqlalchemy import text, create_engine
import requests
import json
import pika
import argparse
import time
import os

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost', heartbeat=1000))
channel = connection.channel()



channel.queue_declare('new_stats', durable=True)

parser = argparse.ArgumentParser()
parser.add_argument('-p', '--primary', default=os.environ['PRIMARY'])
parser.add_argument('-d', '--primarydb', default='yugabyte')
parser.add_argument('-u', '--primaryuser', default=os.environ['PRIMARY_USER'])
parser.add_argument('-r', '--readcopy', default=os.environ['READCOPY'])
parser.add_argument('-D', '--readdb', default='postgres')
parser.add_argument('-U', '--readuser', default=os.environ['READ_USER'])
parser.add_argument('-c', '--cdchost', default='localhost')

args = parser.parse_args()


primary_host, primary_port = args.primary.split(':')
readcopy_host, readcopy_port = args.readcopy.split(':')
pg_engine = create_engine(f'postgresql+psycopg2://{args.readuser}@{args.readcopy}/{args.readdb}')

analyze_counts = {}

def get_saved_analyze_counts():
    global analyze_counts
    if os.path.exists('./current_analyze_counts.json'):
        with open('current_analyze_counts.json') as f:
            analyze_counts = json.load(f)
        
    else:
        with open('current_analyze_counts.json', 'w') as f:
            x = {}
            json.dump(x, f, indent=4)
    
    

def save_analyze_counts():
    with open("current_analyze_counts.json", "w") as f:
        json.dump(analyze_counts, f, indent=4)




def initialize_analyze_counts():
    global analyze_counts
    get_saved_analyze_counts()
    with pg_engine.connect() as conn:
        res = conn.execute(text("SELECT relname, autoanalyze_count, analyze_count, last_analyze, last_autoanalyze FROM pg_stat_user_tables"))
        counts = res.fetchall()
        for count in counts:
            # print(count)
            if analyze_counts.get(count['relname']) is None: # if new relation added and not present in existing one
                analyze_counts[count['relname']] = {
                    'analyze_count': 0, 
                    'autoanalyze_count': 0
                }
            else:
                if count['last_analyze'] is None:
                    analyze_counts[count['relname']]['analyze_count'] = count['analyze_count']
                if count['last_autoanalyze'] is None:
                    analyze_counts[count['relname']]['autoanalyze_count'] = count['autoanalyze_count']
                
        save_analyze_counts()

        



def check_for_new_stats():
    global current_analyze_count, current_autoanalyze_count
    with pg_engine.connect() as conn:
        res = conn.execute(text(f"SELECT relid, schemaname, relname, analyze_count, autoanalyze_count from pg_stat_user_tables"))
        an_counts = res.fetchall()

        changed = False

        for an_count in an_counts:
            if analyze_counts.get(an_count['relname']) is None:
                analyze_counts[an_count['relname']] = {
                    'analyze_count': 0, 
                    'autoanalyze_count': 0
                }

            if an_count['analyze_count'] > analyze_counts[an_count['relname']]['analyze_count'] or an_count['autoanalyze_count'] > analyze_counts[an_count['relname']]['autoanalyze_count']:
                content = {
                    'tablename': an_count['relname'], 
                    'schemaname': an_count['schemaname'], 
                    'relid': an_count['relid']
                }
                changed = True
                analyze_counts[an_count['relname']] = {
                    'analyze_count': an_count['analyze_count'], 
                    'autoanalyze_count': an_count['autoanalyze_count']
                }
                channel.basic_publish(exchange='', routing_key='new_stats', body=json.dumps(content))

        if changed:
            print("Stats changed")
            save_analyze_counts()


def main():
    print("Checking for changes in statistics... ")
    while True:
        initialize_analyze_counts()
        check_for_new_stats()
        time.sleep(5)

if __name__ == '__main__':
    main()
