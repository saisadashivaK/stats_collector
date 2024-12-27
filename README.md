# Solution to copy planner statistics from PostgreSQL cluster and update them in a YugabyteDB cluster
## Functionality
1. *deploy_source_connector.py* - This program creates the YugabyteDB source connector which captures change events and stores them
   in a Kafka Queue.
2. *deploy_sink_connector.py* - This program creates the sink connector which takes the changes from the Kafka queue and applies
   them to the sink Postgres database.
3. *monitor_stats_changes.py* - This program polls the ```pg_stat_user_tables``` and ```pg_stat_user_indexes``` from the PostgreSQL centralized readcopy to see if Postgres has run auto analyze recently.
If it finds that the statistics have been updated it pushes the details to a rabbitmq queue. To extract the statistics from the full read copy, it makes use of a function dump_statistic()
 from an extension called ```dump_stat``` by Dmitri Ivanov. 
4. *update_yb_stats.py* - This program on receiving a message that the stats have changed, connect to the YugabyteDB cluster
   and update those statistics.
5. *monitor_schema_changes.py* - This program monitors changes in the schema and puts them in the queue.
6. *deploy_ddl_changes.py* - This program applies the schema changes in the postgres database.
