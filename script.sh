#!/usr/bin/bash

#python -u monitor_stats_changes.py -d $1 -D $2 > monitor_stats_changes.out 2>&1 &
#python -u monitor_schema_changes.py -d $1 -D $2 > monitor_schema_changes.out 2>&1 &
#python -u deploy_ddl_changes.py -d $1 -D $2 > deploy_ddl_changes.out 2>&1 &
#python -u update_yb_stats.py -d $1 -D $2 > update_yb_stats.out 2>&1 &
#python -u deploy_sink_connector.py -d $1 -D $2 > deploy_sink_connector.out 2>&1 &
#python -u deploy_source_connector.py -d $1 > deploy_source_connector.out 2>&1 &



python -u monitor_stats_changes.py  > monitor_stats_changes.out 2>&1 &
python -u monitor_schema_changes.py > monitor_schema_changes.out 2>&1 &
python -u deploy_ddl_changes.py  > deploy_ddl_changes.out 2>&1 &
python -u update_yb_stats.py  > update_yb_stats.out 2>&1 &
#python -u deploy_sink_connector.py > deploy_sink_connector.out 2>&1 &
#python -u deploy_source_connector.py > deploy_source_connector.out 2>&1 &
