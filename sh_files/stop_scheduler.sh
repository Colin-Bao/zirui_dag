#!/bin/bash

# source /home/lianghua/anaconda3/etc/profile.d/conda.sh
# source activate
# conda activate zirui_env;
kill $(cat /home/lianghua/rtt/soft/airflow/airflow-scheduler.pid);
ps -ef|egrep 'scheduler'|grep -v grep|awk '{print $2}'|xargs kill -9;

# conda deactivate;
# kill $(cat /home/lianghua/rtt/soft/airflow/airflow-scheduler.pid)
# ps -ef|egrep 'scheduler'|grep -v grep|awk '{print $2}'|xargs kill -9