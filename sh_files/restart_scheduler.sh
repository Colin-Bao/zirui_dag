#!/bin/bash

source /home/lianghua/anaconda3/etc/profile.d/conda.sh
conda activate zirui_env
echo "conda activate"

# ps -ef|egrep 'scheduler'|grep -v grep|awk '{print $2}'|xargs kill -9
# kill $(cat /home/lianghua/rtt/soft/airflow/airflow-scheduler.pid)
# echo "scheduler stop "



nohup airflow scheduler > /home/lianghua/rtt/soft/airflow/dags/zirui_dag/log/scheduler_out.log 2>/home/lianghua/rtt/soft/airflow/dags/zirui_dag/log/scheduler_out.err &
