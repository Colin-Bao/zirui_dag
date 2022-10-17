#!/bin/bash

source /home/lianghua/anaconda3/etc/profile.d/conda.sh
conda activate zirui_env
nohup airflow scheduler > /home/lianghua/rtt/soft/airflow/dags/zirui_dag/log/scheduler_out.log 2>/home/lianghua/rtt/soft/airflow/dags/zirui_dag/log/scheduler_out.err &

# /mnt/h/routines/monitor/run_osalpha_server.sh
