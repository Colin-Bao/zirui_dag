#!/bin/bash


# ls -l /bin/sh
# source /home/lianghua/anaconda3/etc/profile.d/conda.sh
# conda activate zirui_env
conda activate zirui_env
nohup airflow scheduler > /home/lianghua/rtt/soft/airflow/dags/zirui_dag/log/scheduler_out.log 2>&1 &


