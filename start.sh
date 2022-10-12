# 启动脚本start_airflow.sh
#!/bin/bash
# P@ssw0rd
nohup airflow webserver --port 8081 > webserverout.log 2>&1 &
nohup airflow scheduler > schedulerout.log 2>&1 &
nohup python check_airflow.py > check_airflow.log 2>&1 &
echo "Start success"

