# 启动脚本start_airflow.sh
#!/bin/bash
# P@ssw0rd
nohup airflow webserver --port 8081 > log/webserverout.log 2>&1 &
nohup airflow scheduler > log/schedulerout.log 2>&1 &
echo "Start success"

