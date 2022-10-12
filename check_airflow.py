import subprocess
from time import time
import yagmail
import time
import logging

logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
                    filename='log/check_airflow.log',
                    filemode='a',  # 追加模式
                    # 日志格式
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )

USER_NAME = "821538716@qq.com"
USER_PWD = "htfxbkerhthpbcfb"
USER_HOST = "smtp.qq.com"
SEND_TO = "523393445@qq.com"
MAX_TRY = 2
TRY_DELAY = 30
# 发送邮件
def send_mail(send_user, send_password, send_host, to_user, to_subject, to_contents):
    yag = yagmail.SMTP(user=send_user, password=send_password, host=send_host)
    yag.send(to_user, to_subject, to_contents)

# 重启进程
def restart_scheduler():
    """
    # TODO 
    无法解决正确激活虚拟环境的问题,只能从根目录启动
    """
    pass
  
    # 清除干净进程
    stop_cmd = 'sh /home/lianghua/rtt/soft/airflow/dags/zirui_dag/sh_files/stop_scheduler.sh'
    logging.warning(f'正在结束scheduler:{stop_cmd}')
    subprocess.Popen(
        stop_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    # 重启进程
    start_cmd = 'sh /home/lianghua/rtt/soft/airflow/dags/zirui_dag/sh_files/start_scheduler.sh'
    logging.warning(f'正在启动scheduler:{start_cmd}')
    subprocess.Popen(
        start_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

# 进程检测
def check_process(Process):
    cmd = 'ps axu | grep %s | grep -v grep | wc -l' % Process
    res = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    logging.info(res)

    # 获取返回值
    if res.stdout.read().decode('utf-8').strip() == '0':
        subject = '[AirFlow警告] 进程 %s 宕机' % Process
        msg = ['%s 已停止运行,请检查,日志文件详见schedulerout.log' % Process]
        logging.warning(msg)
        send_mail(USER_NAME, USER_PWD,
                  USER_HOST, SEND_TO, subject, msg)
        # 重启
        restart_scheduler()
        # raise Exception('重新启动执行完毕')

    else:
        subject = '[AirFlow警告] 进程 %s 运行正常' % Process
        msg = ['%s 正常运行,日志文件详见schedulerout.log'% Process]
        logging.info(msg)
        send_mail(USER_NAME, USER_PWD,
                  USER_HOST, SEND_TO, subject, msg)

    

    logging.info(f'已发送邮件:{SEND_TO} {subject} {msg}')


if __name__ == '__main__':
    # check_process('scheduler')
    # restart_scheduler()
    i = 0
    while i < MAX_TRY:
        try:
            check_process('scheduler')
            logging.info('check_process正常执行,已退出检测')
            break
        except Exception as e:
            logging.warning(e)
            i += 1
            time.sleep(TRY_DELAY)
            logging.info(f'check_process重新执行:{i}/{MAX_TRY},{TRY_DELAY}')
            continue
    