import subprocess
import yagmail

import logging

logging.basicConfig(level=logging.DEBUG,  # 控制台打印的日志级别
                    filename='check_airflow.log',
                    filemode='a',  # 模式，有w和a，w就是写模式，每次都会重新写日志，覆盖之前的日志, a是追加模式，默认如果不写的话，就是追加模式
                    # 日志格式
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
                    )

USER_NAME = "821538716@qq.com"
USER_PWD = "htfxbkerhthpbcfb"
USER_HOST = "smtp.qq.com"
CHECK_TIME = 1800
SEND_TO = "523393445@qq.com"


def send_mail(send_user, send_password, send_host, to_user, to_subject, to_contents):
    yag = yagmail.SMTP(user=send_user, password=send_password, host=send_host)
    yag.send(to_user, to_subject, to_contents)


def check_process(Process):
    cmd = 'ps axu | grep %s | grep -v grep | wc -l' % Process
    res = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if res.stdout.read().decode('utf-8').strip() == '0':
        subject = '[AirFlow警告] 进程 %s 宕机' % Process
        msg = ['%s 已停止运行,请检查,日志文件详见schedulerout.log' % Process]
        logging.warning(msg)

    else:
        subject = '[AirFlow警告] 进程 % s 运行正常' % Process
        msg = ['正常运行,日志文件详见schedulerout.log']
        logging.info(msg)

    send_mail(USER_NAME, USER_PWD,
              USER_HOST, SEND_TO, subject, msg)

    logging.info(f'已发送邮件:{SEND_TO} {subject} {msg}')


if __name__ == '__main__':
    try:
        check_process('scheduler')
    except Exception as e:
        logging.warning(e)
