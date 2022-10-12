import subprocess
import yagmail
import time


USER_NAME = "821538716@qq.com"
USER_PWD = "htfxbkerhthpbcfb"
USER_HOST = "smtp.qq.com"
CHECK_TIME = 1800


def send_mail(send_user, send_password, send_host, to_user, to_subject, to_contents):
    yag = yagmail.SMTP(user=send_user, password=send_password, host=send_host)
    yag.send(to_user, to_subject, to_contents)


def check_process(Process):
    cmd = 'ps axu | grep %s | grep -v grep | wc -l' % Process
    res = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    send_mail(USER_NAME, USER_PWD,
              USER_HOST, "523393445@qq.com", '[AirFlow警告] 进程 %s 运行正常' % Process, '日志文件详见schedulerout.log')
    # send_mail("821538716@qq.com", "htfxbkerhthpbcfb",
    #           "smtp.qq.com", "821538716@qq.com", '[AirFlow警告] 进程 %s 运行正常' % Process, '日志文件详见schedulerout.log')

    if res.stdout.read().decode('utf-8').strip() == '0':
        subject = '[AirFlow警告] 进程 %s 宕机' % Process
        msg = ['%s 已停止运行,请检查,日志文件详见schedulerout.log' % Process]
        send_mail(USER_NAME, USER_PWD,
                  USER_HOST, "523393445@qq.com", subject, msg)


if __name__ == '__main__':
    # check_process('scheduler')
    while True:
        check_process('scheduler')
        time.sleep(CHECK_TIME)
