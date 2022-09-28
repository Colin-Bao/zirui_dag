#!/usr/bin/env python
# -*- coding:utf-8 -*-
# @FileName  :SMTP.py
# @Time      :2022/9/5 14:24
# @Author    :Colin
# @Note      :None

import smtplib
from email.mime.text import MIMEText

# 配置信息
FROM_MAIL = '523393445@qq.com'
FROM_PWD = 'wfanjiafpntkcaie'
TO_MAIL = '523393445@qq.com'
SMTP_CONFIG = {'host': 'smtp.qq.com', 'port': 25}


class MailMsg:
    def __init__(self):
        # 初始化配置信息
        self.FROM_MAIL = FROM_MAIL  # 发件人
        self.FROM_PWD = FROM_PWD  # 发件人密码
        self.TO_MAIL = TO_MAIL  # 收件人
        self.SMTP_CONFIG = SMTP_CONFIG  # SMTP协议配置信息
        self.STATUS = False  # 发送成功

    # 建立SMTP连接
    def __get_conn(self):
        self.smtp = smtplib.SMTP()
        self.smtp.connect(self.SMTP_CONFIG['host'], self.SMTP_CONFIG['port'])
        self.smtp.login(self.FROM_MAIL, self.FROM_PWD)
        return self.smtp

    # 发送邮件
    def send_msg(self, msg_sub, mag_content, msg_to=TO_MAIL, msg_from=FROM_MAIL):
        # 内容
        mail = MIMEText(mag_content)
        mail['Subject'] = msg_sub
        mail['From'] = msg_from
        mail['To'] = msg_to

        # SMTP连接和发送
        self.__get_conn().sendmail(msg_from, msg_to, mail.as_string())
        self.STATUS = True

    # [上下文管理器]
    def __enter__(self):
        return self

    # [上下文管理器]
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.smtp.quit()  # 销毁资源
        if self.STATUS:
            print('发送成功')
        else:
            print('发送失败', exc_type, exc_val, exc_tb)
        return True  # 屏蔽异常


# 邮件发送示例
def demo():
    with MailMsg() as msg:
        msg.send_msg('主题', '内容:发生了异常')
