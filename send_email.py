import os
import time
import configparser
import smtplib
import logging
import pprint
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


class send_email_of_weather():
    '''
    发送邮件
    '''
    def __init__(self, config_file_name, receiver, subject, body, content, logger):
        # self.sender = 'etl@linezonedata.com'  # 发件人邮箱账号
        # self.psd = 'Lzsj2015'  # 发件人邮箱密码(当时申请smtp给的口令)
        host, port, username, password = get_config(config_file_name)
        self.sender = username
        self.psd = password
        self.logger = logger
        if not body:
            self.logger.warning("今日门店要货单没有输出")
            self.send_error_email(receiver, "今日门店要货单没有输出，请及时检查")
            return
        if receiver is not None:
            # for i in receiver:
            ret = self.mail(receiver, subject, content, body)
            if ret:
                self.logger.info("邮件发送%s成功" % receiver)
            else:
                self.logger.error("邮件发送%s失败" % receiver)

    def send_error_email(self, receiver, subject, email_host='smtp.exmail.qq.com', email_port=465):
        ret = True
        try:
            msg = MIMEText(content, 'plain', 'utf-8')
            msg = MIMEMultipart()
            msg["Subject"] = subject  # "这是一部小说"
            msg["From"] = self.sender
            msg["To"] = ",".join(receiver)
            today = time.strftime('%Y-%m-%d')

            server = smtplib.SMTP_SSL(email_host, email_port)  # 发件人邮箱中的SMTP服务器，端口是465
            server.login(self.sender, self.psd)  # 括号中对应的是发件人邮箱账号、邮箱密码
            self.logger.info("登录成功")
            server.sendmail(self.sender, receiver, msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
            # print("login ok")
            server.quit()  # 关闭连接
        except Exception as e:  # 如果 try 中的语句没有执行，则会执行下面的 ret=False
            self.logger.error("Failed to send email to " + ",".join(receiver))
            self.logger.error('错误信息为:', e)
            ret = False
        return ret

    def mail(self, receiver, subject, content, body, email_host='smtp.exmail.qq.com', email_port=465):
        ret = True
        try:
            msg = MIMEText(content, 'plain', 'utf-8')
            msg = MIMEMultipart()
            msg["Subject"] = subject  # "这是一部小说"
            msg["From"] = self.sender
            msg["To"] = ",".join(receiver)
            today = time.strftime('%Y-%m-%d')
            # ---这是文字部分---
            part = MIMEText("你好，附件是{} 今日要货单".format(today))
            msg.attach(part)
            # ---这是附件部分---
            # xlsx类型附件
            for item in body:
                part = MIMEApplication(open(item, 'rb').read())
                file_name = os.path.basename(item)
                part.add_header('Content-Disposition',
                                'attachment',
                                filename=file_name)
                msg.attach(part)
            server = smtplib.SMTP_SSL(email_host, email_port)  # 发件人邮箱中的SMTP服务器，端口是465
            server.login(self.sender, self.psd)  # 括号中对应的是发件人邮箱账号、邮箱密码
            self.logger.info("登录成功")
            server.sendmail(self.sender, receiver, msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
            # print("login ok")
            server.quit()  # 关闭连接
        except Exception as e:  # 如果 try 中的语句没有执行，则会执行下面的 ret=False
            self.logger.error("Failed to send email to " + ",".join(receiver))
            self.logger.error('错误信息为:', e)
            ret = False
        return ret

def get_logger(name): 
    logger = logging.getLogger(name)
    dir_name = os.path.dirname(__file__)
    log_dir = os.path.join(dir_name, "log")
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
    log_name = os.path.join(log_dir, time.strftime('%Y-%m-%d')+".log")
    if not logger.handlers: 
        # Prevent logging from propagating to the root logger 
        logger.propagate = 0 
        # dirname, filename = os.path.split(__file__)
        #log_name = time.strftime('%Y-%m-%d')
        # log_file = os.path.join(log_dir, log_name)
        # 设置打印日志的形式
        LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
        # 设置日志器输出的格式
        r_handler = logging.FileHandler(log_name, encoding='utf-8')
        r_handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(r_handler)
    return logger 


def get_config(file_name):
    """
    从配置文件获取邮箱连接信息
    :param file_name 配置文件路径
    :return host 邮箱服务器ip
    :return port 端口
    :return username 用户名
    :return password 密码
    """
    cf = configparser.ConfigParser()
    cf.read(file_name)
    host = cf.get("email", "host")
    port = cf.get("email", "port")
    username = cf.get("email", "username")
    password = cf.get("email", "password")
    return host, port, username, password


def get_target_excel(dir_names):
    """
    获取指定文件夹下
    """
    target_list = []
    # subfix = '原材料半成品建议要货结果表_{0}.xls'.format(time.strftime('%Y%m%d'))
    # tt = '现制预测核对表_{0}.xls'.format(time.strftime('%Y%m%d'))
    target_set = {'原材料半成品建议要货结果表_{0}.xls'.format(time.strftime('%Y%m%d')), '现制预测核对表_{0}.xls'.format(time.strftime('%Y%m%d'))}
    for dir_name in dir_names:
        for entry in os.scandir(dir_name):
            if entry.name in target_set and entry.is_file():
                target_list.append(os.path.join(dir_name, entry.name))
    return target_list


if __name__ == "__main__":
    dir_name = os.path.dirname(__file__)
    config_file_name = os.path.join(dir_name, 'email.config')
    dir_name = ["""/home/fresh_model/workspace/yy/data""", """/home/fresh_model/workspace/wly/fresh_model/data"""]
    receiver = ['xiehongwang@linezonedata.com', 'chenlong@linezonedata.com', 'yuanye@linezonedata.com']
    subject = "菲尔雪每日要货单"
    content = "你好，附件是菲尔雪今日门店要货单，请查收"
    # 这个是目标文件列表
    body = get_target_excel(dir_name)
    pprint.pprint(body)
    logger = get_logger("fresh_send_email")
    # 设置日志器级别
    logger.setLevel(logging.DEBUG)
    # if body:
    # host, port, username, password = get_config(config_file_name)
    obj = send_email_of_weather(config_file_name, receiver, subject, body, content, logger)
    # else:
        # logger.warning("今日门店要货单没有输出")