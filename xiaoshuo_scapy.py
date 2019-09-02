from selenium import webdriver
import  pprint
import time
import sys
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.support.wait import WebDriverWait  
from selenium.webdriver.support import expected_conditions as EC  
from selenium.webdriver.common.by import By  
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
class cdzdgw(object):
    """
    下载www.cdzdgw.com这个网站的小说，也就是笔趣阁
    """
    def __init__(self, origin_url):
        self.chrome_browser = webdriver.Chrome()
        self.chrome_browser.get(origin_url)
        # 每一章小说的连接
        time.sleep(5)
        WebDriverWait(self.chrome_browser,10).until(EC.visibility_of_element_located((By.ID, 'list')))
        url_div = self.chrome_browser.find_element_by_css_selector("[id='list']").find_elements_by_tag_name('a')
        # 小说的名字
        title = self.chrome_browser.find_element_by_tag_name('h1').text
        #url_every = [i.find_element_by_tag_name('a').get_attribute('href') for i in url_div]
        #pprint.pprint(title)
        # 开始下载
        # pprint.pprint(url_div)
        total_url = []
        start = time.time()
        for i in url_div:
            total_url.append(i.get_attribute('href'))#.find_element_by_tag_name('a')
        # for i in range(len(url_div)-1,-1,-1):
        #     total_url.append(url_div[i].get_attribute('href'))#.find_element_by_tag_name('a')
        # pprint.pprint(total_url)
            # break
        self.downloader(total_url, title)
        end = time.time()
        print("runninng time: {0}".format(end-start))
        #pprint.pprint(url_every[12:])
        # print(chrome_browser.page_source)
        self.chrome_browser.close()
        # time.sleep(1)

    def downloader(self, total_url, title):
        """
        单线程下载传入的每一章的连接
        :param total_url: 小说的所有章节的URL
        :param title: 小说的名字
        :return: 无
        """
        len_ = len(total_url)
        total_txt = []
        for url in total_url:
            # 每一章小说的URL
            self.chrome_browser.get(url)
            WebDriverWait(self.chrome_browser,20,0.5).until(EC.presence_of_element_located((By.ID, 'content')))  
            total_txt.append('\n'+self.chrome_browser.find_element_by_css_selector("[class='bookname']").find_element_by_tag_name('h1').text+'\n')
            content_div = self.chrome_browser.find_element_by_css_selector("[id='content']").text
            # total_txt.append("\n".join(content_div.split('\n')[:-3]))
            total_txt.append(content_div)
            
        print("开始写入文本")
        with open("".join([title, '.txt']), 'w', encoding='utf-8') as f:
            f.write("".join(total_txt))
            print("写入成功")

class send_email_of_weather():
    '''
    发送邮件
    '''
    def __init__(self,receiver,subject,body):
        self.sender = '1172498176@qq.com'    # 发件人邮箱账号
        self.psd = 'lddzogtscniahgfb'       # 发件人邮箱密码(当时申请smtp给的口令)
        if receiver is not None:
            # for i in receiver:
            ret = self.mail(receiver,subject,body)
            if ret:
                print("邮件发送%s成功"%receiver)
            else:
                print("邮件发送%s失败"%receiver)
    #my_sender='3295468820@qq.com'    # 发件人邮箱账号
    #my_pass = 'nlropspeooyvdaii'              # 发件人邮箱密码(当时申请smtp给的口令)
    #receiver='1602983878@qq.com'      # 收件人邮箱账号
    def mail(self,receiver,subject,body):
        ret=True
        try:
            msg=MIMEText(body,'plain','utf-8')
            msg = MIMEMultipart()
            msg["Subject"] = subject#"这是一部小说"
            msg["From"]    = self.sender
            msg["To"]      = receiver
            
            #---这是文字部分---
            part = MIMEText("你好，附件是一部小说请查收")
            msg.attach(part)
            #---这是附件部分---
            #xlsx类型附件
            part = MIMEApplication(open(body,'rb').read())#'九劫剑魔.txt'
            part.add_header('Content-Disposition', 'attachment', filename=body)
            msg.attach(part)

            # msg['From']= formataddr(["发件人昵称",self.sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
            # msg['To']= formataddr(["收件人昵称",receiver])              # 括号里的对应收件人邮箱昵称、收件人邮箱账号
            #msg['Subject']="给最最最可爱的姝姝"                # 邮件的主题，也可以说是标题
            # msg['Subject']= subject
            
            server=smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器，端口是465
            server.login(self.sender, self.psd)  # 括号中对应的是发件人邮箱账号、邮箱密码
            server.sendmail(self.sender,[receiver,],msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
            #print("login ok")
            server.quit()# 关闭连接
        except Exception:# 如果 try 中的语句没有执行，则会执行下面的 ret=False
            print("Failed to send email to "+receiver)
            ret=False
        return ret

if __name__ == '__main__':
    #url = sys.argv[1] if sys.argv[1] else "https://www.cdzdgw.com/6_6828/"
    url = "https://www.biquge.com.cn/book/32726/"
    xs_obj = cdzdgw(url)
    receiver = '1172498176@qq.com'
    subject = "这是一部小说"
    body = "为死者代言.txt"
    obj = send_email_of_weather(receiver, subject, body)
    # t = "http://www.win4000.com/meinv170324.html"
    # print("".join([t[:-11],'1', t[-5:]]))