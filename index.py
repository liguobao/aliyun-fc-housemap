import pymysql.cursors
import smtplib
import datetime
import json
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
# 172.16.150.195
db_host = "172.16.150.195"
db_port = 12222
db_password = ""
email_account = ""
email_password = ""
email_receivers = ""


def handler(event, context):
    # print(event)
    # config_data = json.loads(event)
    # print(config_data)
    # db_host = config_data["db_host"]
    # db_port = int(config_data["db_port"])
    # db_password = config_data["db_password"]
    # email_account = config_data["email_account"]
    # email_password = config_data["email_password"]
    # email_receivers = config_data["email_receivers"]
    connection = pymysql.connect(host=db_host, port=db_port,
                                 user='root', password=db_password,
                                 db='housemap', charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    with connection.cursor() as cursor:
        tables = ["MoguHouse", "ZuberHouse", "DoubanHouse",
                  "BaixingHouse", "DoubanWechatHouse"]
        stats = []
        bodyHTML = '''<table border='1' cellpadding='0' cellspacing='0' width='100%'> 
             <tr> 
             <td>来源</td>
             <td>数量</td>
             <td>最晚发布时间</td>
             <td>最晚入库时间</td> 
             </tr>'''
        houseSum = 0
        for table in tables:
            sql = "SELECT count(*) as Count,Source,MAX(PubTime) AS LastPubTime,MAX(CreateTime) AS LastCreateTime FROM %s where CreateTime > '%s';" % (table, yesterday)
            print(sql)
            cursor.execute(sql)
            result = cursor.fetchone()
            if result["Source"] is None:
                bodyHTML = bodyHTML + " <tr> <td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                    table, "", "", "")
                continue
            stats.append(result)
            bodyHTML = bodyHTML + " <tr> <td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>" % (
                result["Source"], result["Count"], result["LastPubTime"], result["LastCreateTime"])
            houseSum = houseSum + int(result["Count"])
        bodyHTML = bodyHTML + " <tr> <td>共计</td><td>%d</td> </tr>" % (houseSum)
        bodyHTML = bodyHTML + " </table>"
        subject = "地图搜租房每日数据汇总(%s)-新版" % datetime.date.today()
        send_email(email_account, email_receivers,
                   subject, bodyHTML)
        print(stats)
        return stats


def send_email(sender, receivers, subject, content):
        smtp_client = smtplib.SMTP_SSL(
            "smtp.qq.com", port="465")
        smtp_client.login(email_account, email_password)
        message = MIMEText(content, _subtype='html', _charset='utf-8')
        message['Subject'] = subject
        message['From'] = sender
        smtp_client.sendmail(sender, receivers, message.as_string())
        smtp_client.close()


handler("", "")
