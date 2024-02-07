from decouple import config
from datetime import datetime
import urllib
import requests

GATEWAY_URL = config("algorithm")


def send_sms_link(sms_rendered_content, rendered_sms_to, date_time, otp_value, name):
    if rendered_sms_to:
        # print("sms_rendered_content[0][0]=", sms_rendered_content[0][0])
        sms_rendered_contents = sms_rendered_content[0][0].replace('{#otp#}', otp_value).replace('{#time#}', date_time).replace('{#name#}', name).encode('ascii', 'ignore')
        # print("sms_rendered_contents", sms_rendered_contents)
        sms_rendered_content_message = urllib.parse.quote_plus(sms_rendered_contents)
        # print("sms_rendered_content_message", sms_rendered_content_message)
        send_url = sms_rendered_content[0][1].replace('{mobile}', '7004539246').replace('{message}', sms_rendered_content_message)
        # print("send_urlsend_urlsend_url", send_url)

        try:
            if send_url:
                # send_link = send_url.replace('{mobile}','7004539246').replace('{message}',sms_rendered_content_message)
                # print("send_link=",send_link)
                response = requests.request("Post", url=send_url).text
                # print("response==", response)
                return response
        except Exception as e:
            # raise Warning("Some error occurred while sending sms")
            return "Some error occurred while sending sms" + str(e)
