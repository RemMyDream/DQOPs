# read_ews_exchangelib.py
from exchangelib import DELEGATE, Account, Credentials, Configuration, NTLM, Message
import logging

logging.basicConfig(level=logging.INFO)  # bật log để debug nếu cần

# Thay bằng thông tin của bạn
USERNAME = r"csdld_baocao@pvn.vn"   # chú ý escaping \\ hoặc 'pvn\\user'
PASSWORD = "Muadong@2025"
PRIMARY_SMTP = "csdld_baocao@pvn.vn"
EWS_SERVER = "mail.pvn.vn"  # tên server (DNS nội bộ phải resolve đúng)

def main():
    creds = Credentials(username=USERNAME, password=PASSWORD)
    # Cấu hình dùng NTLM; nếu server dùng Basic thì auth_type có thể thay
    config = Configuration(
        server=EWS_SERVER,
        credentials=creds,   # <--- PHẢI CÓ
        auth_type=NTLM
    )
    try:
        account = Account(
            primary_smtp_address=PRIMARY_SMTP,
            credentials=creds,
            config=config,
            autodiscover=False,     # đã có server -> tắt autodiscover
            access_type=DELEGATE
        )
    except Exception as e:
        print("Kết nối tới Exchange thất bại:", e)
        return

    # Lấy 10 mail mới nhất trong Inbox
    try:
        print("Inbox count:", account.inbox.total_count)
        for item in account.inbox.all().order_by('-datetime_received')[:10]:
            print("----")
            print("From:", getattr(item.sender, 'email_address', item.sender) )
            print("==============")
            print("Subject:", item.subject)
            print("==============")
            print("Received:", item.datetime_received)
            # body có thể dài -> in đoạn đầu
            body_snippet = (item.body or "")[:400].replace("\n", " ")
            print("Body snippet:", body_snippet)
            print("==============")
            print("Full body:")
            print(item.body) 
            print("==============")
            # nếu có attachment, lưu ra file
            if item.attachments:
                for att in item.attachments:
                    if hasattr(att, 'content'):
                        filename = att.name
                        with open(filename, 'wb') as f:
                            f.write(att.content)
                        print(f"Saved attachment: {filename}")
    except Exception as e:
        print("Lỗi khi đọc email:", e)


if __name__ == "__main__":
    main()
