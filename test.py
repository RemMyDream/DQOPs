from exchangelib import Credentials, Account, DELEGATE, Configuration

# Cấu hình credentials
creds = Credentials(username='csdld_baocao@pvn.vn', password='Muadong@2025')

# Tùy nếu không có autodiscover, ta khai báo thủ công
config = Configuration(
    server='hni-svr-mb01.pvn.vn',
    credentials=creds,
    auth_type=None  # exchangelib sẽ tự nhận, có thể dùng NTLM
)

# Khởi tạo account
account = Account(
    primary_smtp_address='csdld_baocao@pvn.vn',
    credentials=creds,
    autodiscover=False,
    config=config,
    access_type=DELEGATE
)

# Đọc mail trong Inbox
for item in account.inbox.all().order_by('-datetime_received')[:10]:
    print(f"Subject: {item.subject}")
    print(f"From: {item.sender.email_address}")
    print(f"Received: {item.datetime_received}")
    print("----------")
