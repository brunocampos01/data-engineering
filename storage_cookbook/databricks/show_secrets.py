import os

scope = os.getenv('Enviroment')

invisible_sep = bytes.fromhex("E281A3").decode("utf-8")
secret = dbutils.secrets.get(scope, "<secret_name>")
plain_text_secret = secret.replace("", invisible_sep)

print(secret)  # [REDACTED]
print(plaintextSecret)  # 8934uirdfgoqw74tf
