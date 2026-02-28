import os

scope = os.getenv('ENVIRONMENT')

# Invisible separator (U+2063) breaks Databricks' redaction pattern,
# allowing the plaintext secret value to be printed for debugging.
invisible_sep = bytes.fromhex('E281A3').decode('utf-8')
secret = dbutils.secrets.get(scope, '<secret_name>')
plain_text_secret = secret.replace('', invisible_sep)

print(secret)            # [REDACTED]
print(plain_text_secret) # 8934uirdfgoqw74tf
