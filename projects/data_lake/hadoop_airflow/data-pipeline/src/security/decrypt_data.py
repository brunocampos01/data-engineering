from io import BytesIO

from OpenSSL import crypto
from OpenSSL._util import ffi as _ffi
from OpenSSL._util import lib as _lib


def decrypt_p7s(data: bytes) -> str:
    """
    openssl cms -verify -in xxx.p7s -inform DER -noverify -outform DER -signer cert.pem -print
    """
    p7 = crypto.load_pkcs7_data(crypto.FILETYPE_ASN1,
                                BytesIO(data).getvalue())
    bio_out = crypto._new_mem_buf()
    _lib.PKCS7_verify(p7._pkcs7,
                      _ffi.NULL,
                      _ffi.NULL,
                      _ffi.NULL,
                      bio_out,
                      _lib.PKCS7_NOVERIFY)

    return crypto._bio_to_string(bio_out)


def decrypt_p7b(data: bytes) -> str:
    """
    openssl pkcs7 -inform DER -outform PEM -in xxx.p7b -print
    """
    with tempfile.NamedTemporaryFile() as temp:
        temp.write(data)
        temp.flush()
        return subprocess.check_output(
            ['openssl', 'pkcs7',
             '-inform', 'DER',
             '-outform', 'PEM',
             '-in', temp.name,
             '-print'
             ]
        ).decode('ISO-8859-1')
