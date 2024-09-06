import base64
from Crypto.Cipher import AES
from Crypto import Random
import os
from . import SQLType


class Encrypted(SQLType):
    block_size = 16

    def __init__(self, key):
        super().__init__("text", self.decrypt, self.encrypt)
        self.key = key

    def encrypt(self, raw):
        if raw is None:
            return None
        raw = self.pad(raw).encode()
        iv = Random.new().read(AES.block_size)
        key = self.key() if callable(self.key) else self.key
        cipher = AES.new(key, AES.MODE_CBC, iv)
        return base64.b64encode(iv + cipher.encrypt(raw))

    def decrypt(self, enc):
        if enc is None:
            return None
        enc = base64.b64decode(enc)
        iv = enc[:16]
        key = self.key() if callable(self.key) else self.key
        cipher = AES.new(key, AES.MODE_CBC, iv)
        return self.unpad(cipher.decrypt(enc[16:]).decode())

    def pad(self, s):
        return s + (self.block_size - len(s) % self.block_size) * chr(self.block_size - len(s) % self.block_size)
    
    def unpad(self, s):
        return s[:-ord(s[len(s)-1:])]
