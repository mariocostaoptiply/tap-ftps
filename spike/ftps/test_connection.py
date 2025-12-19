#!/usr/bin/env python3
"""
Simple script to test FTPS connection to local server.
Run this after starting the FTPS server with:
  docker build -t ftps-server spike/ftps/
  docker run -d -p 21:21 -p 30000-30059:30000-30059 --name ftps-server ftps-server
"""

from ftplib import FTP_TLS
import ssl

HOST = 'localhost'
PORT = 21
USER = 'ftptest'
PASS = 'ftptest'

def test_connection():
    """Test FTPS connection to local server"""
    try:
        # Create SSL context (same as in client.py)
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        ctx.options |= ssl.OP_NO_SSLv3
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        # Connect (same pattern as in client.py)
        ftp = FTP_TLS(context=ctx)
        ftp.connect(host=HOST, port=PORT)
        ftp.login(user=USER, passwd=PASS)
        ftp.prot_p()  # Enable secure data channel
        
        print("✓ FTPS connection successful!")
        print(f"  Server: {HOST}:{PORT}")
        print(f"  User: {USER}")
        
        # Test listing directory
        files = ftp.nlst()
        print(f"  Files in root: {files}")
        
        ftp.quit()
        return True
    except Exception as e:
        print(f"✗ FTPS connection failed: {e}")
        return False

if __name__ == '__main__':
    test_connection()
