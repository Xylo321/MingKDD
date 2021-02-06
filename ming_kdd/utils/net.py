import socket
import logging


def tcp_port_test(host_or_ip, port):
    """测试制定的服务器的端口是否打开
    """
    s = socket.socket()
    try:
        s.connect((host_or_ip, port))
        logging.debug('服务器端口打开')
        return True
    except:
        logging.debug('服务器端口关闭')
        return False
    finally:
        s.close()