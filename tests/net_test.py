from unittest import TestCase
from ming_kdd.utils.net import tcp_port_test


class NetTest(TestCase):
    def test_tcp_port_test(self):
        host = '115.159.36.105'
        port = 3306
        print(host, '端口开放状态：', tcp_port_test(host, port))