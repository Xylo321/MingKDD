MINGMQ_CONFIG = {
    'add_article': {
        'host': 'serv_pro',
        'port': 15673,
        'user_name': 'mingmq',
        'passwd': 'mm5201314',
        'pool_size': 1,
        'queue_name': 'ming_kdd_add_article'
    },
    'add_photo': {
        'host': 'serv_pro',
        'port': 15673,
        'user_name': 'mingmq',
        'passwd': 'mm5201314',
        'pool_size': 10,
        'queue_name': 'ming_kdd_add_photo'
    },
    'add_video': {
        'host': 'serv_pro',
        'port': 15673,
        'user_name': 'mingmq',
        'passwd': 'mm5201314',
        'pool_size': 10,
        'queue_name': 'ming_kdd_add_video'
    },
    'download': {
        'host': 'serv_pro',
        'port': 15673,
        'user_name': 'mingmq',
        'passwd': 'mm5201314',
        'pool_size': 10,
        'queue_name': 'ming_kdd_download'
    }
}

DATA_TYPE = {
    'video': 1,
    'photo': 0
}

URL_TYPE = {
    '直接下载': 0,
    'm3url': 1
}