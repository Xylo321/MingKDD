def task_complete_rate(rate):
    """打印一个百分比
    """
    if rate < 1 and rate > 100:
        raise
    print("\r|%s%s|%d%%" % ((rate % 101) * '█', (100 - rate % 101) * ' ', rate), flush=True, end='')