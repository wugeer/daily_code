import datetime
import time


def get_cost_time_log(level):
    """
    打印函数执行时间的装饰器
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            print('[{log_level}]: function {func_name} start at  {local_time}'.format(
                log_level=level,
                local_time=datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                func_name=func.__name__
            ))
            func(*args, **kwargs)
            local_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print('[{log_level}]: function {func_name} end at  {local_time} cost {cost_time} seconds!'.format(
                log_level=level, 
                local_time=local_time,
                func_name=func.__name__,
                cost_time=time.time()-start
            ))
        return wrapper
    return decorator


def singleton(cls):
    """
    使用inner实现单例模式
    """
    _instance = {}

    def inner():
        if cls not in _instance:
            _instance[cls] = cls()
        return _instance[cls]
    return inner
