import redis
import secrets

from datetime import timedelta

r = redis.Redis()


class ResourceLocked(Exception):
    pass




def single(max_processing_time: timedelta):
    def wrapper(func):
        def inner_wrapper(*args, **kwargs):
            name = func.__name__
            if r.lock(name).locked():
                # если ключ name заблокирован, возвращаем ошибку
                raise ResourceLocked
            else:
                # получаем токен для значения ключа
                token = secrets.token_hex()
                # назначаем блокировку по ключу name, назначаем timeout блокировки
                r.lock(name, timeout=max_processing_time.seconds).do_acquire(token)
                res = func(*args, **kwargs)
                # после выполнения функции отпускаем блокировку
                r.lock(name).do_release(token)
                return res
        return inner_wrapper
    return wrapper


@single(max_processing_time=timedelta(minutes=0.5))
def hello(a, b):
    return a + b


print(hello(1, 2))