# Задача - Ограничитель скорости (rate limiter)
#
# Ваше приложение делает HTTP запросы в сторонний сервис (функция make_api_request),
# при этом сторонний сервис имеет проблемы с производительностью и ваша задача ограничить
# количество запросов к этому сервису - не больше пяти запросов за последние три секунды.
#
# Ваша задача реализовать RateLimiter.test метод который:
#
#     возвращает True в случае если лимит на кол-во запросов не достигнут
#     возвращает False если за последние 3 секунды уже сделано 5 запросов.
#
# Ваша реализация должна использовать Redis, т.к. предполагается что приложение работает на нескольких серверах.
from random import randint

import redis
import random
import time

r = redis.Redis()


class RateLimitExceed(Exception):
    pass


class RateLimiter:
    def test(self) -> bool:
        # если ключ существует
        if r.exists("r_l"):
            # получаем количество запросов
            limit = r.get("r_l").decode("utf-8")
            # если кол-во запросов меньше 5
            if int(limit) < 5:
                # увеличиваем кол-во запросов на 1
                r.incrby("r_l", 1)
                return True
            else:
                return False
        else:
            # если ключа нет, создаём запись с временем жизни 3 секунды
            r.setex("r_l", 3, 1)
            return True


def make_api_request(rate_limiter: RateLimiter):
    if not rate_limiter.test():
        raise RateLimitExceed
    else:
        # какая-то бизнес логика
        pass


if __name__ == '__main__':
    rate_limiter = RateLimiter()

    for _ in range(50):
        time.sleep(random.choice([0.1, 0.2, 0.3, 0.4, 0.5]))

        try:
            make_api_request(rate_limiter)
        except RateLimitExceed:
            print("Rate limit exceed!")
        else:
            print("All good")

