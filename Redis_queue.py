# Задача - Очередь
#
# Реализуйте класс очереди который использует редис под капотом


import redis
import json


r = redis.Redis()


class RedisQueue:
    def publish(self, msg: dict):
        r.lpush("my_queue", json.dumps(msg))

    def consume(self) -> dict:
        res = r.rpop("my_queue")
        return json.loads(res)


if __name__ == '__main__':
    q = RedisQueue()
    q.publish({'a': 1})
    q.publish({'b': 2})
    q.publish({'c': 3})


    assert q.consume() == {'a': 1}
    assert q.consume() == {'b': 2}
    assert q.consume() == {'c': 3}

