import redis
import time

client = redis.Redis()
total = 0.0
for i in range(100):
    start = time.time()
    client.mget("test1", "ttest2", "test4")
    total += time.time() - start

print total/100