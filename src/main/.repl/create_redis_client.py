import redis
r = redis.StrictRedis(host="ec2-52-54-82-137.compute-1.amazonaws.com", port=6379, db=0, password="BFHW6zDv3g7kuxDxRXV7K8Y2pdyfR7kw")
p = r.pubsub()
p.psubscribe('tcp.http')
for msg in p.listen():
    print str(msg)
