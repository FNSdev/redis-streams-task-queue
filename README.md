# How it works

If consumer dies, it will try to process all it`s pending messaged on restart.

If some messages were not consumed by workers (all workers were dead, for example),
after first worker starts, it will consume all these messages.

# How to use

### Before you start, create stream and consumer group
```
$ docker-compose up
$ docker exec -it <container_name> redis-cli
$ xgroup create events event_consumers $ MKSTREAM
```

### Run producer
```
$ python producer.py
```

### Run consumers
```
$ python consumer.py w1
```
```
$ python consumer.py w2
```

# TODO
You will need to give your workers same names, or they will not be able to process pending messages.
In case it is not possible, or you can`t restart worker, it would be nice if other worker could claim pending
messages of a dead worker. 
