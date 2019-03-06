# Pywco
Python Websockets Communication library
Made in Czechia :beer:

It provides a high level synchronous API for the asynchronous Python websockets.

Ideally, what you throw in on one end like this

```
pywco_instance.send_message(MsgType.Foo, bar=42)
```

You get on the other end as a blinker signal

```
def my_handler(sender, bar):
  print(bar)

blinker.signal(MsgType.Foo).connect(my_handler)
```

And creating instance should work like this

```
pywco_instance = pywco.Client("localhost", 42424, MsgType)
```

MsgType is an Enum which pywco expects you to provide.
