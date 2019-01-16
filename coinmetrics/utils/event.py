class EventEmitter(object):

    def __init__(self):
        self.subscribers = []

    def subscribe(self, subscriber):
        self.subscribers.append(subscriber)

    def unsubscribe(self, subscriber):
        self.subscribers.remove(subscriber)

    def trigger(self, *args):
        for subscriber in self.subscribers:
            subscriber(*args)
