"""
This is a simple example on how to use Flask and Asynchronous RPC calls.
"""

__author__ = 'eandersson'

import os
import threading
from time import sleep
from urllib.parse import urlparse

from flask import Flask
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)


class RpcClient(object):
    """Asynchronous Rpc client."""

    def __init__(self, host, username, password, rpc_queue):
        self.queue = {}
        self.host = host
        self.username = username
        self.password = password
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.rpc_queue = rpc_queue
        self.open()

    def open(self):
        """Open Connection."""
        self.connection = amqpstorm.Connection(self.host, self.username, self.password)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.rpc_queue)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']
        self.channel.basic.consume(self._on_response, no_ack=True, queue=self.callback_queue)
        self._create_process_thread()

    def _create_process_thread(self):
        """Create a thread to consume RPC responses."""
        thread = threading.Thread(target=self._process_data_events)
        thread.setDaemon(True)
        thread.start()

    def _process_data_events(self):
        """Consume messages continuously."""
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        """Store responses by correlation ID."""
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue
        self.queue[message.correlation_id] = None
        message.publish(routing_key=self.rpc_queue)
        return message.correlation_id


@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    """Simple Flask endpoint for making asynchronous RPC calls."""
    corr_id = RPC_CLIENT.send_request(payload)
    while RPC_CLIENT.queue[corr_id] is None:
        sleep(0.1)
    return RPC_CLIENT.queue[corr_id]


if __name__ == '__main__':
    # Read connection URL from environment
    amqp_url = os.environ.get('AMQP_URL')
    if not amqp_url:
        raise Exception("AMQP_URL environment variable not set")

    parsed = urlparse(amqp_url)
    host = parsed.hostname
    username = parsed.username
    password = parsed.password
    vhost = parsed.path[1:]  # Remove the leading '/'

    RPC_CLIENT = RpcClient(host, username, password, vhost)

    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
