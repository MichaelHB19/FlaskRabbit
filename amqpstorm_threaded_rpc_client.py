"""
This is a simple example on how to use Flask and Asynchronous RPC calls.

I kept this simple, but if you want to use this properly you will need
to expand the concept.

Things that are not included in this example.
    - Reconnection strategy.

    - Consider implementing utility functionality for checking and getting
      responses.

        def has_response(correlation_id)
        def get_response(correlation_id)

Apache/wsgi configuration.
    - Each process you start with apache will create a new connection to
      RabbitMQ.

    - I would recommend depending on the size of the payload that you have
      about 100 threads per process. If the payload is larger, it might be
      worth to keep a lower thread count per process.

For questions feel free to email me: me@eandersson.net
"""
__author__ = 'eandersson'
import os
import threading
from time import sleep

from flask import Flask
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)

class RpcClient(object):
    """Asynchronous Rpc client."""

    def __init__(self, host, username, password, vhost, rpc_queue):
        self.queue = {}
        self.host = host
        self.username = username
        self.password = password
        self.vhost = vhost
        self.rpc_queue = rpc_queue
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.open()

    def open(self):
        """Open Connection."""
        self.connection = amqpstorm.Connection(
            self.host, self.username, self.password, virtual_host=self.vhost
        )
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.rpc_queue)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']
        self.channel.basic.consume(self._on_response, no_ack=True, queue=self.callback_queue)
        self._create_process_thread()

    def _create_process_thread(self):
        """Create a thread responsible for consuming messages in response to RPC requests."""
        thread = threading.Thread(target=self._process_data_events)
        thread.setDaemon(True)
        thread.start()

    def _process_data_events(self):
        """Process Data Events using the Process Thread."""
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        """On Response store the message with the correlation id in a local dictionary."""
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        # Create the Message object.
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue

        # Create an entry in our local dictionary, using the automatically
        # generated correlation_id as our key.
        self.queue[message.correlation_id] = None

        # Publish the RPC request.
        message.publish(routing_key=self.rpc_queue)

        # Return the Unique ID used to identify the request.
        return message.correlation_id

@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    """Simple Flask implementation for making asynchronous Rpc calls."""
    # Send the request and store the requests Unique ID.
    corr_id = RPC_CLIENT.send_request(payload)

    # Wait until we have received a response.
    while RPC_CLIENT.queue[corr_id] is None:
        sleep(0.1)

    # Return the response to the user.
    return RPC_CLIENT.queue[corr_id]

if __name__ == '__main__':
    # Set the environment variables for host, username, password, and vhost
    host = os.getenv('AMQP_HOST', 'hawk.rmq.cloudamqp.com')
    username = os.getenv('AMQP_USER', 'onmndeeo')
    password = os.getenv('AMQP_PASS', 'iKDLaKYYbf-PqSZkvXlKmJid2UvyGH4f')
    vhost = os.getenv('AMQP_VHOST', 'onmndeeo')

    RPC_CLIENT = RpcClient(host, username, password, vhost, 'rpc_queue')
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 5000)))
