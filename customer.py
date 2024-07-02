import pika
import uuid

class CustomerClient:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        # Declare a direct exchange for routing messages
        self.channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

        # Declare a callback queue for receiving responses
        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        # Set up a subscription on the callback queue
        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

        self.response = None
        self.corr_id = None

    def on_response(self, ch, method, properties, body):
        if self.corr_id == properties.correlation_id:
            self.response = body

    def request_location(self, order_id):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        # Send the location request to the 'direct_logs' exchange with the order ID as the routing key
        self.channel.basic_publish(
            exchange='direct_logs',
            routing_key=order_id,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id
            ),
            body=str(order_id)
        )

        # Wait for the response
        while self.response is None:
            self.connection.process_data_events()

        return self.response.decode()

if __name__ == "__main__":
    customer = CustomerClient()
    order_id = "Order-12346"
    response = customer.request_location(order_id)
    print(f"Received location: {response}")