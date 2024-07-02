import pika

class TruckServer:
    def __init__(self, truck_id, order_ids):
        self.truck_id = truck_id
        self.order_ids = order_ids
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        self.channel = self.connection.channel()

        # Declare a direct exchange for routing messages
        self.channel.exchange_declare(exchange='direct_logs', exchange_type='direct')

        # Declare a queue for each order ID the truck handles and bind it to the direct exchange
        self.queues = []
        for order_id in self.order_ids:
            result = self.channel.queue_declare(queue='', exclusive=True)
            queue_name = result.method.queue
            self.channel.queue_bind(exchange='direct_logs', queue=queue_name, routing_key=order_id)
            self.queues.append(queue_name)

    def on_request(self, ch, method, properties, body):
        order_id = body.decode()
        print(f"Received location request for {order_id}")

        # Process the location request (mock processing)
        location = f"Truck {self.truck_id} is at coordinates (lat, long)"

        # Send the location response back to the customer application
        ch.basic_publish(
            exchange='',
            routing_key=properties.reply_to,
            properties=pika.BasicProperties(
                correlation_id=properties.correlation_id
            ),
            body=location
        )

        ch.basic_ack(delivery_tag=method.delivery_tag)

    def start(self):
        for queue_name in self.queues:
            self.channel.basic_consume(queue=queue_name, on_message_callback=self.on_request)

        print(f"Truck {self.truck_id} waiting for location requests. To exit press CTRL+C")
        self.channel.start_consuming()

if __name__ == "__main__":
    # Each truck can handle multiple order IDs
    truck = TruckServer(truck_id="Truck-1", order_ids=["Order-12346", "Order-67890"])
    truck.start()