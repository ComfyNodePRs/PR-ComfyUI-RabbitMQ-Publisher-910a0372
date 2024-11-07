import json

import pika


class RabbitMQClient:
    def __init__(
        self,
        exchange: str,
        routing_key: str = 'image',
        host: str = 'localhost',
        port: int = 5672,
        username: str = 'guest',
        password: str = 'guest'
    ):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(
            host=host,
            port=port,
            credentials=pika.PlainCredentials(username, password)
        ))
        self.channel = self.connection.channel()
        self.exchange = exchange
        self.routing_key = routing_key

    def declare_exchange(
        self,
        exchange_type: str = 'direct',
        durable: bool = True,
        auto_delete: bool = False,
        internal: bool = False
    ):
        """test exchange declaration"""
        self.channel.exchange_declare(
            exchange=self.exchange,
            exchange_type=exchange_type,
            durable=durable,
            auto_delete=auto_delete,
            internal=internal
        )

    def publish(self, message: dict):
        self.channel.basic_publish(
            exchange=self.exchange,
            routing_key=self.routing_key,
            body=json.dumps(message)
        )

    def close(self):
        self.channel.close()
        self.connection.close()


