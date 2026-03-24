import grpc
import threading
import logging
from typing import Callable, List

from proto import broker_pb2_grpc, broker_pb2

logging.basicConfig(level=logging.INFO, format='[BOBBER] %(message)s')
logger = logging.getLogger()

class BobberClient:
    def __init__(self, host='localhost', port=50051):
        self.channel = grpc.insecure_channel(f'{host}:{port}')
        self.stub = broker_pb2_grpc.BrokerServiceStub(self.channel)
        self._active_streams = {}

    def healthcheck(self) -> bool:
        try:
            response = self.stub.HealthCheck(broker_pb2.HealthCheckRequest())
            return response.status == 'OK'
        except grpc.RpcError:
            return False

    def produce(self, topic: str, key: str, value: str) -> bool:
        request = broker_pb2.ProduceRequest(topic=topic, key=key, value=value)
        try:
            self.stub.Produce(request)
            logger.info(f'Produced {topic}:{key}:{value}')
            return True
        except grpc.RpcError as e:
            logger.error(f'Failed to produce {topic}:{key}:{value}. Error: {e.code()} - {e.details()}')
            return False

    def fetch(self, topic: str, partition: int = 0, offset: int = 0, limit: int = 10) -> List[dict]:
        request = broker_pb2.FetchRequest(topic=topic, partition=partition, offset=offset, limit=limit)

        try:
            response = self.stub.Fetch(request)

            messages = []
            for message in response.messages:
                messages.append({
                    'topic': message.topic,
                    'key': message.key,
                    'value': message.value,
                    'offset': message.offset,
                    'partition': message.partition
                })
            return messages
        except grpc.RpcError as e:
            logger.error(f'Failed to fetch {topic}:{partition}:{offset}. Error: {e.code()}')
            return []

    def subscribe(self, topic: str, callback: Callable[[dict], None]) -> threading.Thread:
        def _listen():
            request = broker_pb2.SubscribeRequest(topic=topic)
            try:
                stream = self.stub.Subscribe(request)
                logger.info(f'Subscribed {topic}:{topic}')

                for msg in stream:
                    data = {
                        'topic': msg.topic,
                        'key': msg.key,
                        'value': msg.value,
                        'offset': msg.offset,
                        'partition': msg.partition
                    }

                    callback(data)

            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.CANCELLED:
                    logger.info('Subscribe cancelled by client.')
                else:
                    logger.error(f'Failed to subscribe to \'{topic}\', error: {e.code()}')

        thread = threading.Thread(target=_listen, daemon=True)
        thread.start()
        return thread

    def close(self):
        self.channel.close()
        logger.info('Client closed.')
