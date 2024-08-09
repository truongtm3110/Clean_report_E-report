import time
import uuid
import gzip
from pika import URLParameters, BlockingConnection, BasicProperties
from helper.QueueRPCMessage_pb2 import QueueRequestBodyMessage, QueueResponseDataMessage, MapFieldEntry


class QueueRpcClient(object):

    def __init__(self, rabbitmq_url):
        self.response = None
        self.corr_id = None
        params = URLParameters(rabbitmq_url)
        params.socket_timeout = 5

        self.connection = BlockingConnection(params)

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            gz_un_compressed_body = gzip.decompress(body)
            queue_response_data_message = QueueResponseDataMessage()
            queue_response_data_message.ParseFromString(gz_un_compressed_body)
            content_encoding = None
            for header in queue_response_data_message.headers:
                if header.key.lower() == "content-encoding":
                    content_encoding = header.value
            body_bytes = queue_response_data_message.body_bytes
            if content_encoding is not None and "gzip" in content_encoding:
                try:
                    body_bytes = gzip.decompress(body_bytes)
                except gzip.BadGzipFile:
                    pass
            queue_response_data_message.body_bytes = body_bytes
            self.response = queue_response_data_message

    def call(self, queue_request_body_object: QueueRequestBodyMessage) -> QueueResponseDataMessage:
        body = queue_request_body_object.SerializeToString()
        gz_compressed_body = gzip.compress(body)
        self.response = QueueResponseDataMessage()
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(
            exchange='',
            routing_key='rpc_newqueue',
            properties=BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=self.corr_id,
            ),
            body=gz_compressed_body)
        start_call_time = time.time()
        timeout = 15
        if queue_request_body_object.timeout > 0:
            timeout = queue_request_body_object.timeout
        while len(self.response.worker_mac) <= 0 and time.time() - start_call_time < timeout:
            self.connection.process_data_events(time_limit=timeout)
        return self.response

    def close(self):
        self.connection.close()


def test(item):
    request_body = QueueRequestBodyMessage()
    request_body.full_url = "https://apiv2.beecost.vn/product/detail?product_base_id=1__5765888554__74804504"
    request_body.method = "GET"
    headers = {
        "accept": "application/json",
        "accept-language": "en-US,en;q=0.9,vi;q=0.8",
    }
    for key in headers.keys():
        header = MapFieldEntry()
        header.key = key
        header.value = headers[key]
        request_body.headers.append(header)
    request_body.body_bytes = "data=hello".encode("utf-8")
    request_body.timeout = 30
    qrc = QueueRpcClient(rabbitmq_url="amqp://rpc_producer:SenSen1122_@45.122.220.8:5672/")
    response = qrc.call(request_body)
    qrc.close()
    print("crawl success")
    return response


if __name__ == '__main__':
    from multithread_helper import multithread_helper

    time_start = time.time()
    items = [i for i in range(0, 100)]
    results = multithread_helper(items=items, method=test, debug=False)
    print(f"time execute: {time.time() - time_start}")
