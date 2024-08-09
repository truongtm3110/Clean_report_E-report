import socket
import time
import asyncio
import gzip
import uuid
from hashlib import md5

from aio_pika import connect, IncomingMessage, Message

from helper.QueueRPCMessage_pb2 import QueueRequestBodyMessage, QueueResponseDataMessage, MapFieldEntry
from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple
from helper.multi_thread_new import multi_task_async

logger = LoggerSimple(name=__name__).logger


class QueueRpcClientAsync:
    def __init__(self):
        self.connection = None
        self.channel = None
        self.callback_queue = None
        self.futures = {}
        self.loop = asyncio.get_event_loop()
        self.process_name = md5(socket.gethostname().encode()).hexdigest()[0:5]

    async def connect(self, rabbitmq_url):
        self.connection = await connect(rabbitmq_url, loop=self.loop)
        self.channel = await self.connection.channel()
        self.callback_queue = await self.channel.declare_queue(
            name=f"thanos-{self.process_name}-" + str(uuid.uuid4()),
            exclusive=True, arguments={
                'x-message-ttl': 120 * 1000,
                'x-expires': 120 * 1000
            },
            auto_delete=True
        )
        await self.callback_queue.consume(self.on_response)
        return self

    def on_response(self, message: IncomingMessage):
        body = message.body
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
            except Exception:
                pass
        queue_response_data_message.body_bytes = body_bytes
        future = self.futures.pop(message.correlation_id)

        if not future.cancelled():
            logger.debug(f"Receive response from worker: {queue_response_data_message.worker_name}")
            future.set_result(queue_response_data_message)

    async def call(self, queue_request_body_object: QueueRequestBodyMessage) -> QueueResponseDataMessage:
        if queue_request_body_object.timeout <= 0:
            queue_request_body_object.timeout = 15
        body = queue_request_body_object.SerializeToString()
        gz_compressed_body = gzip.compress(body)
        correlation_id = str(uuid.uuid4())
        future = self.loop.create_future()

        self.futures[correlation_id] = future

        await self.channel.default_exchange.publish(
            Message(
                gz_compressed_body,
                correlation_id=correlation_id,
                reply_to=self.callback_queue.name,
                expiration=120 * 1000,  # in seconds,
                headers={
                    "hostname": socket.gethostname()
                }
            ),
            routing_key="rpc_newqueue",
        )
        return await future

    async def close(self):
        await asyncio.wait_for(self.connection.close(), timeout=10)


async def test(product_base_id, source=None):
    try:
        fibonacci_rpc = await QueueRpcClientAsync().connect(
            rabbitmq_url="amqp://rpc_producer:SenSen1122_@45.122.220.8:5672/"
        )
        request_body = QueueRequestBodyMessage()
        request_body.full_url = f"https://apiv2.beecost.vn/product/detail?product_base_id={product_base_id}"
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
        request_body.timeout = 30
        response = await fibonacci_rpc.call(request_body)
        await fibonacci_rpc.close()
        print(f"{source} crawl success: {response.body_bytes}")
        return response
    except Exception as e:
        log_error(e)
    return None


async def test2(item):
    import random
    await asyncio.sleep(random.randint(0, 10))
    return True


if __name__ == "__main__":
    time_start = time.time()
    lst_product_base_id = [
        '3__8835137__41190759', '3__39854938__39854939', '3__1505851__12422900',
        '3__763130__20274475',
        '3__17879928__17901477', '3__2100771__2108255', '3__9899017__9899018',
        '3__6157249__37683230',
        '3__14297612__14297613', '3__15250253__16558998', '3__7203977__7203979',
        '3__763466__25363417',
        '3__3717135__3801583', '3__10827064__14296325', '3__1889201__8063405', '3__444828__3198659',
        '3__14732182__35191409', '3__49309268__50215664', '3__3495267__32363003',
        '3__14351548__14351549',
        '3__15467782__34403630', '3__22472818__15589944', '3__4899161__5125821',
        '3__6927435__20459965',
        '3__452359__159735', '3__6730579__36582714', '3__20870115__22643874', '3__539422__261793',
        '3__21083268__21083272',
        '3__479320__191616', '3__36701760__36701761', '3__8236378__8236396', '3__12232125__35144722',
        '3__16603297__20735527', '3__340145__20012417', '3__335375__14135487', '3__1513599__1501787',
        '3__764301__17476841', '3__21153000__21153001', '3__3526019__9867778',
        '3__22557467__22557468',
        '3__540574__20489201', '3__2883881__21415381', '3__11220593__26088352',
        '3__8236378__11971178',
        '3__21121756__29151595', '3__18806992__18806996', '3__4146829__11522879',
        '3__817689__822893',
        '3__58900649__58900664', '3__634839__11507758', '3__5913675__21186451',
        '3__36569505__59465144',
        '3__32217636__32217637', '3__15163494__19653798', '3__20855142__20855143',
        '3__11515253__20170877',
        '3__11339977__20484538', '3__6332871__12281319', '3__13442876__21172932',
        '3__23896088__23896089',
        '3__32102906__32102907', '3__27930849__27930850', '3__514987__20426476',
        '3__472232__21295741',
        '3__10827064__14296544', '3__921120__921706', '3__10142107__36156367', '3__435186__12028871',
        '3__802590__813912',
        '3__3906909__37296006', '3__23432493__56416286', '3__2126453__6108449',
        '3__9948015__9948016', '3__861498__864528',
        '3__917982__921980', '3__15147008__21018711', '3__2013223__2014221', '3__492747__206992',
        '3__8123012__31832910',
        '3__13765993__13765994', '3__1938769__1938771', '3__13833466__26254257',
        '3__20524626__20524627',
        '3__1323531__14772909', '3__434489__21297752', '3__15147008__26217161',
        '3__23462316__25000001',
        '3__1611957__1611959', '3__13512590__13512591', '3__33740550__37091294',
        '3__22050601__22050602',
        '3__1082808__2166757', '3__14305539__14305540', '3__593838__4026403',
        '3__23896088__39292565',
        '3__29915107__29915109', '3__1454853__38657701', '3__9724992__63440915',
        '3__33783706__33783707']
    results = multi_task_async(items=[(product_base_id, "Hello") for product_base_id in lst_product_base_id],
                               method=test, debug=False, max_workers=100)
    print(len(results))
    print(f"time execute: {time.time() - time_start}")
