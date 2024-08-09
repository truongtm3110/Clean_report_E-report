import asyncio
import dataclasses
import io
import json as _json
import os
import random
import time
from datetime import datetime, timedelta
from typing import Dict, Union, Optional, List
from urllib.parse import urlparse

import aiohttp
import fastavro
import requests
from async_lru import alru_cache
from dataclasses_avroschema import AvroModel, Int32
from furl import furl
# from kafka import KafkaProducer

from helper.QueueRPCMessage_pb2 import QueueRequestBodyMessage, MapFieldEntry
from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple
from helper.project_helper import get_folder_project_root_path
from helper.queue_rpc_async_helper import QueueRpcClientAsync
from helper.reader_helper import read_by_lines
from helper.url_helper import encode_params, add_params

logger = LoggerSimple(name=__name__).logger


def get_list_proxy():
    url = "https://proxy.webshare.io/api/v2/proxy/list/download/dnqkqxknogvldfjbrlqbifxybhygfxbscpltshxy/-/any/username/direct/-/"
    lst_proxy = []
    list_proxy_file = os.path.join(get_folder_project_root_path(), "data/proxy/web_share_proxy_temp.txt")
    if os.path.exists(list_proxy_file):
        mtime = os.path.getmtime(list_proxy_file)
        last_mod_time = datetime.fromtimestamp(mtime)
        if datetime.now() - last_mod_time < timedelta(minutes=120):
            for line in read_by_lines(list_proxy_file, is_gzip=False, is_json=False):
                lst_proxy.append(line)
            logger.info(f"Gotten {len(lst_proxy)} from local file, last modified time: {last_mod_time}")
            return lst_proxy
    try:
        response = requests.get(url, timeout=30)
        for line in response.text.splitlines():
            proxy_infos = line.split(":")
            if len(proxy_infos) == 4:
                ip, port, username, password = proxy_infos
                lst_proxy.append(f"http://{username}:{password}@{ip}:{port}")
    except Exception as e:
        log_error(e)
    logger.info(f"Gotten {len(lst_proxy)} http proxies")
    if len(lst_proxy) > 0:
        with open(list_proxy_file, "wt") as f:
            f.write("\n".join(lst_proxy))
    return lst_proxy


@dataclasses.dataclass
class HttpDetail(AvroModel):
    content: Optional[bytes]
    headers: Optional[Dict[str, str]]
    cookies: Optional[Dict[str, str]]
    status_code: Optional[Int32] = None
    body_size: Optional[int] = 0

    class Meta:
        namespace = "HttpDetail.v1"
        aliases = ["http_detail"]


@dataclasses.dataclass
class RequestData(AvroModel):
    method: str
    full_url: str
    endpoint: str
    domain: str
    request_detail: HttpDetail
    response_detail: HttpDetail
    timeout: Optional[Int32]
    error: bool
    crawled_at: int
    elapsed_time_ms: Int32
    client_id: Optional[str] = None

    class Meta:
        namespace = "RequestData.v1"
        aliases = ["request_data"]


parsed_schema = RequestData.generate_schema()

REQUEST_RAW_DATA_TOPIC = "COLLECT_RAW_DATA_TOPIC"


class Response:
    def __init__(self, status_code: int = None, text: str = None, content: bytes = None, cookies: Dict[str, str] = None,
                 headers: Dict[str, str] = None):
        self.status_code: int = status_code
        self.text: str = text
        self.content: bytes = content
        self.cookies: Dict[str, str] = cookies
        self.headers: Dict[str, str] = headers

    def json(self) -> Optional[dict]:
        return _json.loads(self.text)


# kafka_username, kafka_password = get_value_setting(key='KAFKA_USER'), get_value_setting(key='KAFKA_PASSWORD')
# kafka_lst_broker_servers = get_value_setting(key="KAFKA_BROKER_SERVERS")
# kafka_producer = KafkaProducer(
#     bootstrap_servers=kafka_lst_broker_servers,
#     security_protocol="SASL_PLAINTEXT",
#     sasl_plain_username=kafka_username,
#     sasl_plain_password=kafka_password,
#     sasl_mechanism="SCRAM-SHA-512",
#     acks='all',
#     compression_type="gzip",
#     linger_ms=100,
#     max_request_size=5 * 1024 * 1024,
# )


class BeeHttpClient:
    def __init__(self, *args, **kwargs):
        self.save_raw = kwargs.get('save_raw', False)
        logger.info(f"Create bee http client save raw: {self.save_raw}")

    def set_save_raw(self, save_raw: bool):
        self.save_raw = save_raw

    @staticmethod
    def get_request_body(data: Union[bytes, str] = None,
                         json=None) -> bytes:
        body = None
        if not data and json is not None:
            body = _json.dumps(json)
        elif data is not None:
            body = encode_params(data)
        if isinstance(body, str):
            body = body.encode()
        return body

    def get_request_data(self,
                         start_time: float,
                         end_time: float,
                         method: str, url: str, params: Dict[str, str] = None,
                         headers: Dict[str, str] = None,
                         cookies: Dict[str, str] = None,
                         timeout: int = 15,
                         data: Union[bytes, str] = None,
                         json=None,
                         response: Response = None,
                         error: bool = False
                         ) -> bytes:
        if response:
            response_detail = {
                "content": response.content,
                "headers": response.headers,
                "cookies": response.cookies,
                "status_code": response.status_code,
                "body_size": len(response.content) if response.content else 0
            }
        else:
            response_detail = {
                "content": None,
                "headers": None,
                "cookies": None,
                "status_code": None
            }
        full_url = add_params(url, params)
        url_parsed = urlparse(full_url)
        request_body = self.get_request_body(data=data, json=json)
        obj = {
            "crawled_at": int(end_time),
            "elapsed_time_ms": int((end_time - start_time) * 1000),
            "method": method,
            "full_url": full_url,
            "endpoint": url_parsed.path,
            "domain": url_parsed.netloc.replace('www.', ''),
            "request_detail": {
                "content": request_body,
                "headers": headers,
                "cookies": cookies,
                "body_size": len(request_body) if request_body else 0
            },
            "response_detail": response_detail,
            "timeout": timeout,
            "error": error
        }
        f = io.BytesIO()
        fastavro.schemaless_writer(f, parsed_schema, obj)
        data = f.getvalue()
        f.close()
        return data

    async def _request(self, method: str, url: str, params: Dict[str, str] = None, headers: Dict[str, str] = None,
                       cookies: Dict[str, str] = None,
                       timeout: int = 15, data: Union[bytes, str] = None, json=None, **kwargs) -> Response:
        error, response = None, None
        start_time = time.time()
        try:
            response = await self.request(
                method=method, url=url, params=params, headers=headers, cookies=cookies,
                timeout=timeout,
                data=data, json=json, **kwargs
            )
        except Exception as e:
            error = e
        end_time = time.time()
        if self.save_raw and kwargs.get('save_raw', True):
            try:
                data = self.get_request_data(start_time=start_time, end_time=end_time, method=method, url=url,
                                             params=params, headers=headers,
                                             cookies=cookies,
                                             timeout=timeout,
                                             data=data, json=json, response=response, error=True if error else False)
                # kafka_producer.send(REQUEST_RAW_DATA_TOPIC, data)
            except Exception as e:
                log_error(e)
        if error:
            raise error
        return response

    async def request(self, method: str, url: str, params: Dict[str, str] = None, headers: Dict[str, str] = None,
                      cookies: Dict[str, str] = None,
                      timeout: int = 15, data: Union[bytes, str] = None, json=None, **kwargs) -> Response:
        pass

    async def get(self, url: str, params: Dict[str, str] = None, headers: Dict[str, str] = None,
                  cookies: Dict[str, str] = None,
                  timeout: int = 15, **kwargs) -> Response:
        return await self._request(method="GET", url=url, params=params, headers=headers, cookies=cookies,
                                   timeout=timeout, **kwargs)

    async def post(self, url: str, params: Dict[str, str] = None, headers: Dict[str, str] = None,
                   cookies: Dict[str, str] = None,
                   timeout: int = 15, data: Union[bytes, str] = None, json: dict = None, **kwargs) -> Response:
        return await self._request(method="POST", url=url, params=params, headers=headers, cookies=cookies,
                                   timeout=timeout,
                                   data=data, json=json, **kwargs)


class ProxyHttpClient(BeeHttpClient):
    def __init__(self, lst_proxy: Optional[List[str]] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if lst_proxy is None:
            self.lst_proxy = []
        else:
            self.lst_proxy: List[str] = lst_proxy

    async def request(self, method: str, url: str, params: Dict[str, str] = None, headers: Dict[str, str] = None,
                      cookies: Dict[str, str] = None,
                      timeout: int = 15, data: Union[bytes, str] = None, json=None, **kwargs) -> Response:
        async with aiohttp.ClientSession() as session:
            proxy = None
            if len(self.lst_proxy) > 0:
                proxy = random.choice(self.lst_proxy)
            try:
                request_response = await session.request(
                    url=url,
                    params=params,
                    method=method,
                    headers=headers,
                    cookies=cookies,
                    timeout=timeout,
                    data=data,
                    json=json,
                    proxy=proxy,
                    verify_ssl=False
                )
                res_headers = {}
                for key, value in request_response.raw_headers:
                    key = key.decode()
                    value = value.decode()
                    if key is not None and not key.lower().strip() == "set-cookie":
                        res_headers[key] = value
                cookies: Dict[str, str] = {}
                for key, value in request_response.cookies.items():
                    cookies[key] = value.value
                response_content = await request_response.read()
                try:
                    response_text = response_content.decode("utf-8")
                except UnicodeDecodeError:
                    response_text = None
                response = Response(
                    status_code=request_response.status.real,
                    headers=res_headers,
                    cookies=cookies,
                    content=response_content,
                    text=response_text
                )
            except Exception as e:
                log_error(e)
                response = Response(status_code=0, content=None, text=None)
        return response


class GCafeHttpClient(BeeHttpClient):

    async def request(self, method: str, url: str, params: Dict[str, str] = None, headers: Dict[str, str] = None,
                      cookies: Dict[str, str] = None,
                      timeout: int = 15, data: Union[bytes, str] = None, json=None, **kwargs) -> Response:
        retry_cnt = 0
        max_retry = kwargs.get("max_retry")
        if not isinstance(max_retry, int):
            max_retry = 2
        response = Response(status_code=0, content=None, text=None)
        while retry_cnt < max_retry:
            data = bytes([]) if data is None else data
            headers = {} if headers is None else headers
            params = {} if params is None else params
            cookies = {} if cookies is None else cookies
            timeout = 15 if timeout is None or timeout <= 0 else timeout

            request_body = QueueRequestBodyMessage()
            request_body.method = method
            if method.lower() == "post":
                body = None
                if not data and json is not None:
                    body = _json.dumps(json)
                    header = MapFieldEntry()
                    header.key = "Content-Type"
                    header.value = "application/json"
                    request_body.headers.append(header)
                elif data is not None:
                    body = encode_params(data)
                    if not isinstance(data, (str, bytes)) or hasattr(data, 'read'):
                        header = MapFieldEntry()
                        header.key = "Content-Type"
                        header.value = "application/x-www-form-urlencoded"
                        request_body.headers.append(header)
                if isinstance(body, str):
                    body = body.encode()
                if isinstance(body, bytes):
                    request_body.body_bytes = body
            for key in headers.keys():
                header = MapFieldEntry()
                header.key = key
                header.value = headers.get(key, None)
                request_body.headers.append(header)
            request_body.full_url = furl(url).add(params).url
            for key in cookies.keys():
                request_body.cookies[key] = cookies.get(key)
            request_body.timeout = timeout
            client = None
            try:
                client = await asyncio.wait_for(QueueRpcClientAsync().connect(
                    rabbitmq_url="amqp://rpc_producer:SenSen1122_@45.122.220.8:5672/"
                ), timeout=30)
                gcafe_response = await asyncio.wait_for(
                    client.call(request_body), timeout=request_body.timeout + 5
                )
                await client.close()
                res_headers = {}
                for header in gcafe_response.headers:
                    if header.key is not None and not header.key.lower().strip() == "set-cookie":
                        res_headers[header.key] = header.value
                try:
                    response_text = gcafe_response.body_bytes.decode("utf-8")
                except UnicodeDecodeError:
                    response_text = None
                response = Response(status_code=gcafe_response.status_code, headers=res_headers,
                                    cookies=gcafe_response.cookies,
                                    content=gcafe_response.body_bytes, text=response_text)
            except ConnectionResetError:
                response = Response(status_code=0, content=None, text=None)
            except Exception as e:
                log_error(e)
                response = Response(status_code=0, content=None, text=None)
            finally:
                if client is not None:
                    await client.close()
            if response.status_code == 0:
                retry_cnt += 1
                continue
            else:
                return response
        return response


class AutoSelectHTTPClient(BeeHttpClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.proxy_client = ProxyHttpClient(
            lst_proxy=get_list_proxy()
        )
        self.gcafe_client = GCafeHttpClient()
        self.ping_url = "https://api.ipify.org"
        self.error_cnt = 0
        self.last_get_client = None

    @classmethod
    def get_ttl_hash(cls, seconds=3600):
        """Return the same value withing `seconds` time period"""
        return round(time.time() / seconds)

    @alru_cache()
    async def get_client(self, ttl_hash=None) -> BeeHttpClient:
        del ttl_hash
        client = None
        try:
            response = await self.gcafe_client.get(self.ping_url, timeout=10, save_raw=False)
            if response.status_code == 200:
                client = self.gcafe_client
        except Exception as e:
            log_error(e)
        if client is None:
            client = ProxyHttpClient(
                lst_proxy=get_list_proxy()
            )
        logger.info(f"Gotten http client: {client.__class__.__name__}")
        self.error_cnt = 0
        self.last_get_client = datetime.now()
        return client

    async def request(self, method: str, url: str, params: Dict[str, str] = None, headers: Dict[str, str] = None,
                      cookies: Dict[str, str] = None,
                      timeout: int = 15, data: Union[bytes, str] = None, json=None, **kwargs) -> Response:
        http_client = await self.get_client(ttl_hash=self.get_ttl_hash(seconds=15 * 60))
        response = await http_client.request(method, url, params, headers, cookies, timeout, data, json, **kwargs)
        if response.status_code == 0:
            self.error_cnt += 1
            if self.error_cnt > 5:
                if datetime.now() - self.last_get_client > timedelta(minutes=5):
                    self.get_client.cache_clear()
        else:
            self.error_cnt = 0
        return response


def get_http_agent(save_raw=False, use_proxy=True, lst_proxy: List[str] = None) -> BeeHttpClient:
    if use_proxy and lst_proxy is None:
        logger.info("Using default proxy list")
        lst_proxy = get_list_proxy()
    return ProxyHttpClient(
        lst_proxy=lst_proxy,
        save_raw=save_raw
    )


def get_bee_requests(save_raw=False) -> BeeHttpClient:
    if isinstance(os.environ.get("save_raw"), str) and os.environ.get("save_raw").strip().lower() in {"true", "false"}:
        return AutoSelectHTTPClient(save_raw=os.environ.get("save_raw").strip().lower() == "true")
    return AutoSelectHTTPClient(save_raw=save_raw)


async def main():
    os.environ['save_raw'] = "false"
    bee_requests = get_bee_requests(save_raw=False)
    while True:
        response = await bee_requests.get("https://api.ipify.org", timeout=10, save_raw=False)
        print(response.text)
    # ip_set = set()
    # from helper.multi_thread_new import run_multi_task_async
    # from helper.reader_helper import store_lines_perline_in_file
    # results = await run_multi_task_async(
    #     items=["https://api.ipify.org" for _ in range(0, 10)], method=bee_requests.get,
    #     max_workers=300,
    #     method_timeout=30
    # )
    # for result in results:
    #     if result is not None:
    #         print(result.text)
    #         ip_set.add(result.text)
    # store_lines_perline_in_file(
    #     lines=list(ip_set),
    #     file_output_path="/Users/obito/Downloads/ip.txt"
    # )


if __name__ == '__main__':
    asyncio.run(main())
