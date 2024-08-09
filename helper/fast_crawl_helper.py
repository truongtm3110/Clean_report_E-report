import asyncio
import random
from typing import Union, List

from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple
from helper.reader_helper import store_jsons_perline_in_file

logger = LoggerSimple(name=__name__).logger


class CrawlerWorker:
    def __init__(self, worker_id, input_queue: asyncio.Queue, output_queue: asyncio.Queue):
        self.worker_id = worker_id
        self.input_queue = input_queue
        self.output_queue = output_queue

    async def run_crawl(self, method, timeout: Union[int, float, None] = 10, debug=False):
        while True:
            try:
                item = await self.input_queue.get()
            except asyncio.QueueEmpty:
                break
            try:
                if type(item) == tuple:
                    task = method(*item)
                elif type(item) == dict:
                    task = method(**item)
                else:
                    task = method(item)
                result = await asyncio.wait_for(task, timeout=timeout)
                await self.output_queue.put(result)
            except asyncio.TimeoutError as e:
                if debug:
                    log_error(e)
            except Exception as e:
                log_error(e)
            self.input_queue.task_done()
            if debug:
                logger.info(f'{self.worker_id} executed {method.__name__} {item}')
        if debug:
            logger.info(f"Exit worker: {self.worker_id}")


class ConsumerWorker:
    def __init__(self, output_file: str, output_queue: asyncio.Queue = None, total_input=0):
        self.output_file = output_file
        self.output_queue = output_queue
        self.total_input = total_input
        self.lst_result = []

    async def de_queue(self, batch_size=1000):
        while True:
            try:
                result = await self.output_queue.get()
                if result is not None and type(result) == dict:
                    self.lst_result.append(result)
                    if len(self.lst_result) >= batch_size:
                        self.store_result_to_file()
                self.output_queue.task_done()
            except asyncio.QueueEmpty:
                break

    def store_result_to_file(self):
        logger.info(f"Store {len(self.lst_result)} results to file -> {self.output_file}")
        store_jsons_perline_in_file(self.lst_result, file_output_path=self.output_file, is_append=True)
        self.lst_result.clear()


async def run_multi_task_async(items, method, consumer_worker: ConsumerWorker, max_workers=20,
                               method_timeout: Union[int, float, None] = 15,
                               debug=False):
    if not asyncio.iscoroutinefunction(method):
        raise TypeError("method need async")
    input_queue = asyncio.Queue()
    output_queue = asyncio.Queue()

    consumer_worker.output_queue = output_queue

    for item in items:
        input_queue.put_nowait(item)

    tasks = []
    for worker_index in range(max_workers):
        worker = CrawlerWorker(worker_id=f"Worker-{worker_index}", input_queue=input_queue, output_queue=output_queue)
        task = asyncio.create_task(
            worker.run_crawl(method=method, timeout=method_timeout, debug=debug)
        )
        tasks.append(task)

    consumer_task = asyncio.create_task(
        consumer_worker.de_queue()
    )

    tasks.append(consumer_task)

    await asyncio.gather(*tasks)

    await input_queue.join()
    await output_queue.join()

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()
        # Wait until all worker tasks are cancelled.

    if len(consumer_worker.lst_result) > 0:
        consumer_worker.store_result_to_file()
    return None


def multi_task_async_store_result(items, method, consumer_worker: ConsumerWorker, max_workers=20,
                                  method_timeout: Union[int, float, None] = 15,
                                  debug=False):
    if len(items) < max_workers:
        max_workers = len(items)
    asyncio.run(run_multi_task_async(items, method, consumer_worker, max_workers, method_timeout, debug=debug))


async def test_method(product_base_id):
    await asyncio.sleep(0.02)
    return {
        "product_base_id": product_base_id,
        "random": random.randint(0, 1000)
    }


if __name__ == '__main__':
    lst_item = []
    for i in range(50_000):
        lst_item.append(f"1__{i}")
    c_worker = ConsumerWorker(output_file="/storage/data/temp/hello.json.gz")
    multi_task_async_store_result(lst_item, method=test_method, consumer_worker=c_worker,
                                  method_timeout=10, max_workers=100,
                                  debug=False)
