import asyncio
from typing import Union

import aiohttp
import time

from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger


async def _fetch(client, url, data=None):
    async with (client.get(url) if data is None else client.post(url, data)) as resp:
        return await resp.text()


async def _async_get(urls, timeout=300):
    async with aiohttp.ClientSession(timeout=timeout) as client:
        return await asyncio.gather(*(_fetch(client, url) for url in urls))


async def _async_post(urls, data, timeout=300):
    async with aiohttp.ClientSession(timeout=timeout) as client:
        if isinstance(data, list):
            return await asyncio.gather(*(_fetch(client, url, datum) for url, datum in zip(urls, data)))
        else:
            return await asyncio.gather(*(_fetch(client, url, data) for url in urls))


def async_request(urls, method="GET", data=None, timeout=300, debug=False):
    start = time.time()

    result = None
    loop = asyncio.get_event_loop()
    if method == "GET":
        result = loop.run_until_complete(_async_get(urls, timeout))
    elif method == "POST":
        result = loop.run_until_complete(_async_post(urls, data, timeout))

    if debug:
        logger.info(f'Async_{method}: {(int(time.time() - start) * 1000)} ms')

    return result


async def multi_task(items, method, timeout: Union[int, float]):
    results = []
    tasks = []
    for item in items:
        if type(item) == tuple:
            task = method(*item)
        else:
            task = method(item)
        tasks.append(task)
    done, pending = await asyncio.wait(tasks, timeout=timeout)
    for task in done:
        results.append(task.result())
    return results


async def gather_with_concurrency(concurrency, lst_task, timeout: Union[int, float]):
    results = []
    semaphore = asyncio.Semaphore(concurrency)

    async def sem_task(task):
        async with semaphore:
            return await task

    done, pending = await asyncio.wait([sem_task(task) for task in lst_task], timeout=timeout)
    for task in done:
        results.append(task.result())
    return results


async def multi_task_merge_results(lst_task, concurrency=5, ignore_none=True, timeout: Union[int, float] = 380):
    results = []
    # for result in await asyncio.gather(*lst_task, return_exceptions=True):
    for result in await gather_with_concurrency(concurrency, lst_task=lst_task, timeout=timeout):
        if ignore_none and result is None:
            continue
        results.append(result)
    return results


def multi_task_async(items, method, max_workers=20, timeout: Union[int, float] = 380, debug=False):
    if not asyncio.iscoroutinefunction(method):
        raise TypeError("method need async")
    results = []
    for index in range(0, len(items), max_workers):
        results_per_step = asyncio.run(multi_task(items[index: index + max_workers], method, timeout=timeout))
        results.extend(results_per_step)
    return results


if __name__ == "__main__":
    async_request(urls=["https://www.google.com"])
