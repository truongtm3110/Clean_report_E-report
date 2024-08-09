import asyncio
import random
from typing import Union, List

from helper.error_helper import log_error
from helper.logger_helper import LoggerSimple

logger = LoggerSimple(name=__name__).logger


async def worker(name, method, queue: asyncio.Queue, result_queue: asyncio.Queue, timeout: Union[int, float, None] = 10,
                 debug=False):
    while True:
        try:
            item = queue.get_nowait()
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
            await result_queue.put(result)
        except asyncio.TimeoutError as e:
            if debug:
                log_error(e)
        except Exception as e:
            log_error(e)
        queue.task_done()
        if debug:
            logger.info(f'{name} executed {method.__name__} {item}')
    if debug:
        logger.info(f"Exit worker: {name}")


async def run_multi_task_async(items, method, max_workers=20, method_timeout: Union[int, float, None] = 15,
                               debug=False):
    if not asyncio.iscoroutinefunction(method):
        raise TypeError("method need async")
    task_queue = asyncio.Queue()
    result_task_queue = asyncio.Queue()

    for item in items:
        task_queue.put_nowait(item)

    tasks = []
    for i in range(max_workers):
        task = asyncio.create_task(
            worker(name=f'Worker-{i}', method=method, queue=task_queue, result_queue=result_task_queue,
                   timeout=method_timeout, debug=debug)
        )
        tasks.append(task)

    try:
        await asyncio.gather(*tasks, return_exceptions=False)
    except Exception as e:
        if debug:
            log_error(e)
    await task_queue.join()

    # Cancel our worker tasks.
    for task in tasks:
        task.cancel()
        # Wait until all worker tasks are cancelled.

    results = []
    while True:
        try:
            result = result_task_queue.get_nowait()
            results.append(result)
            result_task_queue.task_done()
        except asyncio.QueueEmpty:
            break
    return results


def multi_task_async(items, method, max_workers=20, method_timeout: Union[int, float, None] = 15, debug=False) -> List:
    if len(items) < max_workers:
        max_workers = len(items)
    return asyncio.run(run_multi_task_async(items, method, max_workers, method_timeout, debug=debug))


if __name__ == '__main__':
    from helper import gcafe_request_async as requests
    from datetime import datetime


    async def get_product_base(product_base_id):
        response = await requests.get(f"https://apiv2.beecost.vn/product/detail?product_base_id={product_base_id}")
        print(response.text)
        return response.text


    lst_product_base_id = ['3__8835137__41190759', '3__39854938__39854939', '3__1505851__12422900',
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
    start_time = datetime.now()
    test = multi_task_async(items=[{"product_base_id": random.choice(lst_product_base_id)} for i in range(1000)],
                            method=get_product_base,
                            max_workers=100, method_timeout=15, debug=True)
    logger.info(f"Total time execute: {datetime.now() - start_time}")
    logger.info(len(test))
    if len(test) > 0:
        logger.info(test[0])
