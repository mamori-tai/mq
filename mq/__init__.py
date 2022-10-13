# import asyncio
# import concurrent.futures
# import contextlib
# import datetime
# import multiprocessing.pool
# import os
# import pickle
# from enum import Enum
# from multiprocessing import Process
# from typing import Any, Callable, Coroutine, Dict, Optional
#
# import bson
# from bson import ObjectId
# from loguru import logger
#
# # noinspection PyProtectedMember
# from motor.motor_asyncio import (
#     AsyncIOMotorCollection,
#     AsyncIOMotorCursor,
#     AsyncIOMotorClient,
# )
# from pydantic import BaseModel, Field
# from pymongo import ASCENDING
# from pymongo.errors import CollectionInvalid
#
#
#
# async def dequeue_coro(*, mongo_uri, db_name, collection):
#     q = AsyncIOMotorClient(mongo_uri)[db_name][collection]
#     logger.debug(f"Started dequeuing process {os.getpid()} on {q.name!r}")
#     while True:
#         #cursor: AsyncIOMotorCursor = q.find({Job.STATUS: JobStatus.WAITING})
#         try:
#             #current_job = Job(**(await cursor.next()))
#             # noinspection PyProtectedMember
#             logger.debug("Treating row {}", current_job.id)
#
#             await current_job.pending(q)
#
#             loads = pickle.loads
#             f, args, kwargs = loads(current_job.f)
#
#             with concurrent.futures.ThreadPoolExecutor() as executor:
#                 future = executor.submit(f, *args, **kwargs)
#                 result = future.result()
#                 if asyncio.iscoroutine(result):
#                     try:
#                         result = await result
#                     except Exception as e:
#                         logger.exception(e)
#                         await current_job.on_error(q)
#                     else:
#                         await current_job.finished(result, q)
#         except (StopAsyncIteration, StopIteration):
#             pass
#         except Exception as e:
#             logger.exception(e)
#         await asyncio.sleep(5)
#
#
# def syncify(coro, *args, **kwargs):
#     loop = asyncio.new_event_loop()
#     asyncio.set_event_loop(loop)
#     try:
#         loop.create_task(coro(*args, **kwargs), name="dequeuing")
#         loop.run_forever()
#     except Exception as e:
#         logger.exception(e)
#         try:
#             tasks = asyncio.gather(*asyncio.all_tasks(loop), return_exceptions=True)
#             tasks.cancel()
#             with contextlib.suppress(asyncio.CancelledError):
#                 loop.run_until_complete(tasks)
#             loop.run_until_complete(loop.shutdown_asyncgens())
#         finally:
#             loop.close()
#         raise e
#
#
# async def _monitor_dequeuing_process_thread(p: Process) -> None:
#     logger.debug("launching thread monitor on main process {}", os.getpid())
#     while 1:
#         if not p.is_alive():
#             logger.debug("process {} is not alive anymore !", p.pid)
#             p.close()
#             break
#         await asyncio.sleep(10)
#     logger.debug("Ending monitor thread...")
#
#
# async def _dequeue_process(*, mongo_uri, db_name, collection):
#     logger.debug(f"main process {os.getpid()=}")
#     while 1:
#         p = multiprocessing.Process(
#             target=syncify,
#             args=(dequeue_coro,),
#             kwargs=dict(mongo_uri=mongo_uri, db_name=db_name, collection=collection),
#         )
#         p.start()
#         maybe_coro = await asyncio.to_thread(_monitor_dequeuing_process_thread, p)
#         assert asyncio.iscoroutine(maybe_coro)
#         await maybe_coro
#         await asyncio.sleep(1)
#
#
# def dequeue_process_from_job_queue(*, mongo_uri, db_name, collection):
#     asyncio.create_task(
#         _dequeue_process(mongo_uri=mongo_uri, db_name=db_name, collection=collection)
#     )
