import asyncio
import threading
import time
import websockets
import logging
import msgpack
import janus

from .communicator import Communicator

log = logging.getLogger(__name__)
class Client(Communicator):
   
    def start_communication(self):
        self.stopping = False
        self.communication_task = self.loop.create_task(self.start_async_communication())
        self.loop.run_forever()
    
    async def start_async_communication(self):
        self.websocket = await websockets.connect(f"ws://{self.address}:{self.port}")
        while not self.stopping:
            self.consumer_task = self.loop.create_task(self.consumer_handler())
            self.producer_task = self.loop.create_task(self.producer_handler())
            done, pending = await asyncio.wait([self.consumer_task, self.producer_task], return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()

    def send_message(self, command, message={}):
        message["command"] = command
        self.send_queue.sync_q.put(message)

    async def producer_handler(self):
        message = await self.send_queue.async_q.get()
        message_string = msgpack.packb(message, default=self.encode_command, use_bin_type=True)
        await self.websocket.send(message_string)        
        self.send_queue.async_q.task_done()

    async def consumer_handler(self):
        async for received_string in self.websocket:
            self.handle_message(received_string)

    def stop(self):
        self.loop.call_soon_threadsafe(self.do_stop)

    def do_stop(self):
        self.stopping = True
        self.loop.create_task(self.stop_async()).add_done_callback(self.stop_phase_2)
        
    def stop_phase_2(self, result):
        self.producer_task.cancel()
        self.consumer_task.cancel()
        self.loop.stop()

    async def stop_async(self):
        await self.websocket.close()
        self.send_queue.close()
        await self.send_queue.wait_closed()
