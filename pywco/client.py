import asyncio
import threading
import time
import websockets
import websockets.exceptions
import logging
import msgpack
import janus
import blinker
from blinker import signal

from .communicator import Communicator

log = logging.getLogger(__name__)

connection_lost = blinker.Signal()
client_stopped = blinker.Signal()
connected = blinker.Signal()


class Client(Communicator):

    async def start_async_communication(self):
        protocol = "wss" if self._ssl else "ws"
        self.websocket = await websockets.connect(f"{protocol}://{self.address}:{self.port}", ssl=self._ssl)
        connected.send(self)
        while not self.stopping:
            self.consumer_task = self.loop.create_task(self.consumer_handler())
            self.producer_task = self.loop.create_task(self.producer_handler())
            done, pending = await asyncio.wait([self.consumer_task, self.producer_task], return_when=asyncio.FIRST_COMPLETED)
            for task in pending:
                task.cancel()

    def send_message(self, command, **message):
        self._add_command_and_verify_message(command, message)
        self.send_queue.sync_q.put(message)

    async def producer_handler(self):
        message = await self.send_queue.async_q.get()
        message_string = msgpack.packb(message, default=self.encode_command, use_bin_type=True)
        try:
            await self.websocket.send(message_string)
            self.send_queue.async_q.task_done()
        except websockets.exceptions.ConnectionClosed as ex:
            self.stop()
            connection_lost.send(self, exception=ex)

    async def consumer_handler(self):
        try:
            async for received_string in self.websocket:
                self.handle_client_message(received_string)
        except websockets.exceptions.ConnectionClosed as ex:
            self.stop()
            connection_lost.send(self, exception=ex)


    def handle_client_message(self, message):
        command, received = self.decode_message(message)
        try:
            signal(command).send(self, **received)
        except Exception as e:
            log.exception(f"Error handling the {command} command.")

    async def _stop_3(self):
        await self.websocket.close(code=1001)
        self.send_queue.close()
        await self.send_queue.wait_closed()
        self.consumer_task.cancel()
        self.producer_task.cancel()

    def _stop_final(self):
        self.communication_thread.join()
        client_stopped.send(self)
