import asyncio
import logging
import random
import threading

import websockets
import msgpack
import blinker

from .communicator import Communicator

log = logging.getLogger(__name__)
client_connected = blinker.Signal()
client_disconnected = blinker.Signal()
server_stopped = blinker.Signal()


class Server(Communicator):
    
    def __init__(self, address, port, known_commands, ssl=None):
        super().__init__(address, port, known_commands, ssl)
        self.clients = {}
        self._rng = random.SystemRandom()
        self._cur_client_id = None
        self.server = None

    async def start_async_communication(self):
        self.server = await websockets.serve(self.handler, self.address, self.port, loop=self.loop, ssl=self._ssl)

    async def handler(self, websocket, path):
        client_id = self._rng.randint(0, 2**64) # Because of the birthday paradox we expect a collision in 2**32 attempts, but that should be fine.
        self.clients[client_id] = websocket
        client_connected.send(self, client_id=client_id)
        is_connected = True
        while is_connected and not self.stopping:
            if websocket.closed:
                self.handle_client_disconnect(client_id, False)
                client_disconnected.send(self, client_id=client_id, abnormal=False)
                break
            self.consumer_task = self.loop.create_task(self.consumer_handler(websocket, path, client_id))
            self.producer_task = self.loop.create_task(self.producer_handler(client_id))
            try:
                done, pending = await asyncio.wait([self.consumer_task, self.producer_task], return_when=asyncio.FIRST_COMPLETED)
                for future in done:
                    future.result()
            except websockets.ConnectionClosed as exc:
                is_connected = False
                abnormal = exc.code not in {1000, 1001}
                self.handle_client_disconnect(client_id, abnormal)
                client_disconnected.send(self, client_id=client_id, abnormal=abnormal)
            for task in pending:
                task.cancel()

    def answer(self, command, **message):
        self._add_command_to_message(command, message)
        wrap = Wrap(False, self._cur_client_id, message)
        self.send_queue.sync_q.put(wrap)

    def send_message(self, command, client_id, **message):
        self._add_command_to_message(command, message)
        wrap = Wrap(False, client_id, message)
        self.send_queue.sync_q.put(wrap)

    def broadcast(self, command, **message):
        self._add_command_to_message(command, message)
        wrap = Wrap(True, None, message)
        self.send_queue.sync_q.put(wrap)

    def get_clients(self):
        return self.clients

    async def send(self, message, client_id):
        await self.clients[client_id].send(message)

    async def producer_handler(self, client_id):
        wrap = await self.send_queue.async_q.get()
        message_string = msgpack.packb(wrap.message, default=self.encode_command, use_bin_type=True)
        if wrap.broadcast:
            await asyncio.wait([self.send(message_string, client_id) for client_id in self.clients.keys()])
        else:
            await self.send(message_string, wrap.client_id)
      
        self.send_queue.async_q.task_done()

    async def consumer_handler(self, websocket, path, client_id):
        async for received_string in websocket:
            self._cur_client_id = client_id
            self.handle_message(received_string)
            self._cur_client_id = None

    def handle_client_disconnect(self, client_id, abnormal):
        del self.clients[client_id]

    async def _stop_3(self):
        self.server.close()
        await self.server.wait_closed()
        self.send_queue.close()
        await self.send_queue.wait_closed()

    def _stop_final(self):
        self.communication_thread.join()
        server_stopped.send(self)

    @property
    def current_client_id(self):
        """The client id corresponding to the currently processed message, e. g. client id of the sender."""
        return self._cur_client_id

    async def kick_client_async(self, client_id, reason=""):
        await self.clients[client_id].close(1008, reason or "You have been kicked out.")
        del self.clients[client_id]

    def kick_client(self, client_id, reason=""):
        self.loop.create_task(self.kick_client_async(client_id, reason))

class Wrap:
    
    def __init__(self, broadcast, client_id, message):
        self.broadcast = broadcast
        self.client_id = client_id
        self.message = message
