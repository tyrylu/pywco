import asyncio
import logging
import websockets
import msgpack
import blinker

from .communicator import Communicator

log = logging.getLogger(__name__)
client_connected = blinker.Signal()
client_disconnected = blinker.Signal()


class Server(Communicator):
    
    def __init__(self, address, port, known_commands):
        super().__init__(address, port, known_commands)
        self.clients = {}
        self._next_client_id = 0

    def start_communication(self):
        start_server = websockets.serve(self.handler, self.address, self.port, loop=self.loop)
        self.loop.run_until_complete(start_server)
        self.loop.run_forever()

    async def handler(self, websocket, path):
        client_id = self._next_client_id
        self.clients[client_id] = websocket
        self._next_client_id += 1
        client_connected.send(self, client_id=client_id)
        is_connected = True
        while is_connected:
            if websocket.closed:
                self.handle_client_disconnect(client_id, False)
                client_disconnected.send(self, client_id=client_id, abnormal=False)
                break
            consumer_task = self.loop.create_task(self.consumer_handler(websocket, path, client_id))
            producer_task = self.loop.create_task(self.producer_handler(client_id))
            try:
                done, pending = await asyncio.wait([consumer_task, producer_task], return_when=asyncio.FIRST_COMPLETED)
                for future in done:
                    future.result()
            except websockets.ConnectionClosed as exc:
                is_connected = False
                abnormal = exc.code not in {1000, 1001}
                self.handle_client_disconnect(client_id, abnormal)
                client_disconnected.send(self, client_id=client_id, abnormal=abnormal)
            for task in pending:
                task.cancel()

    def answer(self, command, message={}):
        message["command"] = command
        wrap = Wrap(False, self._cur_client_id, message)
        self.send_queue.sync_q.put(wrap)

    def send_message(self, command, client_id, message={}):
        message["command"] = command
        wrap = Wrap(False, client_id, message)
        self.send_queue.sync_q.put(wrap)

    def broadcast(self, command, message={}):
        message["command"] = command
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

    def handle_client_disconnect(self, client_id, abnormal):
        del self.clients[client_id]

    @property
    def current_client_id(self):
        """The client id corresponding to the currently processed message, e. g. client id of the sender."""
        return self._cur_client_id


class Wrap:
    
    def __init__(self, broadcast, client_id, message):
        self.broadcast = broadcast
        self.client_id = client_id
        self.message = message
