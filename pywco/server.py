import asyncio
import logging
import random
import traceback
import collections

import websockets
import msgpack
import blinker
from blinker import signal

from .communicator import Communicator

log = logging.getLogger(__name__)
client_connected = blinker.Signal()
client_disconnected = blinker.Signal()
server_stopped = blinker.Signal()


class Server(Communicator):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.clients = {}
        self._rng = random.SystemRandom()
        self._cur_client_id = None
        self._client_groups = collections.defaultdict(set)
        self.server = None

    async def start_async_communication(self):
        self.server = await websockets.serve(self.handler, self.address, self.port, loop=self.loop, ssl=self._ssl, timeout=self._timeout)

    async def handler(self, websocket, path):
        pywco_client_id = self._rng.randint(0, 2**64) # Because of the birthday paradox we expect a collision in 2**32 attempts, but that should be fine.
        self.clients[pywco_client_id] = websocket
        self.add_to_clients_group(pywco_client_id, "EVERYONE")
        client_connected.send(self, pywco_client_id=pywco_client_id)
        is_connected = True
        while is_connected and not self.stopping:
            if websocket.closed:
                self.handle_client_disconnect(pywco_client_id, False)
                client_disconnected.send(self, pywco_client_id=pywco_client_id, abnormal=False)
                break
            self.consumer_task = self.loop.create_task(self.consumer_handler(websocket, path, pywco_client_id))
            self.producer_task = self.loop.create_task(self.producer_handler(pywco_client_id))
            try:
                done, pending = await asyncio.wait([self.consumer_task, self.producer_task], return_when=asyncio.FIRST_COMPLETED)
                for future in done:
                    future.result()
            except websockets.ConnectionClosed as exc:
                is_connected = False
                abnormal = exc.code not in {1000, 1001}
                self.handle_client_disconnect(pywco_client_id, abnormal)
                client_disconnected.send(self, pywco_client_id=pywco_client_id, abnormal=abnormal)
            for task in pending:
                task.cancel()

    def answer(self, command, **message):
        self.send_message(command, self.current_client_id, **message)

    def send_message(self, command, pywco_client_id, **message):
        self._add_command_and_verify_message(command, message)
        wrap = Wrap(False, pywco_client_id, message, traceback.format_stack())
        self.send_queue.sync_q.put(wrap)

    def broadcast(self, command, **message):
        self._add_command_and_verify_message(command, message)
        wrap = Wrap("EVERYONE", None, message, traceback.format_stack())
        self.send_queue.sync_q.put(wrap)

    def broadcast_to_group(self, group_name, command, **message):
        self._add_command_and_verify_message(command, message)
        wrap = Wrap(group_name, None, message, traceback.format_stack())
        self.send_queue.sync_q.put(wrap)
    
        
    def add_to_clients_group(self, pywco_client_id, group_name):
        self._client_groups[group_name].add(pywco_client_id)

    def remove_from_clients_group(self, pywco_client_id, group_name):
        self._client_groups[group_name].remove(pywco_client_id)

    def get_clients(self):
        return self.clients

    def get_clients_in_group(self, group_name):
        return self._client_groups[group_name]

    async def send(self, message, pywco_client_id):
        await self.clients[pywco_client_id].send(message)

    async def producer_handler(self, pywco_client_id):
        wrap = await self.send_queue.async_q.get()
        try:
            message_string = msgpack.packb(wrap.message, default=self.encode_command, use_bin_type=True)
        except TypeError as exc:
            log.error("Failed to serialize the message %s, error was %s, call stack at time of message queueing was %s.", wrap.message, exc, "\n".join(wrap.caller_stack))
            self.kick_client(self.current_client_id, 1011)
            return
        if wrap.clients_group:
            await asyncio.wait([self.send(message_string, pywco_client_id) for pywco_client_id in self._client_groups[wrap.clients_group]])
        else:
            await self.send(message_string, wrap.pywco_client_id)

        self.send_queue.async_q.task_done()

    async def consumer_handler(self, websocket, path, pywco_client_id):
        async for received_string in websocket:
            self._cur_client_id = pywco_client_id
            self.handle_server_message(received_string, pywco_client_id)
            self._cur_client_id = None

    def handle_server_message(self, message, pywco_client_id):
        try:
            command, received = self.decode_message(message)
        except Exception as e:
            log.exception("Error decoding an incoming message.")
            return
        received["pywco_client_id"] = pywco_client_id
        try:
            signal(command).send(self, **received)
        except Exception as e:
            log.exception(f"Error handling the {command} command.")

    def handle_client_disconnect(self, pywco_client_id, abnormal):
        del self.clients[pywco_client_id]
        for group in self._client_groups:
            if pywco_client_id in group:
                group.remove(pywco_client_id)
        
    async def _stop_3(self):
        self.server.close()
        await self.server.wait_closed()
        self.send_queue.close()
        await self.send_queue.wait_closed()
        if self.consumer_task is not None: self.consumer_task.cancel()
        if self.producer_task is not None: self.producer_task.cancel()

    def _stop_final(self):
        self.communication_thread.join()
        server_stopped.send(self)

    @property
    def current_client_id(self):
        """The client id corresponding to the currently processed message, e. g. client id of the sender."""
        return self._cur_client_id

    async def kick_client_async(self, pywco_client_id, reason=""):
        # Note that the client gets deleted by the normal client disconnection logic.
        await self.clients[pywco_client_id].close(1008, reason or "You have been kicked out.")

    def kick_sender(self, reason=""):
        self.kick_client(self.current_client_id, reason)

    def kick_client(self, pywco_client_id, reason=""):
        self.loop.create_task(self.kick_client_async(pywco_client_id, reason))

class Wrap:
    
    def __init__(self, clients_group, pywco_client_id, message, caller_stack):
        self.clients_group = clients_group
        self.pywco_client_id = pywco_client_id
        self.message = message
        self.caller_stack = caller_stack
