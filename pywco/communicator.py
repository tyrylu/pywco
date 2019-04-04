from enum import Enum
import msgpack
import asyncio
import threading
import abc
import logging
import janus
from blinker import signal


log = logging.getLogger(__name__)


class Communicator:

    def __init__(self, address, port, known_commands, ssl=None, timeout=10):
        log.debug(f"Instancing pywco with address {address}:{port}")
        self.stopping = False
        self.address = address
        self.port = port
        self._ssl = ssl
        self._timeout = timeout
        self.producer_task = None
        self.consumer_task = None

        if issubclass(known_commands, Enum):
            self.known_commands = known_commands
        else:
            raise TypeError(
                "known_commands needs to be an enum")

        self.loop = asyncio.new_event_loop()
        self.send_queue = janus.Queue(loop=self.loop)
        self.communication_thread = threading.Thread(target=self.start_communication)

    def start(self):
        self.communication_thread.start()
        return self

    def start_communication(self):
        self.communication_task = self.loop.create_task(self.start_async_communication())
        self.loop.run_forever()

    def join(self):
        self.communication_thread.join()

    def encode_command(self, value):
        return value.value

    def decode_message(self, message):
        received = msgpack.unpackb(message, encoding="utf-8")
        command = self.known_commands(received["pywco_command"])
        if command not in self.known_commands:
            log.warning(f"This command is not known {command}")
            return
        del received["pywco_command"]
        return command, received

    def _add_command_and_verify_message(self, command, message):
        for key in message.keys():
            if key.startswith("pywco_"):
                raise AttributeError(f"{key} and the whole pywco prefix is reserved. Please use a different name.")

        message["pywco_command"] = command
        return message

    def stop(self):
        self.loop.call_soon_threadsafe(self._stop_2)

    def _stop_2(self):
        self.stopping = True
        self.loop.create_task(self._stop_3()).add_done_callback(self._stop_4)

    @abc.abstractmethod
    def _stop_3(self): pass

    def _stop_4(self, result):
        self.loop.stop()
        thread = threading.Thread(target=self._stop_final)
        thread.start()

    @abc.abstractmethod
    def _stop_final(self): pass

    @abc.abstractmethod
    async def start_async_communication(self): pass

    @abc.abstractmethod
    async def producer_handler(self): pass

    @abc.abstractmethod
    async def consumer_handler(self): pass


"""def command_handler(react_to):
    def wrap(func):
        current_enum_type = type(react_to)
        if Communicator.enum_type == None:
            Communicator.enum_type = current_enum_type
        elif Communicator.enum_type != current_enum_type:
            raise AssertionError(f"Expected enum type {Communicator.enum_type} but {current_enum_type} was given")
        Communicator.handlers_to_register[react_to] = func
        return func
    return wrap"""