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

    instance = None

    @classmethod
    def get_instance(cls):
        if cls.instance:
            return cls.instance
        else:
            raise RuntimeError("No instance running")

    def __init__(self, address, port, known_commands):
        self.address = address
        self.port = port

        if issubclass(known_commands, Enum):
            self.known_commands = known_commands
        else:
            raise TypeError(
                "known_commands needs to be an enum")

        self.loop = asyncio.new_event_loop()
        self.send_queue = janus.Queue(loop=self.loop)
        self.communication_thread = threading.Thread(target=self.start_communication)
        self.communication_thread.start()
        Communicator.instance = self
    
    def start_communication(self):
        self.communication_task = self.loop.create_task(self.start_async_communication())
        self.loop.run_forever()

    def join(self):
        self.communication_thread.join()

    def encode_command(self, value):
        return value.value

    def handle_message(self, message):
        received = msgpack.unpackb(message, encoding="utf-8")
        command = self.known_commands(received["pywco_command"])
        if command not in self.known_commands:
            log.warning(f"This command is not known {command}")
            return
        del received["pywco_command"]
        try:
            signal(command).send(self, **received)
        except Exception as e:
            log.exception(f"Error handling the {command} command.")

    def _add_command_to_message(self, command, message):
        if "pywco_command" in message:
            raise AttributeError("pywco_command is reserved for pywco. Please use a different name.")

        message["pywco_command"] = command
        return message

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