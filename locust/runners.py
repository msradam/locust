from __future__ import annotations

from locust import __version__

import asyncio
import functools
import inspect
import json
import logging
import os
import re
import socket
import sys
import time
import traceback
from abc import abstractmethod
from collections import defaultdict
from collections.abc import Callable, Iterator, MutableMapping, ValuesView
from operator import itemgetter, methodcaller
from types import TracebackType
from typing import TYPE_CHECKING, Any, NoReturn, TypedDict, cast
from uuid import uuid4

import psutil

from . import argument_parser
from .dispatch import UsersDispatcher
from .exception import RPCError, RPCReceiveError, RPCSendError, StopTest
from .log import get_logs
from .rpc import Message, rpc
from .stats import RequestStats, StatsError, setup_distributed_stats_event_listeners
from .util.directory import get_abspaths_in
from .util.url import is_url

if TYPE_CHECKING:
    from . import User
    from .env import Environment

logger = logging.getLogger(__name__)

STATE_INIT, STATE_SPAWNING, STATE_RUNNING, STATE_CLEANUP, STATE_STOPPING, STATE_STOPPED, STATE_MISSING = [
    "ready",
    "spawning",
    "running",
    "cleanup",
    "stopping",
    "stopped",
    "missing",
]
WORKER_REPORT_INTERVAL = 3.0
WORKER_LOG_REPORT_INTERVAL = 10
CPU_MONITOR_INTERVAL = 10.0
CPU_WARNING_THRESHOLD = 90
HEARTBEAT_INTERVAL = 1
HEARTBEAT_LIVENESS = 3
HEARTBEAT_DEAD_INTERNAL = -60
MASTER_HEARTBEAT_TIMEOUT = 60
FALLBACK_INTERVAL = 5
CONNECT_TIMEOUT = 5
CONNECT_RETRY_COUNT = 60


def locust_exception_handler(environment: Environment):
    """Exception handler for asyncio tasks"""

    def handler(task: asyncio.Task):
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            return
        if exc is None:
            return
        if isinstance(exc, StopTest):
            logger.error(str(exc))
            logger.warning("Stopping Locust...")
            if environment.parsed_options.headless:
                environment.runner.quit()
            else:
                environment.runner.stop()
        else:
            logger.critical("Unhandled exception in task: %s", task.get_name(), exc_info=exc)

    return handler


class ExceptionDict(TypedDict):
    count: int
    msg: str
    traceback: str
    nodes: set[str]


class Runner:
    """
    Orchestrates the load test by starting and stopping the users.

    Use one of the :meth:`create_local_runner <locust.env.Environment.create_local_runner>`,
    :meth:`create_master_runner <locust.env.Environment.create_master_runner>` or
    :meth:`create_worker_runner <locust.env.Environment.create_worker_runner>` methods on
    the :class:`Environment <locust.env.Environment>` instance to create a runner of the
    desired type.
    """

    def __init__(self, environment: Environment) -> None:
        self.environment = environment
        self.user_tasks: dict[User, asyncio.Task] = {}
        self.background_tasks: list[asyncio.Task] = []
        self.state = STATE_INIT
        self.spawning_task: asyncio.Task | None = None
        self.shape_task: asyncio.Task | None = None
        self.shape_last_tick: tuple[int, float] | tuple[int, float, list[type[User]] | None] | None = None
        self.current_cpu_usage: float = 0.0
        self.cpu_warning_emitted: bool = False
        self.worker_cpu_warning_emitted: bool = False
        self.current_memory_usage: int = 0
        self._monitor_task: asyncio.Task | None = None  # started when event loop is running
        self.exceptions: dict[int, ExceptionDict] = {}
        # Because of the way the ramp-up/ramp-down is implemented, target_user_classes_count
        # is only updated at the end of the ramp-up/ramp-down.
        # See https://github.com/locustio/locust/issues/1883#issuecomment-919239824 for context.
        self.target_user_classes_count: dict[str, int] = {}
        # target_user_count is set before the ramp-up/ramp-down occurs.
        self.target_user_count: int = 0
        self.custom_messages: dict[str, tuple[Callable, bool]] = {}

        self._users_dispatcher: UsersDispatcher | None = None

        # set up event listeners for recording requests
        def on_request(request_type, name, response_time, response_length, exception=None, **_kwargs):
            self.stats.log_request(request_type, name, response_time, response_length)
            if exception:
                self.stats.log_error(request_type, name, exception)

        self.environment.events.request.add_listener(on_request)

        self.connection_broken = False
        self.final_user_classes_count: dict[str, int] = {}  # just for the ratio report, fills before runner stops

        # register listener that resets stats when spawning is complete
        def on_spawning_complete(user_count: int) -> None:
            self.update_state(STATE_RUNNING)
            if environment.reset_stats:
                logger.info("Resetting stats\n")
                self.stats.reset_all()

        self.environment.events.spawning_complete.add_listener(on_spawning_complete)

    def __del__(self) -> None:
        # cancel any stray tasks if runner is removed
        for task in self.background_tasks:
            if not task.done():
                task.cancel()

    @property
    def user_classes(self) -> list[type[User]]:
        return self.environment.user_classes

    @property
    def user_classes_by_name(self) -> dict[str, type[User]]:
        return self.environment.user_classes_by_name

    @property
    def stats(self) -> RequestStats:
        return self.environment.stats

    @property
    def errors(self) -> dict[str, StatsError]:
        return self.stats.errors

    @property
    def user_count(self) -> int:
        """
        :returns: Number of currently running users
        """
        return len([t for t in self.user_tasks.values() if not t.done()])

    @property
    def user_classes_count(self) -> dict[str, int]:
        """
        :returns: Number of currently running users for each user class
        """
        user_classes_count = {user_class.__name__: 0 for user_class in self.user_classes}
        for user, task in self.user_tasks.items():
            if task.done():
                continue
            user_classes_count[user.__class__.__name__] += 1
        return user_classes_count

    def update_state(self, new_state: str) -> None:
        """
        Updates the current state
        """
        # I (cyberwiz) commented out this logging, because it is too noisy even for debug level
        # Uncomment it if you are specifically debugging state transitions
        # logger.debug("Updating state to '%s', old state was '%s'" % (new_state, self.state))
        self.state = new_state

    def cpu_log_warning(self) -> bool:
        """Called at the end of the test"""
        if self.cpu_warning_emitted:
            logger.warning(
                "CPU usage was too high at some point during the test! See https://docs.locust.io/en/stable/running-distributed.html for how to distribute the load over multiple CPU cores or machines"
            )
        return self.cpu_warning_emitted

    async def spawn_users(self, user_classes_spawn_count: dict[str, int], wait: bool = False):
        if self.state == STATE_INIT or self.state == STATE_STOPPED:
            self.update_state(STATE_SPAWNING)

        logger.debug(
            f"Spawning additional {json.dumps(user_classes_spawn_count)} ({json.dumps(self.user_classes_count)} already running)..."
        )

        new_users: list[User] = []
        for user_class, spawn_count in user_classes_spawn_count.items():
            for n in range(spawn_count):
                new_user = self.user_classes_by_name[user_class](self.environment)
                assert hasattr(new_user, "environment"), (
                    f"Attribute 'environment' is missing on user {user_class}. Perhaps you defined your own __init__ and forgot to call the base constructor? (super().__init__(*args, **kwargs))"
                )
                task = asyncio.create_task(new_user.run())
                task.add_done_callback(lambda t: self._handle_user_exception(t))
                self.user_tasks[new_user] = task
                new_user._task = task
                new_users.append(new_user)
                if (n + 1) % 10 == 0 or (n + 1) == spawn_count:
                    logger.debug("%i users spawned" % self.user_count)
            logger.debug(f"All users of class {user_class} spawned")

        if wait:
            await asyncio.gather(*[self.user_tasks[u] for u in new_users], return_exceptions=True)
            logger.info("All users stopped\n")
        return new_users

    def _handle_user_exception(self, task: asyncio.Task):
        """Callback for user task completion"""
        try:
            task.result()
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error("User task failed: %s", e)

    async def stop_users(self, user_classes_stop_count: dict[str, int]) -> None:
        tasks_to_stop: list[asyncio.Task] = []

        for user_class, stop_count in user_classes_stop_count.items():
            if self.user_classes_count.get(user_class, 0) == 0:
                continue

            to_stop: list[User] = []
            for user, task in self.user_tasks.items():
                if len(to_stop) == stop_count:
                    break
                if task.done():
                    continue
                if type(user) is self.user_classes_by_name[user_class]:
                    to_stop.append(user)

            for user_to_stop in to_stop:
                task = self.user_tasks.get(user_to_stop)
                if task is None or task.done():
                    continue
                logger.debug(f"Stopping user {user_to_stop}")
                if self.environment.stop_timeout:
                    user_to_stop.stop(force=False)
                    tasks_to_stop.append(task)
                else:
                    user_to_stop.stop(force=True)

        if tasks_to_stop and self.environment.stop_timeout:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks_to_stop, return_exceptions=True),
                    timeout=self.environment.stop_timeout
                )
            except asyncio.TimeoutError:
                logger.info(
                    f"Not all users finished their tasks & terminated in {self.environment.stop_timeout} seconds. Stopping them..."
                )
                for task in tasks_to_stop:
                    if not task.done():
                        task.cancel()
                await asyncio.gather(*tasks_to_stop, return_exceptions=True)

        logger.debug(
            "%g users have been stopped, %g still running", sum(user_classes_stop_count.values()), self.user_count
        )

    async def monitor_cpu_and_memory(self) -> NoReturn:
        process = psutil.Process()
        while True:
            await asyncio.sleep(CPU_MONITOR_INTERVAL)
            self.current_cpu_usage = process.cpu_percent()
            self.current_memory_usage = process.memory_info().rss
            if self.current_cpu_usage > CPU_WARNING_THRESHOLD:
                self.environment.events.cpu_warning.fire(environment=self.environment, cpu_usage=self.current_cpu_usage)
                if not self.cpu_warning_emitted:
                    logging.warning(
                        f"CPU usage above {CPU_WARNING_THRESHOLD}%! This may constrain your throughput and may even give inconsistent response time measurements! See https://docs.locust.io/en/stable/running-distributed.html for how to distribute the load over multiple CPU cores or machines"
                    )
                    self.cpu_warning_emitted = True

            self.environment.events.usage_monitor.fire(
                environment=self.environment, cpu_usage=self.current_cpu_usage, memory_usage=self.current_memory_usage
            )

    @abstractmethod
    async def start(
        self, user_count: int, spawn_rate: float, wait: bool = False, user_classes: list[type[User]] | None = None
    ) -> None: ...

    @abstractmethod
    def send_message(self, msg_type: str, data: Any = None, client_id: str | None = None) -> None: ...

    def start_shape(self) -> None:
        """
        Start running a load test with a custom LoadTestShape specified in the :meth:`Environment.shape_class <locust.env.Environment.shape_class>` parameter.
        """
        if self.shape_task and not self.shape_task.done():
            logger.info("There is an ongoing shape test running. Editing is disabled")
            return

        logger.info("Shape test starting.")
        self.update_state(STATE_INIT)
        self.shape_task = asyncio.create_task(self.shape_worker())
        self.shape_task.add_done_callback(locust_exception_handler(self.environment))
        if self.environment.shape_class is not None:
            self.environment.shape_class.reset_time()

    async def shape_worker(self) -> None:
        logger.info("Shape worker starting")
        while self.state == STATE_INIT or self.state == STATE_SPAWNING or self.state == STATE_RUNNING:
            shape_adjustment_start = time.time()
            current_tick = self.environment.shape_class.tick() if self.environment.shape_class is not None else None
            if current_tick is None:
                logger.info("Shape test stopping")
                if self.environment.parsed_options and self.environment.parsed_options.headless:
                    self.quit()
                else:
                    await self.stop()
                self.shape_task = None
                self.shape_last_tick = None
                return
            elif self.shape_last_tick != current_tick:
                if len(current_tick) == 2:
                    user_count, spawn_rate = current_tick
                    user_classes = None
                else:
                    user_count, spawn_rate, user_classes = current_tick
                logger.info("Shape test updating to %d users at %.2f spawn rate" % (user_count, spawn_rate))
                await self.start(user_count=user_count, spawn_rate=spawn_rate, user_classes=user_classes)
                self.shape_last_tick = current_tick
            shape_adjustment_time_ms = time.time() - shape_adjustment_start
            await asyncio.sleep(max(1 - shape_adjustment_time_ms, 0))

    async def stop(self) -> None:
        """
        Stop a running load test by stopping all running users
        """
        if self.state == STATE_STOPPED:
            return
        try:
            caller = inspect.getframeinfo(inspect.stack()[1][0])
            logger.debug(f"Stopping all users (called from {caller.filename}:{caller.lineno})")
        except Exception:
            logger.debug("Stopping all users (couldn't determine where stop() was called from)")
        self.environment.events.test_stopping.fire(environment=self.environment)
        self.final_user_classes_count = {**self.user_classes_count}
        self.update_state(STATE_CLEANUP)

        # if we are currently spawning users we need to cancel the spawning task first
        if self.spawning_task and not self.spawning_task.done():
            self.spawning_task.cancel()
            try:
                await self.spawning_task
            except asyncio.CancelledError:
                pass

        if self.environment.shape_class is not None:
            # If the test was not started yet and locust is
            # stopped/quit, shape_task will be None.
            if self.shape_task is not None and not self.shape_task.done():
                self.shape_task.cancel()
                try:
                    await self.shape_task
                except asyncio.CancelledError:
                    pass
                self.shape_task = None
            self.shape_last_tick = None

        await self.stop_users(self.user_classes_count)

        self._users_dispatcher = None

        self.update_state(STATE_STOPPED)

        self.cpu_log_warning()
        self.environment.events.test_stop.fire(environment=self.environment)

    def quit(self) -> None:
        """
        Stop any running load test and cancel all tasks for the runner
        """
        # schedule the async stop and task cancellation
        asyncio.create_task(self._quit_async())

    async def _quit_async(self) -> None:
        """Async implementation of quit"""
        await self.stop()
        # cancel all background tasks
        for task in self.background_tasks:
            if not task.done():
                task.cancel()
        if self.background_tasks:
            await asyncio.gather(*self.background_tasks, return_exceptions=True)
        # cancel monitor task
        if self._monitor_task and not self._monitor_task.done():
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass

    def log_exception(self, node_id: str, msg: str, formatted_tb: str) -> None:
        key = hash(formatted_tb)
        row = self.exceptions.setdefault(key, {"count": 0, "msg": msg, "traceback": formatted_tb, "nodes": set()})
        row["count"] += 1
        row["nodes"].add(node_id)
        self.exceptions[key] = row

    def register_message(self, msg_type: str, listener: Callable, concurrent=False) -> None:
        """
        Register a listener for a custom message from another node

        :param msg_type: The type of the message to listen for
        :param listener: The function to execute when the message is received
        """
        if msg_type in self.custom_messages:
            raise Exception(f"Tried to register listener method for {msg_type}, but it already had a listener!")
        self.custom_messages[msg_type] = (listener, concurrent)


class LocalRunner(Runner):
    """
    Runner for running single process load test
    """

    def __init__(self, environment) -> None:
        """
        :param environment: Environment instance
        """
        super().__init__(environment)
        # These attributes dont make a lot of sense for LocalRunner
        # but it makes it easier to write tests that work for both local and distributed runs
        self.worker_index = 0
        self.client_id = socket.gethostname() + "_" + uuid4().hex
        self.worker_count = 1
        # Only when running in standalone mode (non-distributed)
        self._local_worker_node = WorkerNode(id="local")
        self._local_worker_node.user_classes_count = self.user_classes_count

        # register listener that's logs the exception for the local runner
        def on_user_error(user_instance, exception, tb):
            formatted_tb = "".join(traceback.format_tb(tb))
            self.log_exception("local", str(exception), formatted_tb)

        self.environment.events.user_error.add_listener(on_user_error)

    async def _start(self, user_count: int, spawn_rate: float, wait: bool = False, user_classes: list | None = None) -> None:
        """
        Start running a load test

        :param user_count: Total number of users to start
        :param spawn_rate: Number of users to spawn per second
        :param wait: If True calls to this method will block until all users are spawned.
                     If False (the default), a task that spawns the users will be
                     started and the call to this method will return immediately.
        :param user_classes: The user classes to be dispatched, None indicates to use the classes the dispatcher was
                             invoked with.
        """
        self.target_user_count = user_count

        if self.state != STATE_RUNNING and self.state != STATE_SPAWNING:
            self.stats.clear_all()
            self.exceptions = {}
            self.cpu_warning_emitted = False
            self.worker_cpu_warning_emitted = False
            self.environment._filter_tasks_by_tags()
            self.environment.events.test_start.fire(environment=self.environment)

        if wait and user_count - self.user_count > spawn_rate:
            raise ValueError("wait is True but the amount of users to add is greater than the spawn rate")

        for user_class in self.user_classes:
            if self.environment.host:
                user_class.host = self.environment.host

        if self.state != STATE_INIT and self.state != STATE_STOPPED:
            self.update_state(STATE_SPAWNING)

        if self._users_dispatcher is None:
            self._users_dispatcher = self.environment.dispatcher_class(
                worker_nodes=[self._local_worker_node], user_classes=self.user_classes
            )

        logger.info("Ramping to %d users at a rate of %.2f per second" % (user_count, spawn_rate))

        self._users_dispatcher.new_dispatch(user_count, spawn_rate, user_classes)

        try:
            for dispatched_users in self._users_dispatcher:
                user_classes_spawn_count: dict[str, int] = {}
                user_classes_stop_count: dict[str, int] = {}
                user_classes_count = dispatched_users[self._local_worker_node.id]
                logger.debug(f"Ramping to {_format_user_classes_count_for_log(user_classes_count)}")
                for user_class_name, user_class_count in user_classes_count.items():
                    if self.user_classes_count[user_class_name] > user_class_count:
                        user_classes_stop_count[user_class_name] = (
                            self.user_classes_count[user_class_name] - user_class_count
                        )
                    elif self.user_classes_count[user_class_name] < user_class_count:
                        user_classes_spawn_count[user_class_name] = (
                            user_class_count - self.user_classes_count[user_class_name]
                        )

                if wait:
                    # spawn_users will block, so we need to call stop_users first
                    await self.stop_users(user_classes_stop_count)
                    await self.spawn_users(user_classes_spawn_count, wait)
                else:
                    # call spawn_users before stopping the users since stop_users
                    # can be blocking because of the stop_timeout
                    await self.spawn_users(user_classes_spawn_count, wait)
                    await self.stop_users(user_classes_stop_count)

                self._local_worker_node.user_classes_count = next(iter(dispatched_users.values()))

        except KeyboardInterrupt:
            # We need to catch keyboard interrupt. Otherwise, if KeyboardInterrupt is received while in
            # an asyncio.sleep inside the dispatch_users function, locust won't gracefully shutdown.
            self.quit()

        logger.info(f"All users spawned: {_format_user_classes_count_for_log(self.user_classes_count)}")

        self.target_user_classes_count = self.user_classes_count

        self.environment.events.spawning_complete.fire(user_count=sum(self.target_user_classes_count.values()))

    async def start(
        self, user_count: int, spawn_rate: float, wait: bool = False, user_classes: list[type[User]] | None = None
    ) -> None:
        if spawn_rate > 100:
            logger.warning(
                "Your selected spawn rate is very high (>100), and this is known to sometimes cause issues. Do you really need to ramp up that fast?"
            )

        if self.spawning_task and not self.spawning_task.done():
            # cancel existing spawning_task before we start a new one
            self.spawning_task.cancel()
            try:
                await self.spawning_task
            except asyncio.CancelledError:
                pass
        self.spawning_task = asyncio.create_task(
            self._start(user_count, spawn_rate, wait=wait, user_classes=user_classes)
        )
        self.spawning_task.add_done_callback(locust_exception_handler(self.environment))

    async def stop(self) -> None:
        if self.state == STATE_STOPPED:
            return
        await super().stop()

    def send_message(self, msg_type: str, data: Any = None, client_id: str | None = None) -> None:
        """
        Emulates internodal messaging by calling registered listeners

        :param msg_type: The type of the message to emulate sending
        :param data: Optional data to include
        """
        logger.debug(f"Running locally: sending {msg_type} message to self")
        if msg_type in self.custom_messages:
            listener, concurrent = self.custom_messages[msg_type]
            msg = Message(msg_type, data, "local")
            listener(environment=self.environment, msg=msg)
        else:
            logger.warning(f"Unknown message type received: {msg_type}")


class DistributedRunner(Runner):
    def __init__(self, environment) -> None:
        super().__init__(environment)
        setup_distributed_stats_event_listeners(self.environment.events, self.stats)


class WorkerNode:
    def __init__(self, id: str, state=STATE_INIT, heartbeat_liveness=HEARTBEAT_LIVENESS) -> None:
        self.id: str = id
        self.state = state
        self.heartbeat = heartbeat_liveness
        self.cpu_usage: int = 0
        self.cpu_warning_emitted = False
        self.memory_usage: int = 0
        # The reported users running on the worker
        self.user_classes_count: dict[str, int] = {}

    @property
    def user_count(self) -> int:
        return sum(self.user_classes_count.values())


class WorkerNodes(MutableMapping):
    def __init__(self) -> None:
        self._worker_nodes: dict[str, WorkerNode] = {}

    def get_by_state(self, state) -> list[WorkerNode]:
        return [c for c in self.values() if c.state == state]

    @property
    def all(self) -> ValuesView[WorkerNode]:
        return self.values()

    @property
    def ready(self) -> list[WorkerNode]:
        return self.get_by_state(STATE_INIT)

    @property
    def spawning(self) -> list[WorkerNode]:
        return self.get_by_state(STATE_SPAWNING)

    @property
    def running(self) -> list[WorkerNode]:
        return self.get_by_state(STATE_RUNNING)

    @property
    def missing(self) -> list[WorkerNode]:
        return self.get_by_state(STATE_MISSING)

    def __setitem__(self, k: str, v: WorkerNode) -> None:
        self._worker_nodes[k] = v

    def __delitem__(self, k: str) -> None:
        del self._worker_nodes[k]

    def __getitem__(self, k: str) -> WorkerNode:
        return self._worker_nodes[k]

    def __len__(self) -> int:
        return len(self._worker_nodes)

    def __iter__(self) -> Iterator[str]:
        return iter(list(self._worker_nodes.keys()))


class MasterRunner(DistributedRunner):
    """
    Runner used to run distributed load tests across multiple processes and/or machines.

    MasterRunner doesn't spawn any user tasks itself. Instead it expects
    :class:`WorkerRunners <WorkerRunner>` to connect to it, which it will then direct
    to start and stop user tasks. Stats sent back from the
    :class:`WorkerRunners <WorkerRunner>` will aggregated.
    """

    def __init__(self, environment, master_bind_host, master_bind_port) -> None:
        """
        :param environment: Environment instance
        :param master_bind_host: Host/interface to use for incoming worker connections
        :param master_bind_port: Port to use for incoming worker connections
        """
        super().__init__(environment)
        self.worker_cpu_warning_emitted = False
        self.master_bind_host = master_bind_host
        self.master_bind_port = master_bind_port
        self.spawn_rate: float = 0.0
        self.spawning_completed = False
        self.worker_indexes: dict[str, int] = {}
        self.worker_index_max = 0
        self._heartbeat_task: asyncio.Task | None = None
        self._client_listener_task: asyncio.Task | None = None

        self.clients = WorkerNodes()
        try:
            self.server = rpc.Server(master_bind_host, master_bind_port)
        except RPCError as e:
            if e.args[0] == "Socket bind failure: Address already in use":
                port_string = (
                    master_bind_host + ":" + str(master_bind_port) if master_bind_host != "*" else str(master_bind_port)
                )
                logger.error(
                    f"The Locust master port ({port_string}) was busy. Close any applications using that port - perhaps an old instance of Locust master is still running? ({e.args[0]})"
                )
                sys.exit(1)
            else:
                raise

        self._users_dispatcher: UsersDispatcher | None = None

        # background tasks will be started when the event loop is running
        # they are created in start_background_tasks() called from main

        # listener that gathers info on how many users the worker has spawned
        def on_worker_report(client_id: str, data: dict[str, Any]) -> None:
            if client_id not in self.clients:
                logger.info("Discarded report from unrecognized worker %s", client_id)
                return
            self.clients[client_id].user_classes_count = data["user_classes_count"]

        self.environment.events.worker_report.add_listener(on_worker_report)

        # register listener that sends quit message to worker nodes
        def on_quitting(environment: Environment, **kw):
            self.quit()

        self.environment.events.quitting.add_listener(on_quitting)

    def rebalancing_enabled(self) -> bool:
        return self.environment.parsed_options is not None and cast(
            bool, self.environment.parsed_options.enable_rebalancing
        )

    def get_worker_index(self, client_id):
        """
        Get the worker index for the specified client ID;
        this is a deterministic 0-based ordinal number and guaranteed to not change
        while Master is alive.
        """
        if client_id in self.worker_indexes:
            return self.worker_indexes[client_id]
        index = self.worker_index_max
        self.worker_indexes[client_id] = index
        self.worker_index_max += 1
        return index

    @property
    def user_count(self) -> int:
        return sum([c.user_count for c in self.clients.values()])

    def cpu_log_warning(self) -> bool:
        warning_emitted = Runner.cpu_log_warning(self)
        if self.worker_cpu_warning_emitted:
            logger.warning("CPU usage threshold was exceeded on workers during the test!")
            warning_emitted = True
        return warning_emitted

    def start_background_tasks(self) -> None:
        """Start the heartbeat and client listener background tasks.
        Called when the event loop is running."""
        self._heartbeat_task = asyncio.create_task(self.heartbeat_worker())
        self._heartbeat_task.add_done_callback(locust_exception_handler(self.environment))
        self._client_listener_task = asyncio.create_task(self.client_listener())
        self._client_listener_task.add_done_callback(locust_exception_handler(self.environment))

    async def start(
        self, user_count: int, spawn_rate: float, wait=False, user_classes: list[type[User]] | None = None
    ) -> None:
        self.spawning_completed = False

        self.target_user_count = user_count

        num_workers = len(self.clients.ready) + len(self.clients.running) + len(self.clients.spawning)
        if not num_workers:
            logger.warning("You can't start a distributed test before at least one worker processes has connected")
            return

        for user_class in self.user_classes:
            if self.environment.host:
                user_class.host = self.environment.host

        self.spawn_rate = spawn_rate

        if self._users_dispatcher is None:
            self._users_dispatcher = self.environment.dispatcher_class(
                worker_nodes=list(self.clients.values()), user_classes=self.user_classes
            )

        logger.info(
            "Sending spawn jobs of %d users at %.2f spawn rate to %d ready workers"
            % (user_count, spawn_rate, num_workers)
        )

        worker_spawn_rate = float(spawn_rate) / (num_workers or 1)
        if worker_spawn_rate > 100:
            logger.warning(
                "Your selected spawn rate is very high (>100/worker), and this is known to sometimes cause issues. Do you really need to ramp up that fast?"
            )

        if self.state != STATE_RUNNING and self.state != STATE_SPAWNING:
            self.stats.clear_all()
            self.exceptions = {}
            self.environment._filter_tasks_by_tags()
            self.environment.events.test_start.fire(environment=self.environment)
            if self.environment.shape_class:
                self.environment.shape_class.reset_time()

        self.update_state(STATE_SPAWNING)

        self._users_dispatcher.new_dispatch(
            target_user_count=user_count, spawn_rate=spawn_rate, user_classes=user_classes
        )

        try:
            for dispatched_users in self._users_dispatcher:
                # send spawn messages to all workers concurrently
                send_tasks = []
                for worker_node_id, worker_user_classes_count in dispatched_users.items():
                    data = {
                        "timestamp": time.time(),
                        "user_classes_count": worker_user_classes_count,
                        "host": self.environment.host,
                        "stop_timeout": self.environment.stop_timeout,
                        "parsed_options": vars(self.environment.parsed_options)
                        if self.environment.parsed_options
                        else {},
                    }
                    # run send in thread pool since RPC may block
                    send_tasks.append(
                        asyncio.create_task(
                            asyncio.to_thread(
                                self.server.send_to_client,
                                Message("spawn", data, worker_node_id),
                            )
                        )
                    )
                dispatched_user_count = sum(map(sum, map(methodcaller("values"), dispatched_users.values())))
                logger.debug(
                    "Sending spawn messages for %g total users to %i worker(s)",
                    dispatched_user_count,
                    len(send_tasks),
                )
                await asyncio.gather(*send_tasks)

                logger.debug(
                    f"Currently spawned users: {_format_user_classes_count_for_log(self.reported_user_classes_count)}"
                )

            self.target_user_classes_count = _aggregate_dispatched_users(dispatched_users)

        except KeyboardInterrupt:
            # We need to catch keyboard interrupt. Otherwise, if KeyboardInterrupt is received while in
            # an asyncio.sleep inside the dispatch_users function, locust won't gracefully shutdown.
            self.quit()

        # Wait a little for workers to report their users to the master
        # so that we can give an accurate log message below and fire the `spawning_complete` event
        # when the user count is really at the desired value.
        wait_timeout = self._wait_for_workers_report_after_ramp_up()
        msg_prefix = "All users spawned"
        try:
            start_time = time.time()
            while self.user_count != self.target_user_count:
                if time.time() - start_time > wait_timeout:
                    msg_prefix = (
                        "Spawning is complete and report waittime is expired, but not all reports received from workers"
                    )
                    break
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            raise

        user_count = sum(self.target_user_classes_count.values())
        self.environment.events.spawning_complete.fire(user_count=user_count)
        # notify workers so they can fire their own event
        self.send_message("spawning_complete", data={"user_count": user_count})
        self.spawning_completed = True

        logger.info(f"{msg_prefix}: {_format_user_classes_count_for_log(self.reported_user_classes_count)}")

    @functools.lru_cache
    def _wait_for_workers_report_after_ramp_up(self) -> float:
        """
        The amount of time to wait after a ramp-up in order for all the workers to report their state
        to the master. If not supplied by the user, it is 1000ms by default. If the supplied value is a number,
        it is taken as-is. If the supplied value is a pattern like "some_number * WORKER_REPORT_INTERVAL",
        the value will be "some_number * WORKER_REPORT_INTERVAL". The most sensible value would be something
        like "1.25 * WORKER_REPORT_INTERVAL". However, some users might find it too high, so it is left
        to a relatively small value of 1000ms by default.
        """
        locust_wait_for_workers_report_after_ramp_up = os.getenv("LOCUST_WAIT_FOR_WORKERS_REPORT_AFTER_RAMP_UP")
        if locust_wait_for_workers_report_after_ramp_up is None:
            return 1.0

        match = re.search(
            r"^(?P<coeff>(\d+)|(\d+\.\d+))[ ]*\*[ ]*WORKER_REPORT_INTERVAL$",
            locust_wait_for_workers_report_after_ramp_up,
        )
        if match is None:
            assert float(locust_wait_for_workers_report_after_ramp_up) >= 0
            return float(locust_wait_for_workers_report_after_ramp_up)
        else:
            return float(match.group("coeff")) * WORKER_REPORT_INTERVAL

    async def stop(self, send_stop_to_client: bool = True) -> None:
        if self.state not in [STATE_INIT, STATE_STOPPED, STATE_STOPPING]:
            logger.debug("Stopping...")
            self.environment.events.test_stopping.fire(environment=self.environment)
            self.final_user_classes_count = {**self.reported_user_classes_count}
            self.update_state(STATE_STOPPING)

            if self.environment.shape_class is not None and self.shape_task is not None:
                if not self.shape_task.done():
                    self.shape_task.cancel()
                    try:
                        await self.shape_task
                    except asyncio.CancelledError:
                        pass
                self.shape_task = None
                self.shape_last_tick = None

            self._users_dispatcher = None

            if send_stop_to_client:
                for client in self.clients.all:
                    logger.debug(f"Sending stop message to worker {client.id}")
                    await asyncio.to_thread(self.server.send_to_client, Message("stop", None, client.id))

                # Give an additional 60s for all workers to stop
                stop_timeout = (self.environment.stop_timeout or 0) + 60
                start_time = time.time()
                while self.user_count != 0:
                    if time.time() - start_time > stop_timeout:
                        logger.error("Timeout waiting for all workers to stop")
                        break
                    await asyncio.sleep(1)
            self.environment.events.test_stop.fire(environment=self.environment)

    def quit(self) -> None:
        asyncio.create_task(self._quit_async())

    async def _quit_async(self) -> None:
        await self.stop(send_stop_to_client=False)
        logger.debug("Quitting...")
        for client in self.clients.all:
            logger.debug(f"Sending quit message to worker {client.id} (index {self.get_worker_index(client.id)})")
            await asyncio.to_thread(self.server.send_to_client, Message("quit", None, client.id))
        await asyncio.sleep(0.5)  # wait for final stats report from all workers
        # cancel background tasks
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
        if self._client_listener_task and not self._client_listener_task.done():
            self._client_listener_task.cancel()

    def check_stopped(self) -> None:
        if (
            self.state == STATE_STOPPING
            and all(x.state == STATE_INIT for x in self.clients.all)
            or all(x.state not in (STATE_RUNNING, STATE_SPAWNING, STATE_INIT) for x in self.clients.all)
        ):
            self.update_state(STATE_STOPPED)

    async def heartbeat_worker(self) -> NoReturn:
        while True:
            await asyncio.sleep(HEARTBEAT_INTERVAL)
            if self.connection_broken:
                await asyncio.to_thread(self.reset_connection)
                continue

            missing_clients_to_be_removed = []
            for client in self.clients.all:
                # if clients goes missing for more than HEARTBEAT_DEAD_INTERNAL then add them to be removed list
                if client.state == STATE_MISSING and client.heartbeat <= HEARTBEAT_DEAD_INTERNAL:
                    missing_clients_to_be_removed.append(client.id)

                if client.heartbeat < 0 and client.state != STATE_MISSING:
                    logger.info(f"Worker {str(client.id)} failed to send heartbeat, setting state to missing.")
                    client.state = STATE_MISSING
                    client.user_classes_count = {}
                    if self._users_dispatcher is not None:
                        self._users_dispatcher.remove_worker(client)
                        if self.rebalancing_enabled() and self.state == STATE_RUNNING and self.spawning_completed:
                            await self.start(self.target_user_count, self.spawn_rate)
                    if self.worker_count <= 0:
                        logger.info("The last worker went missing, stopping test.")
                        await self.stop()
                        self.check_stopped()
                else:
                    client.heartbeat -= 1

            # if there are any missing clients to be removed then remove them and trigger rebalance.
            if len(missing_clients_to_be_removed) > 0:
                for to_remove_client_id in missing_clients_to_be_removed:
                    if self.clients.get(to_remove_client_id) is not None:
                        del self.clients[to_remove_client_id]
                if self.state == STATE_RUNNING or self.state == STATE_SPAWNING:
                    # _users_dispatcher is set to none so that during redistribution the dead clients are not picked, alternative is to call self.stop() before start
                    self._users_dispatcher = None
                    # trigger redistribution after missing client removal
                    await self.start(user_count=self.target_user_count, spawn_rate=self.spawn_rate)

    def reset_connection(self) -> None:
        logger.info("Resetting RPC server and all worker connections.")
        try:
            self.server.close(linger=0)
            self.server = rpc.Server(self.master_bind_host, self.master_bind_port)
            self.connection_broken = False
        except RPCError as e:
            logger.error(f"Temporary failure when resetting connection: {e}, will retry later.")

    async def client_listener(self) -> NoReturn:
        while True:
            try:
                # run blocking RPC receive in thread pool
                client_id, msg = await asyncio.to_thread(self.server.recv_from_client)
            except RPCReceiveError as e:
                client_id = e.addr

                if client_id and client_id in self.clients:
                    logger.error(f"RPCError when receiving from client: {e}. Will reset client {client_id}.")
                    try:
                        await asyncio.to_thread(self.server.send_to_client, Message("reconnect", None, client_id))
                    except Exception as error:
                        logger.error(f"Error sending reconnect message to worker: {error}. Will reset RPC server.")
                        self.connection_broken = True
                        await asyncio.sleep(FALLBACK_INTERVAL)
                        continue
                else:
                    message = f"{e}" if not client_id else f"{e} from {client_id}"
                    logger.error(f"Unrecognized message detected: {message}")
                    continue
            except RPCSendError as e:
                logger.error(f"Error sending reconnect message to worker: {e}. Will reset RPC server.")
                self.connection_broken = True
                await asyncio.sleep(FALLBACK_INTERVAL)
                continue
            except RPCError as e:
                if self.clients.ready or self.clients.spawning or self.clients.running:
                    logger.error(f"RPCError: {e}. Will reset RPC server.")
                else:
                    logger.debug(
                        f"RPCError when receiving from worker: {e} (but no workers were expected to be connected anyway)"
                    )
                self.connection_broken = True
                await asyncio.sleep(FALLBACK_INTERVAL)
                continue
            except KeyboardInterrupt:
                logging.debug(
                    "Got KeyboardInterrupt in client_listener. Other tasks should catch this and shut down."
                )
            await self.handle_message(client_id, msg)

    async def handle_message(self, client_id: str, msg: Message) -> None:
        match msg.type:
            case "client_ready":
                if not msg.data:
                    logger.error(f"An old (pre 2.0) worker tried to connect ({client_id}). That's not going to work.")
                    return
                elif msg.data != __version__ and msg.data != -1:
                    if msg.data[0:4] == __version__[0:4]:
                        logger.debug(
                            f"A worker ({client_id}) running a different patch version ({repr(msg.data)}) connected, master version is {repr(__version__)}"
                        )
                    else:
                        logger.warning(
                            f"A worker ({client_id}) running a different version ({msg.data}) connected, master version is {__version__}"
                        )
                self.send_message("ack", client_id=client_id, data={"index": self.get_worker_index(client_id)})
                self.environment.events.worker_connect.fire(client_id=msg.node_id)
                client_already_connected = client_id in self.clients
                self.clients[client_id] = WorkerNode(client_id, heartbeat_liveness=HEARTBEAT_LIVENESS)
                if self._users_dispatcher is not None:
                    self._users_dispatcher.add_worker(worker_node=self.clients[client_id])
                    if not self._users_dispatcher.dispatch_in_progress and self.state == STATE_RUNNING:
                        # TODO: Test this situation
                        await self.start(self.target_user_count, self.spawn_rate)
                if client_already_connected:
                    logger.debug(
                        f"{client_id} (index {self.get_worker_index(client_id)}) reported as ready (duplicate message). {len(self.clients.ready + self.clients.running + self.clients.spawning)} workers connected."
                    )
                else:
                    logger.info(
                        f"{client_id} (index {self.get_worker_index(client_id)}) reported as ready. {len(self.clients.ready + self.clients.running + self.clients.spawning)} workers connected."
                    )
                if self.rebalancing_enabled() and self.state == STATE_RUNNING and self.spawning_completed:
                    await self.start(self.target_user_count, self.spawn_rate)
                # emit a warning if the worker's clock seem to be out of sync with our clock
                # if abs(time() - msg.data["time"]) > 5.0:
                #    warnings.warn("The worker node's clock seem to be out of sync. For the statistics to be correct the different locust servers need to have synchronized clocks.")
            case "locustfile":
                if not msg.data["version"]:
                    logger.error("A very old worker version requested locustfile. This probably won't work.")
                elif msg.data["version"][0:4] == __version__[0:4]:
                    logger.debug(
                        f"A worker ({msg.node_id}) running a different patch version ({msg.data['version']}) connected, master version is {__version__}"
                    )

                logging.debug("Worker requested locust file")
                assert self.environment.parsed_locustfiles
                locustfile_options = self.environment.parsed_locustfiles
                locustfile_list = [f.strip() for f in locustfile_options if not os.path.isdir(f)]

                for locustfile_option in locustfile_options:
                    if os.path.isdir(locustfile_option):
                        locustfile_list.extend(get_abspaths_in(locustfile_option, extension=".py"))

                try:
                    locustfiles: list[str | dict[str, str]] = []

                    for filename in locustfile_list:
                        if is_url(filename):
                            locustfiles.append(filename)
                        else:
                            with open(filename) as f:
                                filename = os.path.basename(filename)
                                file_contents = f.read()

                            locustfiles.append({"filename": filename, "contents": file_contents})
                except Exception as e:
                    error_message = "locustfile must be a full path to a single locustfile, a comma-separated list of .py files, or a URL for file distribution to work"
                    logger.error(f"{error_message} {e}")
                    self.send_message(
                        "locustfile",
                        client_id=client_id,
                        data={"error": f"{error_message} (was '{filename}')"},
                    )
                else:
                    self.send_message(
                        "locustfile",
                        client_id=client_id,
                        data={"locustfiles": locustfiles},
                    )
                return
            case "client_stopped":
                if msg.node_id not in self.clients:
                    logger.warning(f"Received {msg.type} message from an unknown worker: {msg.node_id}.")
                    return
                client = self.clients[msg.node_id]
                del self.clients[msg.node_id]
                if self._users_dispatcher is not None:
                    self._users_dispatcher.remove_worker(client)
                    if not self._users_dispatcher.dispatch_in_progress and self.state == STATE_RUNNING:
                        # TODO: Test this situation
                        await self.start(self.target_user_count, self.spawn_rate)
                logger.info(f"{msg.node_id} (index {self.get_worker_index(client_id)}) reported that it has stopped")
            case "heartbeat":
                if msg.node_id in self.clients:
                    c = self.clients[msg.node_id]
                    c.heartbeat = HEARTBEAT_LIVENESS
                    client_state = msg.data["state"]
                    if c.state == STATE_MISSING:
                        logger.info(f"Worker {str(c.id)} self-healed with heartbeat, setting state to {client_state}.")
                        if self._users_dispatcher is not None:
                            self._users_dispatcher.add_worker(worker_node=c)
                            if not self._users_dispatcher.dispatch_in_progress and self.state == STATE_RUNNING:
                                # TODO: Test this situation
                                await self.start(self.target_user_count, self.spawn_rate)
                    c.state = client_state
                    c.cpu_usage = msg.data["current_cpu_usage"]
                    if not c.cpu_warning_emitted and c.cpu_usage > 90:
                        self.worker_cpu_warning_emitted = True  # used to fail the test in the end
                        c.cpu_warning_emitted = True  # used to suppress logging for this node
                        logger.warning(
                            f"Worker {msg.node_id} (index {self.get_worker_index(msg.node_id)}) exceeded cpu threshold (will only log this once per worker)"
                        )
                    if "current_memory_usage" in msg.data:
                        c.memory_usage = msg.data["current_memory_usage"]
                    self.environment.events.heartbeat_sent.fire(client_id=msg.node_id, timestamp=time.time())
                    await asyncio.to_thread(self.server.send_to_client, Message("heartbeat", None, msg.node_id))
                else:
                    logging.debug(f"Got heartbeat message from unknown worker {msg.node_id}")
            case "stats":
                self.environment.events.worker_report.fire(client_id=msg.node_id, data=msg.data)
            case "spawning":
                try:
                    self.clients[msg.node_id].state = STATE_SPAWNING
                except KeyError:
                    logger.warning(f"Got spawning message from unknown worker {msg.node_id}. Asking worker to quit.")
                    await asyncio.to_thread(self.server.send_to_client, Message("quit", None, msg.node_id))
            case "spawning_complete":
                # a worker finished spawning (this happens multiple times during rampup)
                self.clients[msg.node_id].state = STATE_RUNNING
                self.clients[msg.node_id].user_classes_count = msg.data["user_classes_count"]
            case "logs":
                self.environment.update_worker_logs(msg.data)
            case "quit":
                if msg.node_id in self.clients:
                    client = self.clients[msg.node_id]
                    del self.clients[msg.node_id]
                    if self._users_dispatcher is not None:
                        self._users_dispatcher.remove_worker(client)
                        if not self._users_dispatcher.dispatch_in_progress and self.state == STATE_RUNNING:
                            # TODO: Test this situation
                            await self.start(self.target_user_count, self.spawn_rate)
                    logger.info(
                        f"Worker {msg.node_id!r} (index {self.get_worker_index(msg.node_id)}) quit. {len(self.clients.ready)} workers ready."
                    )
                    if self.worker_count - len(self.clients.missing) <= 0:
                        logger.info("The last worker quit, stopping test.")
                        await self.stop()
                        if self.environment.parsed_options and self.environment.parsed_options.headless:
                            self.quit()
            case "exception":
                self.log_exception(msg.node_id, msg.data["msg"], msg.data["traceback"])
            case _ if lc := self.custom_messages.get(msg.type):
                listener, concurrent = lc
                logger.debug(
                    f"Received {msg.type} message from worker {msg.node_id} (index {self.get_worker_index(msg.node_id)})"
                )
                try:
                    if not concurrent:
                        listener(environment=self.environment, msg=msg)
                    else:
                        asyncio.create_task(asyncio.to_thread(listener, environment=self.environment, msg=msg))
                except Exception:
                    logging.error(f"Uncaught exception in handler for {msg.type}\n{traceback.format_exc()}")
            case _:
                logger.warning(
                    f"Unknown message type received from worker {msg.node_id} (index {self.get_worker_index(msg.node_id)}): {msg.type}"
                )

        self.check_stopped()

    @property
    def worker_count(self) -> int:
        return len(self.clients.ready) + len(self.clients.spawning) + len(self.clients.running)

    @property
    def reported_user_classes_count(self) -> dict[str, int]:
        reported_user_classes_count: dict[str, int] = defaultdict(int)
        for client in self.clients.ready + self.clients.spawning + self.clients.running:
            for name, count in client.user_classes_count.items():
                reported_user_classes_count[name] += count
        return reported_user_classes_count

    def send_message(self, msg_type: str, data: dict[str, Any] | None = None, client_id: str | None = None):
        """
        Sends a message to attached worker node(s)

        :param msg_type: The type of the message to send
        :param data: Optional data to send
        :param client_id: Optional id of the target worker node.
                            If None, will send to all attached workers
        """
        if client_id:
            logger.debug(f"Sending {msg_type} message to worker {client_id}")
            self.server.send_to_client(Message(msg_type, data, client_id))
        else:
            for client in self.clients.all:
                logger.debug(f"Sending {msg_type} message to worker {client.id}")
                self.server.send_to_client(Message(msg_type, data, client.id))


class WorkerRunner(DistributedRunner):
    """
    Runner used to run distributed load tests across multiple processes and/or machines.

    WorkerRunner connects to a :class:`MasterRunner` from which it'll receive
    instructions to start and stop user tasks. The WorkerRunner will periodically
    take the stats generated by the running users and send back to the :class:`MasterRunner`.
    """

    # the worker index is set on ACK, if master provided it (masters <= 2.10.2 do not provide it)
    worker_index = -1

    def __init__(self, environment: Environment, master_host: str, master_port: int) -> None:
        """
        :param environment: Environment instance
        :param master_host: Host/IP to use for connection to the master
        :param master_port: Port to use for connecting to the master
        """
        super().__init__(environment)
        self.retry = 0
        self.connected = False
        self.last_heartbeat_timestamp: float | None = None
        self.connection_event = asyncio.Event()
        self.worker_state = STATE_INIT
        self.client_id = socket.gethostname() + "_" + uuid4().hex
        self.master_host = master_host
        self.master_port = master_port
        self.web_base_path = environment.parsed_options.web_base_path if environment.parsed_options else ""
        self.logs: list[str] = []
        self.worker_cpu_warning_emitted = False
        self._users_dispatcher: UsersDispatcher | None = None
        self.client = rpc.Client(master_host, master_port, self.client_id)
        # background tasks - will be started when the event loop is running
        self._worker_task: asyncio.Task | None = None
        self._heartbeat_task: asyncio.Task | None = None
        self._heartbeat_timeout_task: asyncio.Task | None = None
        self._stats_reporter_task: asyncio.Task | None = None
        self._logs_reporter_task: asyncio.Task | None = None

        # register listener that adds the current number of spawned users to the report that is sent to the master node
        def on_report_to_master(client_id: str, data: dict[str, Any]):
            data["user_classes_count"] = self.user_classes_count
            data["user_count"] = self.user_count

        self.environment.events.report_to_master.add_listener(on_report_to_master)

        # register listener that sends quit message to master
        def on_quitting(environment: Environment, **kw) -> None:
            self.client.send(Message("quit", None, self.client_id))

        self.environment.events.quitting.add_listener(on_quitting)

        # register listener that's sends user exceptions to master
        def on_user_error(user_instance: User, exception: Exception, tb: TracebackType) -> None:
            formatted_tb = "".join(traceback.format_tb(tb))
            self.client.send(Message("exception", {"msg": str(exception), "traceback": formatted_tb}, self.client_id))

        self.environment.events.user_error.add_listener(on_user_error)

    def start_background_tasks(self) -> None:
        """Start background tasks for worker. Called when event loop is running."""
        self._worker_task = asyncio.create_task(self.worker())
        self._worker_task.add_done_callback(locust_exception_handler(self.environment))
        self._heartbeat_task = asyncio.create_task(self.heartbeat())
        self._heartbeat_task.add_done_callback(locust_exception_handler(self.environment))
        self._heartbeat_timeout_task = asyncio.create_task(self.heartbeat_timeout_checker())
        self._heartbeat_timeout_task.add_done_callback(locust_exception_handler(self.environment))
        self._stats_reporter_task = asyncio.create_task(self.stats_reporter())
        self._stats_reporter_task.add_done_callback(locust_exception_handler(self.environment))
        self._logs_reporter_task = asyncio.create_task(self.logs_reporter())
        self._logs_reporter_task.add_done_callback(locust_exception_handler(self.environment))

    async def connect_to_master_async(self) -> None:
        """Async version of connect_to_master"""
        self.retry += 1
        await asyncio.to_thread(self.client.send, Message("client_ready", __version__, self.client_id))
        try:
            await asyncio.wait_for(self.connection_event.wait(), timeout=CONNECT_TIMEOUT)
        except asyncio.TimeoutError:
            if self.retry < 30 / CONNECT_TIMEOUT:  # lower log level during the first 30 seconds
                logger.debug(
                    f"Failed to connect to master {self.master_host}:{self.master_port}{self.web_base_path}, retry {self.retry}/{CONNECT_RETRY_COUNT}."
                )
            else:
                logger.info(
                    f"Failed to connect to master {self.master_host}:{self.master_port}{self.web_base_path}, retry {self.retry}/{CONNECT_RETRY_COUNT}."
                )
            if self.retry > CONNECT_RETRY_COUNT:
                raise ConnectionError()
            await self.connect_to_master_async()
        except asyncio.CancelledError:
            sys.exit(1)
        self.connected = True

    def spawning_complete(self, user_count):
        assert user_count == sum(self.user_classes_count.values())
        self.client.send(
            Message(
                "spawning_complete",
                {"user_classes_count": self.user_classes_count, "user_count": self.user_count},
                self.client_id,
            )
        )
        self.worker_state = STATE_RUNNING

    async def start(
        self, user_count: int, spawn_rate: float, wait: bool = False, user_classes: list[type[User]] | None = None
    ) -> None:
        raise NotImplementedError("use start_worker")

    async def start_worker(self, user_classes_count: dict[str, int], **kwargs) -> None:
        """
        Start running a load test as a worker

        :param user_classes_count: Users to run
        """
        self.target_user_classes_count = user_classes_count
        self.target_user_count = sum(user_classes_count.values())

        for user_class in self.user_classes:
            if self.environment.host:
                user_class.host = self.environment.host

        user_classes_spawn_count: dict[str, int] = {}
        user_classes_stop_count: dict[str, int] = {}

        for user_class_name, user_class_count in user_classes_count.items():
            if self.user_classes_count[user_class_name] > user_class_count:
                user_classes_stop_count[user_class_name] = self.user_classes_count[user_class_name] - user_class_count
            elif self.user_classes_count[user_class_name] < user_class_count:
                user_classes_spawn_count[user_class_name] = user_class_count - self.user_classes_count[user_class_name]

        # call spawn_users before stopping the users since stop_users
        # can be blocking because of the stop_timeout
        await self.spawn_users(user_classes_spawn_count)
        await self.stop_users(user_classes_stop_count)
        await asyncio.to_thread(self.spawning_complete, sum(self.user_classes_count.values()))
        self.update_state(STATE_RUNNING)
        self.worker_state = STATE_RUNNING

    async def heartbeat(self) -> NoReturn:
        while True:
            try:
                await asyncio.to_thread(
                    self.client.send,
                    Message(
                        "heartbeat",
                        {
                            "state": self.worker_state,
                            "current_cpu_usage": self.current_cpu_usage,
                            "current_memory_usage": self.current_memory_usage,
                        },
                        self.client_id,
                    )
                )
            except RPCError as e:
                logger.error(f"RPCError found when sending heartbeat: {e}")
                await asyncio.to_thread(self.reset_connection)
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def heartbeat_timeout_checker(self) -> NoReturn:
        while True:
            await asyncio.sleep(1)
            if self.last_heartbeat_timestamp and self.last_heartbeat_timestamp < time.time() - MASTER_HEARTBEAT_TIMEOUT:
                logger.error(f"Didn't get heartbeat from master in over {MASTER_HEARTBEAT_TIMEOUT}s")
                self.quit()

    def reset_connection(self) -> None:
        logger.info("Reset connection to master")
        try:
            self.client.close()
            self.client = rpc.Client(self.master_host, self.master_port, self.client_id)
        except RPCError as e:
            logger.error(f"Temporary failure when resetting connection: {e}, will retry later.")

    async def worker(self) -> NoReturn:
        self.last_received_spawn_timestamp = 0
        while True:
            try:
                msg = await asyncio.to_thread(self.client.recv)
            except RPCError as e:
                logger.error(f"RPCError found when receiving from master: {e}")
            else:
                await self.handle_message(msg)

    async def handle_message(self, msg: Message) -> None:
        match msg.type:
            case "ack":
                # backward-compatible support of masters that do not send a worker index
                if msg.data is not None and "index" in msg.data:
                    self.worker_index = msg.data["index"]
                self.connection_event.set()
            case "spawn":
                await asyncio.to_thread(self.client.send, Message("spawning", None, self.client_id))
                job = msg.data
                if job["timestamp"] <= self.last_received_spawn_timestamp:
                    logger.info(
                        "Discard spawn message with older or equal timestamp than timestamp of previous spawn message"
                    )
                    return
                self.environment.host = job["host"]
                self.environment.stop_timeout = job["stop_timeout"] or 0.0

                # receive custom arguments
                if self.environment.parsed_options is None:
                    default_parser = argument_parser.get_empty_argument_parser()
                    argument_parser.setup_parser_arguments(default_parser)
                    self.environment.parsed_options = default_parser.parse(args=[])
                custom_args_from_master = {
                    k: v
                    for k, v in job["parsed_options"].items()
                    if k not in argument_parser.default_args_dict()
                    # these settings are sometimes needed on workers
                    or k in ["expect_workers", "tags", "exclude_tags"]
                }
                vars(self.environment.parsed_options).update(custom_args_from_master)

                if self.worker_state != STATE_RUNNING and self.worker_state != STATE_SPAWNING:
                    self.stats.clear_all()
                    self.exceptions = {}
                    self.cpu_warning_emitted = False
                    self.worker_cpu_warning_emitted = False
                    self.environment._filter_tasks_by_tags()
                    self.environment.events.test_start.fire(environment=self.environment)

                self.worker_state = STATE_SPAWNING

                if self.spawning_task and not self.spawning_task.done():
                    # cancel existing spawning task before we launch new one
                    self.spawning_task.cancel()
                    try:
                        await self.spawning_task
                    except asyncio.CancelledError:
                        pass
                self.spawning_task = asyncio.create_task(self.start_worker(job["user_classes_count"]))
                self.spawning_task.add_done_callback(locust_exception_handler(self.environment))
                self.last_received_spawn_timestamp = job["timestamp"]
            case "stop":
                await self.stop()
                await asyncio.to_thread(self.client.send, Message("client_stopped", None, self.client_id))
                # +additional_wait is just a small buffer to account for the random network latencies and/or other
                # random delays inherent to distributed systems.
                additional_wait = int(os.getenv("LOCUST_WORKER_ADDITIONAL_WAIT_BEFORE_READY_AFTER_STOP", 0))
                await asyncio.sleep((self.environment.stop_timeout or 0) + additional_wait)
                await asyncio.to_thread(self.client.send, Message("client_ready", __version__, self.client_id))
                self.worker_state = STATE_INIT
            case "quit":
                logger.info("Got quit message from master, shutting down...")
                await self.stop()
                self._send_stats()  # send a final report, in case there were any samples not yet reported
                await self._cancel_background_tasks()
            case "reconnect":
                logger.warning("Received reconnect message from master. Resetting RPC connection.")
                await asyncio.to_thread(self.reset_connection)
            case "heartbeat":
                self.last_heartbeat_timestamp = time.time()
                self.environment.events.heartbeat_received.fire(
                    client_id=msg.node_id, timestamp=self.last_heartbeat_timestamp
                )
            case "update_user_class":
                self.environment.update_user_class(msg.data)
            case "spawning_complete":
                # master says we have finished spawning (happens only once during a normal rampup)
                self.environment.events.spawning_complete.fire(user_count=msg.data["user_count"])
            case _ if lc := self.custom_messages.get(msg.type):
                listener, concurrent = lc
                logger.debug(f"Received {msg.type} message from master")
                if not concurrent:
                    listener(environment=self.environment, msg=msg)
                else:
                    asyncio.create_task(asyncio.to_thread(listener, environment=self.environment, msg=msg))
            case _:
                logger.warning(f"Unknown message type received: {msg.type}")

    async def _cancel_background_tasks(self) -> None:
        """Cancel all background tasks"""
        tasks = [
            self._worker_task, self._heartbeat_task, self._heartbeat_timeout_task,
            self._stats_reporter_task, self._logs_reporter_task
        ]
        for task in tasks:
            if task and not task.done():
                task.cancel()
        await asyncio.gather(*[t for t in tasks if t], return_exceptions=True)

    async def stats_reporter(self) -> NoReturn:
        while True:
            try:
                await asyncio.to_thread(self._send_stats)
            except RPCError as e:
                logger.error(f"Temporary connection lost to master server: {e}, will retry later.")
            await asyncio.sleep(WORKER_REPORT_INTERVAL)

    async def logs_reporter(self) -> None:
        if WORKER_LOG_REPORT_INTERVAL < 0:
            return

        while True:
            current_logs = get_logs()

            if (len(current_logs) - len(self.logs)) > 70:
                logger.warning(
                    "The worker attempted to send more than 70 log lines in one interval. Further log sending was disabled for this worker."
                )
                self._send_logs(get_logs())
                break
            if len(current_logs) > len(self.logs):
                self._send_logs(current_logs)

            self.logs = current_logs
            await asyncio.sleep(WORKER_LOG_REPORT_INTERVAL)

    def send_message(self, msg_type: str, data: dict[str, Any] | None = None, client_id: str | None = None) -> None:
        """
        Sends a message to master node

        :param msg_type: The type of the message to send
        :param data: Optional data to send
        :param client_id: (unused)
        """
        logger.debug(f"Sending {msg_type} message to master")
        self.client.send(Message(msg_type, data, self.client_id))

    def _send_stats(self) -> None:
        data: dict[str, Any] = {}
        self.environment.events.report_to_master.fire(client_id=self.client_id, data=data)
        self.client.send(Message("stats", data, self.client_id))

    def _send_logs(self, current_logs) -> None:
        self.send_message("logs", {"worker_id": self.client_id, "logs": current_logs})



def _format_user_classes_count_for_log(user_classes_count: dict[str, int]) -> str:
    return "{} ({} total users)".format(  # noqa: UP032
        json.dumps(dict(sorted(user_classes_count.items(), key=itemgetter(0)))),
        sum(user_classes_count.values()),
    )


def _aggregate_dispatched_users(d: dict[str, dict[str, int]]) -> dict[str, int]:
    # TODO: Test it
    user_classes = list(next(iter(d.values())).keys())
    return {u: sum(d[u] for d in d.values()) for u in user_classes}
