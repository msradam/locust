from __future__ import annotations

from locust.clients import HttpSession
from locust.exception import CatchResponseError, StopTest, StopUser
from locust.user.task import (
    LOCUST_STATE_RUNNING,
    LOCUST_STATE_STOPPING,
    LOCUST_STATE_WAITING,
    DefaultTaskSet,
    TaskSet,
    get_tasks_from_base_classes,
)
from locust.user.wait_time import constant
from locust.util import deprecation

import asyncio
import inspect
import logging
import sys
import time
import traceback
from collections.abc import Callable
from typing import TYPE_CHECKING, final

from requests.exceptions import RequestException
from urllib3 import PoolManager

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    import pytest

logger = logging.getLogger(__name__)


class UserMeta(type):
    """
    Meta class for the main User class. It's used to allow User classes to specify task execution
    ratio using an {task:int} dict, or a [(task0,int), ..., (taskN,int)] list.
    """

    def __new__(mcs, classname, bases, class_dict):
        # gather any tasks that is declared on the class (or it's bases)
        tasks = get_tasks_from_base_classes(bases, class_dict)
        class_dict["tasks"] = tasks

        if not class_dict.get("abstract"):
            # Not a base class
            class_dict["abstract"] = False

        deprecation.check_for_deprecated_task_set_attribute(class_dict)

        return type.__new__(mcs, classname, bases, class_dict)


class User(metaclass=UserMeta):
    """
    Represents a "user" which is to be spawned and attack the system that is to be load tested.

    The behaviour of this user is defined by its tasks. Tasks can be declared either directly on the
    class by using the :py:func:`@task decorator <locust.task>` on methods, or by setting
    the :py:attr:`tasks attribute <locust.User.tasks>`.

    This class should usually be subclassed by a class that defines some kind of client. For
    example when load testing an HTTP system, you probably want to use the
    :py:class:`HttpUser <locust.HttpUser>` class.
    """

    host: str | None = None
    """Base hostname to swarm. i.e: http://127.0.0.1:1234"""

    min_wait = None
    """Deprecated: Use wait_time instead. Minimum waiting time between the execution of locust tasks"""

    max_wait = None
    """Deprecated: Use wait_time instead. Maximum waiting time between the execution of locust tasks"""

    wait_time = constant(0)
    """
    Method that returns the time (in seconds) between the execution of locust tasks.
    Can be overridden for individual TaskSets.

    Example::

        from locust import User, between
        class MyUser(User):
            wait_time = between(3, 25)
    """

    wait_function = None
    """
    .. warning::

        DEPRECATED: Use wait_time instead. Note that the new wait_time method should return seconds and not milliseconds.

    Method that returns the time between the execution of locust tasks in milliseconds
    """

    tasks: list[TaskSet | Callable] = []
    """
    Collection of python callables and/or TaskSet classes that the Locust user(s) will run.

    If tasks is a list, the task to be performed will be picked randomly.

    If tasks is a *(callable,int)* list of two-tuples, or a {callable:int} dict,
    the task to be performed will be picked randomly, but each task will be weighted
    according to its corresponding int value. So in the following case, *ThreadPage* will
    be fifteen times more likely to be picked than *write_post*::

        class ForumPage(TaskSet):
            tasks = {ThreadPage:15, write_post:1}
    """

    weight: float = 1
    """Probability of user class being chosen. The higher the weight, the greater the chance of it being chosen."""

    fixed_count: int = 0
    """
    If the value > 0, the weight property will be ignored and the 'fixed_count'-instances will be spawned.
    These Users are spawned first. If the total target count (specified by the --users arg) is not enough
    to spawn all instances of each User class with the defined property, the final count of each User is undefined.
    """

    abstract: bool = True
    """If abstract is True, the class is meant to be subclassed, and locust will not spawn users of this class during a test."""

    def __init__(self, environment) -> None:
        super().__init__()
        self.environment = environment
        """A reference to the :py:class:`Environment <locust.env.Environment>` in which this user is running"""
        self._state: str | None = None
        self._task: asyncio.Task | None = None
        self._taskset_instance: TaskSet | None = None
        self._cp_last_run = time.time()  # used by constant_pacing wait_time

    def on_start(self) -> None:
        """
        Called when a User starts running.
        Can be either sync or async.
        """
        pass

    def on_stop(self):
        """
        Called when a User stops running (is killed).
        Can be either sync or async.
        """
        pass

    async def _call_on_start(self):
        """Call on_start, handling both sync and async implementations"""
        result = self.on_start()
        if inspect.iscoroutine(result):
            await result

    async def _call_on_stop(self):
        """Call on_stop, handling both sync and async implementations"""
        result = self.on_stop()
        if inspect.iscoroutine(result):
            await result

    @final
    async def run(self):
        self._state = LOCUST_STATE_RUNNING
        self._taskset_instance = DefaultTaskSet(self)
        try:
            try:
                await self._call_on_start()
            except Exception as e:
                logger.error("%s\n%s", e, traceback.format_exc())
                raise

            await self._taskset_instance.run()
        except (asyncio.CancelledError, StopUser, StopTest):
            await self._call_on_stop()

    async def wait(self):
        """
        Make the running user sleep for a duration defined by the User.wait_time
        function.

        The user can also be killed gracefully while it's sleeping, so calling this
        method within a task makes it possible for a user to be killed mid-task even if you've
        set a stop_timeout. If this behaviour is not desired, you should make the user wait using
        asyncio.sleep() instead.
        """
        await self._taskset_instance.wait()

    def start(self, task_group: asyncio.TaskGroup) -> asyncio.Task:
        """
        Start an asyncio task that runs this User instance.

        :param task_group: TaskGroup instance where the task will be created.
        :returns: The created task.
        """
        self._task = task_group.create_task(self.run())
        return self._task

    def stop(self, force: bool = False):
        """
        Stop the user task.

        :param force: If False (the default) the stopping is done gracefully by setting the state to LOCUST_STATE_STOPPING
                      which will make the User instance stop once any currently running task is complete and on_stop
                      methods are called. If force is True the task will be cancelled immediately.
        :returns: True if the task was cancelled immediately, otherwise False
        """
        if force or self._state == LOCUST_STATE_WAITING:
            self._task.cancel()
            return True
        elif self._state == LOCUST_STATE_RUNNING:
            self._state = LOCUST_STATE_STOPPING
            return False
        else:
            raise Exception(f"Tried to stop User in an unexpected state: {self._state}. This should never happen.")

    @property
    def task(self):
        return self._task

    def context(self) -> dict:
        """
        Adds the returned value (a dict) to the context for :ref:`request event <request_context>`.
        Override this in your User class to customize the context.
        """
        return {}

    @classmethod
    def json(cls):
        return {
            "host": cls.host,
            "weight": cls.weight,
            "fixed_count": cls.fixed_count,
            "tasks": [task.__name__ for task in cls.tasks],
        }

    @classmethod
    def fullname(cls) -> str:
        """Fully qualified name of the user class, e.g. my_package.my_module.MyUserClass"""
        return ".".join(filter(lambda x: x != "<locals>", (cls.__module__ + "." + cls.__qualname__).split(".")))


class HttpUser(User):
    """
    Represents an HTTP "user" which is to be spawned and attack the system that is to be load tested.

    The behaviour of this user is defined by its tasks. Tasks can be declared either directly on the
    class by using the :py:func:`@task decorator <locust.task>` on methods, or by setting
    the :py:attr:`tasks attribute <locust.User.tasks>`.

    This class creates a *client* attribute on instantiation which is an HTTP client with support
    for keeping a user session between requests.
    """

    abstract: bool = True
    """If abstract is True, the class is meant to be subclassed, and users will not choose this locust during a test"""

    pool_manager: PoolManager | None = None
    """Connection pool manager to use. If not given, a new manager is created per single user."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.host is None:
            raise StopTest(
                "You must specify the base host. Either in the host attribute in the User class, or on the command line using the --host option."
            )

        self.client = HttpSession(
            base_url=self.host,
            request_event=self.environment.events.request,
            user=self,
            pool_manager=self.pool_manager,
        )
        """
        Instance of HttpSession that is created upon instantiation of Locust.
        The client supports cookies, and therefore keeps the session between HTTP requests.
        """
        self.client.trust_env = False


class PytestUser(User):
    abstract = True
    functions: list[pytest.Function]
    fixtures: list

    @override
    def run(self):  # type: ignore[override] # We actually DO want to change the default User behavior
        self._state = LOCUST_STATE_RUNNING
        self.fixtures = [next(f.fixturedef.func(self)) for f in self.functions]  # type: ignore[attr-defined]
        while True:
            for i in range(len(self.fixtures)):
                try:  # try-except is for supporting .raise_for_status() in tests
                    self.functions[i].obj(self.fixtures[i])
                except RequestException as e:
                    if isinstance(e, ValueError):  # things like MissingSchema etc, lets not catch that
                        raise
                    logger.debug("%s\n%s", e, traceback.format_exc())
                except CatchResponseError as e:
                    logger.debug("%s\n%s", e, traceback.format_exc())
                except OSError as e:  # includes ConnectionError
                    logger.debug("%s\n%s", e, traceback.format_exc())
