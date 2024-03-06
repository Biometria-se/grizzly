"""Locust glue that starts a load test based on conditions specified in the feature file."""
from __future__ import annotations

import itertools
import logging
import subprocess
import sys
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass
from math import ceil, floor
from operator import attrgetter, itemgetter
from os import environ
from signal import SIGINT, SIGTERM, Signals
from time import perf_counter
from typing import TYPE_CHECKING, Any, Callable, Dict, Generator, Iterator, List, NoReturn, Optional, Set, SupportsIndex, Tuple, Type, TypeVar, cast

import gevent
from jinja2.exceptions import TemplateError
from locust import events
from locust import stats as lstats
from locust.dispatch import UsersDispatcher
from locust.log import setup_logging
from locust.user.users import User
from locust.util.timespan import parse_timespan
from roundrobin import smooth

from . import __locust_version__, __version__
from .context import GrizzlyContext
from .listeners import init, init_statistics_listener, locust_test_start, locust_test_stop, quitting, spawning_complete, validate_result
from .testdata.utils import initialize_testdata
from .types import RequestType, TestdataType
from .types.behave import Context, Status
from .types.locust import Environment, LocustRunner, MasterRunner, Message, MessageHandler, WorkerRunner
from .users import GrizzlyUser
from .utils import create_scenario_class_type, create_user_class_type

__all__ = [
    'stats_logger',
    'UsersDispatcher',
]


if TYPE_CHECKING:
    from locust.runners import WorkerNode


unhandled_greenlet_exception: bool = False
abort_test: bool = False


logger = logging.getLogger('grizzly.locust')

stats_logger = logging.getLogger('locust.stats_logger')

T = TypeVar('T')


class LengthOptimizedList(List[T]):
    """Simple implementation of a list that keeps track of its length for speed optimizations."""

    __optimized_length__: int

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        self.__optimized_length__ = 0

    def append(self, __object: Any) -> None:
        self.__optimized_length__ += 1
        super().append(__object)

    def pop(self, __index: SupportsIndex = -1) -> Any:
        self.__optimized_length__ -= 1
        return super().pop(__index)

    def __getattribute__(self, __name: str) -> Any:
        if __name not in ['append', 'pop', '__optimized_length__', '__class__']:
            message = f'{__name} is not implemented'
            raise NotImplementedError(message)
        return super().__getattribute__(__name)

    def __len__(self) -> int:
        return self.__optimized_length__


class FixedUsersDispatcher(UsersDispatcher):
    """Fixed count (only) based iterator that dispatches users to the workers.

    Distribution is based on fixed count (`User.fixed_count`), it is also possible to group certain user types to the same
    workers by using `User.sticky_tag`. Available workers will then be assigned to one of the tags, and only spawn users with
    the same tag value.

    `User.weight`, if set on the user type, will be ignored with this dispatcher.
    """

    def __init__(self, worker_nodes: list[WorkerNode], user_classes: list[type[GrizzlyUser]]) -> None:
        self._worker_nodes = worker_nodes
        self._sort_workers()
        self._user_classes = sorted(user_classes, key=attrgetter('__name__'))
        self._original_user_classes = self._user_classes.copy()

        assert len(user_classes) > 0
        assert len(set(self._user_classes)) == len(self._user_classes)

        self._initial_users_on_workers = {
            worker_node.id: {user_class.__name__: 0 for user_class in self._user_classes}
            for worker_node in worker_nodes
        }

        self._users_on_workers = self._fast_users_on_workers_copy(self._initial_users_on_workers)

        # To keep track of how long it takes for each dispatch iteration to compute
        self._dispatch_iteration_durations: list[float] = []

        self._dispatch_in_progress: bool = False

        self._dispatcher_generator: Generator[dict[str, dict[str, int]], None, None]

        self._user_generator = self._create_user_generator()

        self._user_count_per_dispatch_iteration: int

        self._active_users: LengthOptimizedList[tuple[WorkerNode, str]] = LengthOptimizedList()

        self._spawn_rate: float

        self._wait_between_dispatch: float

        self._rebalance: bool = False

        self._no_user_to_spawn: bool = False

        for user_class in user_classes:
            if not hasattr(user_class, 'sticky_tag'):
                user_class.sticky_tag = None

        self._users_to_sticky_tag = {
            user_class.__name__: user_class.sticky_tag or '__orphan__' for user_class in user_classes
        }

        self._user_class_name_to_type = {user_class.__name__: user_class for user_class in user_classes}

        self._workers_to_sticky_tag: dict[WorkerNode, str] = {}

        self._sticky_tag_to_workers: dict[str, itertools.cycle[WorkerNode]] = {}
        self.__sticky_tag_to_workers: dict[str, list[WorkerNode]] = {}

        # make sure there are not more sticky tags than worker nodes
        assert len(set(self._users_to_sticky_tag.values())) <= len(worker_nodes)

        self.__target_user_count_length__: int | None = None
        self.target_user_count = {user_class.__name__: user_class.fixed_count for user_class in self._user_classes}

        self._grizzly_current_user_count: dict[str, int] = {user_class.__name__: 0 for user_class in self._user_classes}

    def _sort_workers(self) -> None:
        # Sorting workers ensures repeatable behaviour
        worker_nodes_by_id = sorted(self._worker_nodes, key=lambda w: w.id)

        # Give every worker an index indicating how many workers came before it on that host
        workers_per_host: dict[str, int] = defaultdict(int)
        for worker_node in worker_nodes_by_id:
            host = worker_node.id.split("_")[0]
            worker_node._index_within_host = workers_per_host[host]  # type: ignore[attr-defined]
            workers_per_host[host] = workers_per_host[host] + 1

        # Sort again, first by index within host, to ensure Users get started evenly across hosts
        self._worker_nodes = sorted(self._worker_nodes, key=lambda worker: (worker._index_within_host, worker.id))  # type: ignore[attr-defined]

    @staticmethod
    def _fast_users_on_workers_copy(users_on_workers: dict[str, dict[str, int]]) -> dict[str, dict[str, int]]:
        """Builtin `copy.deepcopy` is too slow, so we use this custom copy function.

        The implementation was profiled and compared to other implementations such as dict-comprehensions
        and the one below is the most efficient.
        """
        # type is ignored due to: https://github.com/python/mypy/issues/1507
        return dict(zip(users_on_workers.keys(), map(dict.copy, users_on_workers.values())))  # type: ignore[arg-type]

    def __next__(self) -> dict[str, dict[str, int]]:
        users_on_workers = next(self._dispatcher_generator)
        return self._fast_users_on_workers_copy(users_on_workers)

    def get_target_user_count(self) -> int:
        if self.__target_user_count_length__ is None:
            self.__target_user_count_length__ = sum(self._grizzly_target_user_count.values())

        return self.__target_user_count_length__

    @property
    def target_user_count(self) -> dict[str, int]:
        return self._grizzly_target_user_count

    @target_user_count.setter
    def target_user_count(self, value: dict[str, int]) -> None:
        self.__target_user_count_length__ = None
        self._grizzly_target_user_count = dict(sorted(value.items(), key=itemgetter(1), reverse=True))

    @property
    def dispatch_in_progress(self) -> bool:
        return self._dispatch_in_progress

    @property
    def dispatch_iteration_durations(self) -> list[float]:
        return self._dispatch_iteration_durations

    def _get_user_current_count(self, user: str) -> int:
        return sum([users_on_node.get(user, 0) for users_on_node in self._users_on_workers.values()])

    def add_worker(self, worker_node: WorkerNode) -> None:
        """Call when a new worker connects to the master.
        When a new worker is added, the users dispatcher will flag that a rebalance is required
        and ensure that the next dispatch iteration will be made to redistribute the users
        on the new pool of workers.

        :param worker_node: The worker node to add.
        """
        self._worker_nodes.append(worker_node)
        self._sort_workers()
        self._prepare_rebalance()

    def remove_worker(self, worker_node: WorkerNode) -> None:
        """Similar to the above `add_worker`.
        When a worker disconnects (because of e.g. network failure, worker failure, etc.),
        this method will ensure that the next dispatch iteration redistributes the users on the
        remaining workers.

        :param worker_node: The worker node to remove.
        """
        self._worker_nodes = [w for w in self._worker_nodes if w.id != worker_node.id]
        if len(self._worker_nodes) == 0:
            return
        self._prepare_rebalance()

    def _prepare_rebalance(self) -> None:
        """When a rebalance is required because of added and/or removed workers, we compute the desired state as if
        we started from 0 user. So, if we were currently running 500 users, then the `_distribute_users` will
        perform a fake ramp-up without any waiting and return the final distribution.
        """
        self._spread_sticky_tags_on_workers()

        # Reset users before recalculating since the current users is used to calculate how many
        # fixed users to add.
        self._users_on_workers = {
            worker_node.id: {user_class.__name__: 0 for user_class in self._original_user_classes}
            for worker_node in self._worker_nodes
        }

        users_on_workers, user_gen, active_users = self._grizzly_distribute_users(self._grizzly_current_user_count)

        self._users_on_workers = users_on_workers
        self._active_users = active_users

        # It's important to reset the generators by using the ones from `_distribute_users`
        # so that the next iterations are smooth and continuous.
        self._user_generator = user_gen

        self._rebalance = True

    def new_dispatch(
        self,
        target_user_count: int,
        spawn_rate: float,
        user_classes: list[type[User]] | None = None,
    ) -> None:
        """Initialize a new dispatch cycle.

        :param target_user_count: The desired user count, per user, at the end of the dispatch cycle
        :param spawn_rate: The spawn rate
        :param user_classes: The user classes to be used for the new dispatch
        """
        # this dispatcher does not care about target_user_count
        assert target_user_count == -1

        grizzly_user_classes = cast(Optional[List[Type[GrizzlyUser]]], user_classes)

        if grizzly_user_classes is not None and self._user_classes != sorted(grizzly_user_classes, key=attrgetter('__name__')):
            self._user_classes = sorted(grizzly_user_classes, key=attrgetter('__name__'))

            # map original user classes (supplied when users dispatcher was created), with additional new ones (might be duplicates)
            self._users_to_sticky_tag = {
                user_class.__name__: user_class.sticky_tag or '__orphan__'
                for user_class in cast(List[Type[GrizzlyUser]], self._original_user_classes + grizzly_user_classes)
            }

            self._user_class_name_to_type = {
                user_class.__name__: user_class for user_class in cast(List[Type[GrizzlyUser]], self._original_user_classes + grizzly_user_classes)
            }

            # only merge target user count for classes that has been specified in user classes
            grizzly_target_user_count = {user_class.__name__: user_class.fixed_count for user_class in grizzly_user_classes}
            self.target_user_count = {**self._grizzly_target_user_count, **grizzly_target_user_count}
        else:
            self.target_user_count = {user_class.__name__: user_class.fixed_count for user_class in self._user_classes}

        self._spawn_rate = spawn_rate

        self._user_count_per_dispatch_iteration = max(1, floor(self._spawn_rate))

        self._wait_between_dispatch = self._user_count_per_dispatch_iteration / self._spawn_rate

        self._spread_sticky_tags_on_workers()

        self._initial_users_on_workers = self._users_on_workers

        self._users_on_workers = self._fast_users_on_workers_copy(self._initial_users_on_workers)

        self._dispatcher_generator = self.dispatcher()

        self.dispatch_iteration_durations.clear()

        self._user_generator = self._create_user_generator()

    def get_current_user_count_total(self) -> int:
        return self._active_users.__optimized_length__

    def _get_current_user_count(self, user: str) -> int:
        return sum([users_on_worker.get(user, 0) for users_on_worker in self._users_on_workers.values()])

    def has_reached_target_user_count(self) -> bool:
        return self.get_current_user_count_total() == self.get_target_user_count()

    def is_below_target_user_count(self) -> bool:
        return self.get_current_user_count_total() < self.get_target_user_count()

    def is_above_target_user_count(self) -> bool:
        return self.get_current_user_count_total() > self.get_target_user_count()

    def _add_users_on_workers(self) -> dict[str, dict[str, int]]:
        """Add users on the workers until the target number of users is reached for the current dispatch iteration.

        :return: The users that we want to run on the workers
        """
        current_user_count_actual = self._active_users.__optimized_length__
        current_user_count_target = min(
            current_user_count_actual + self._user_count_per_dispatch_iteration, self.get_target_user_count(),
        )

        current_user_count: dict[str, int] = {}
        for user_counts in self._users_on_workers.values():
            for user_class_name, count in user_counts.items():
                current_user_count.update({user_class_name: current_user_count.get(user_class_name, 0) + count})

        for next_user_class_name in self._user_generator:
            if not next_user_class_name:
                self._no_user_to_spawn = True
                break

            if current_user_count[next_user_class_name] + 1 > self._grizzly_target_user_count[next_user_class_name]:
                continue

            sticky_tag = self._users_to_sticky_tag[next_user_class_name]
            worker_node = next(self._sticky_tag_to_workers[sticky_tag])
            self._users_on_workers[worker_node.id][next_user_class_name] += 1
            current_user_count_actual += 1
            current_user_count[next_user_class_name] += 1
            self._active_users.append((worker_node, next_user_class_name))

            if current_user_count_actual >= current_user_count_target:
                self._grizzly_current_user_count = current_user_count
                break

        return self._users_on_workers

    def _remove_users_from_workers(self) -> dict[str, dict[str, int]]:
        """Remove users from the workers until the target number of users is reached for the current dispatch iteration.

        :return: The users that we want to run on the workers
        """
        current_user_count_actual = self._active_users.__optimized_length__
        current_user_count_target = max(
            current_user_count_actual - self._user_count_per_dispatch_iteration, self.get_target_user_count(),
        )

        while True:
            try:
                worker_node, user = self._active_users.pop()
            except IndexError:
                return self._users_on_workers

            self._users_on_workers[worker_node.id][user] -= 1
            current_user_count_actual -= 1
            if current_user_count_actual == 0 or current_user_count_actual <= current_user_count_target:
                self._grizzly_current_user_count.clear()
                for user_counts in self._users_on_workers.values():
                    for user_class_name, count in user_counts.items():
                        self._grizzly_current_user_count.update(
                            {user_class_name: self._grizzly_current_user_count.get(user_class_name, 0) + count},
                        )
                return self._users_on_workers

    @contextmanager
    def _wait_between_dispatch_iteration_context(self) -> Generator[None, None, None]:
        t0_rel = perf_counter()

        # We don't use `try: ... finally: ...` because we don't want to sleep
        # if there's an exception within the context.
        yield

        delta = perf_counter() - t0_rel

        self._dispatch_iteration_durations.append(delta)

        if self.has_reached_target_user_count():
            # No sleep when this is the last dispatch iteration
            return

        sleep_duration = max(0.0, self._wait_between_dispatch - delta)
        gevent.sleep(sleep_duration)

    @staticmethod
    def _infinite_cycle_gen(value_weights: list[tuple[type[GrizzlyUser] | str, int]]) -> itertools.cycle[str | None]:
        if not value_weights:
            return itertools.cycle([None])

        # Normalize the weights so that the smallest weight will be equal to "target_min_weight".
        # The value "2" was experimentally determined because it gave a better distribution especially
        # when dealing with weights which are close to each others, e.g. 1.5, 2, 2.4, etc.
        target_min_weight = 2

        # 'Value' here means weight or fixed count
        try:
            min_value = min(weight for _, weight in value_weights if weight > 0)
        except ValueError:
            min_value = 0

        normalized_value_weights = [
            (
                getattr(value, '__name__', value),
                round(target_min_weight * weight / min_value) if min_value > 0 else 0,
            )
            for value, weight in value_weights
        ]
        generation_length_to_get_proper_distribution = sum(
            normalized_weight for _, normalized_weight in normalized_value_weights
        )
        gen = smooth(normalized_value_weights)

        # Instead of calling `gen()` for each user, we cycle through a generator of fixed-length
        # `generation_length_to_get_proper_distribution`. Doing so greatly improves performance because
        # we only ever need to call `gen()` a relatively small number of times. The length of this generator
        # is chosen as the sum of the normalized weights. So, for users A, B, C of weights 2, 5, 6, the length is
        # 2 + 5 + 6 = 13 which would yield the distribution `CBACBCBCBCABC` that gets repeated over and over
        # until the target user count is reached.
        return itertools.cycle(gen() for _ in range(generation_length_to_get_proper_distribution))

    def _spread_sticky_tags_on_workers(self) -> None:
        sticky_tag_user_count: dict[str, int] = {}

        # summarize target user count per sticky tag
        for user_class_name, sticky_tag in self._users_to_sticky_tag.items():
            user_count = self._grizzly_target_user_count.get(user_class_name, None)
            if user_count is None:
                continue

            user_count = sticky_tag_user_count.get(sticky_tag, 0) + user_count
            sticky_tag_user_count.update({sticky_tag: user_count})

        logger.debug('user count per sticky tag: %r', sticky_tag_user_count)

        worker_node_count = len(self._worker_nodes)
        # sort sticky tags based on number of users (more user types should have more workers)
        sticky_tags: dict[str, int] = dict(sorted(sticky_tag_user_count.items(), key=itemgetter(1), reverse=True))
        sticky_tag_count = len(sticky_tag_user_count)

        # not enough sticky tags per worker, so cycle sticky tags so all workers gets a tag
        sticky_tags_gen: Iterator[str | None]
        if worker_node_count > sticky_tag_count:
            # make sure each tag get at least one worker, then spread the remaining based on how many users that sticky tag has been assigned
            sticky_tags_gen = itertools.chain(
                sticky_tags.keys(),
                self._infinite_cycle_gen(list(sticky_tags.items())),
            )
        else:
            sticky_tags_gen = iter(sticky_tags.keys())

        # map worker to sticky tag
        self._workers_to_sticky_tag.clear()
        for worker, worker_sticky_tag in zip(self._worker_nodes, sticky_tags_gen):
            if worker_sticky_tag is None:
                continue
            self._workers_to_sticky_tag.update({worker: worker_sticky_tag})

        # map sticky tag to workers
        orig__sticky_tag_to_workers = self.__sticky_tag_to_workers.copy()
        self.__sticky_tag_to_workers.clear()
        for worker, sticky_tag in self._workers_to_sticky_tag.items():
            self.__sticky_tag_to_workers.update(
                {sticky_tag: self.__sticky_tag_to_workers.get(sticky_tag, []) + [worker]},  # noqa: RUF005
            )

        logger.debug(
            'workers per sticky tag: %r',
            {
                sticky_tag: [worker.id for worker in workers]
                for sticky_tag, workers in self.__sticky_tag_to_workers.items()
            },
        )

        # check if workers has changed since last time
        # do not reset worker cycles if only target user count has changed
        changes_for_sticky_tag: dict[str, list[WorkerNode] | None] = {}
        for sticky_tag in self.__sticky_tag_to_workers:
            workers = self.__sticky_tag_to_workers.get(sticky_tag, None)
            if workers is not None and orig__sticky_tag_to_workers.get(sticky_tag, []) != workers:
                changes_for_sticky_tag.update({sticky_tag: workers})
            elif workers is None:
                changes_for_sticky_tag.update({sticky_tag: None})
            else:  # nothing has changed, keep the worker cycle as it was
                pass

        # apply changes
        for sticky_tag, change in changes_for_sticky_tag.items():
            if change is not None:
                self._sticky_tag_to_workers.update({sticky_tag: itertools.cycle(change)})
            else:
                del self._sticky_tag_to_workers[sticky_tag]

    def _create_user_generator(self) -> Generator[str | None, None, None]:
        user_cycle: list[tuple[type[GrizzlyUser] | str, int]] = [
            (self._user_class_name_to_type[user_class_name], fixed_count)
            for user_class_name, fixed_count in self._grizzly_target_user_count.items()
        ]
        user_generator: itertools.cycle[str | None] = self._infinite_cycle_gen(user_cycle)

        while user_class_name := next(user_generator):
            if not user_class_name:
                break

            yield user_class_name

    def dispatcher(self) -> Generator[dict[str, dict[str, int]], None, None]:
        self._dispatch_in_progress = True

        if self._rebalance:
            self._rebalance = False
            yield self._users_on_workers
            if self.has_reached_target_user_count():
                return

        if self.has_reached_target_user_count():
            yield self._initial_users_on_workers
            self._dispatch_in_progress = False
            return

        while self.is_below_target_user_count():
            with self._wait_between_dispatch_iteration_context():
                yield self._add_users_on_workers()
                if self._rebalance:
                    self._rebalance = False
                    yield self._users_on_workers
                if self._no_user_to_spawn:
                    self._no_user_to_spawn = False
                    break

        while self.is_above_target_user_count():
            with self._wait_between_dispatch_iteration_context():
                yield self._remove_users_from_workers()
                if self._rebalance:
                    self._rebalance = False
                    yield self._users_on_workers

        self._dispatch_in_progress = False

    def _grizzly_distribute_users(
        self, target_user_count: dict[str, int],
    ) -> tuple[dict[str, dict[str, int]], Generator[str | None, None, None], LengthOptimizedList[tuple[WorkerNode, str]]]:
        """Distribute users on available workers, and continue user cycle from there."""
        # used target as setup based on user class values, without changing the original value

        if target_user_count == {}:
            target_user_count = {**self._grizzly_target_user_count}

        self._spread_sticky_tags_on_workers()

        user_gen = self._create_user_generator()

        users_on_workers = {
            worker_node.id: {user_class.__name__: 0 for user_class in self._original_user_classes}
            for worker_node in self._worker_nodes
        }

        active_users: LengthOptimizedList[tuple[WorkerNode, str]] = LengthOptimizedList()

        user_count_target = sum(target_user_count.values())
        current_user_count: dict[str, int] = {}
        for user_counts in users_on_workers.values():
            for user_class_name, count in user_counts.items():
                current_user_count.update({user_class_name: current_user_count.get(user_class_name, 0) + count})

        user_count_total = 0

        for next_user_class_name in user_gen:
            if not next_user_class_name:
                break

            if current_user_count[next_user_class_name] + 1 > target_user_count[next_user_class_name]:
                continue

            sticky_tag = self._users_to_sticky_tag[next_user_class_name]
            worker_node = next(self._sticky_tag_to_workers[sticky_tag])
            users_on_workers[worker_node.id][next_user_class_name] += 1
            user_count_total += 1
            current_user_count[next_user_class_name] += 1
            active_users.append((worker_node, next_user_class_name))

            if user_count_total >= user_count_target:
                self._grizzly_current_user_count = current_user_count
                break

        return users_on_workers, user_gen, active_users


def greenlet_exception_logger(logger: logging.Logger, level: int = logging.CRITICAL) -> Callable[[gevent.Greenlet], None]:
    def exception_handler(greenlet: gevent.Greenlet) -> None:
        global unhandled_greenlet_exception  # noqa: PLW0603
        message = f'unhandled exception in greenlet: {greenlet}: {greenlet.value}'
        logger.log(level, message, exc_info=True)
        unhandled_greenlet_exception = True

    return exception_handler


def on_master(context: Context) -> bool:
    value: bool = 'master' in context.config.userdata and context.config.userdata['master'].lower() == 'true'
    if value:
        environ['LOCUST_IS_MASTER'] = str(value).lower()

    return value


def on_worker(context: Context) -> bool:
    value: bool = 'worker' in context.config.userdata and context.config.userdata['worker'].lower() == 'true'
    if value:
        environ['LOCUST_IS_WORKER'] = str(value).lower()

    return value


def on_local(context: Context) -> bool:
    value: bool = not on_master(context) and not on_worker(context)
    if value:
        environ['LOCUST_IS_LOCAL'] = str(value).lower()

    return value


def setup_locust_scenarios(grizzly: GrizzlyContext) -> Tuple[List[Type[GrizzlyUser]], Set[str]]:
    user_classes: List[Type[GrizzlyUser]] = []

    scenarios = grizzly.scenarios()

    assert len(scenarios) > 0, 'no scenarios in feature'

    external_dependencies: Set[str] = set()
    distribution: Dict[str, int] = {}

    if grizzly.setup.dispatcher_class in [UsersDispatcher, None]:
        user_count = grizzly.setup.user_count or 0
        total_weight = sum([scenario.user.weight for scenario in scenarios])
        for scenario in scenarios:
            scenario_user_count = ceil(user_count * (scenario.user.weight / total_weight))
            distribution[scenario.name] = scenario_user_count

        total_user_count = sum(distribution.values())
        user_overflow = total_user_count - user_count

        assert len(distribution.keys()) <= user_count, (
            f"increase the number in step 'Given \"{user_count}\" users' "
            f"to at least {len(distribution.keys())}"
        )

        if user_overflow < 0:
            logger.warning('there should be %d users, but there will only be %d users spawned', user_count, total_user_count)

        while user_overflow > 0:
            for scenario_name in dict(sorted(distribution.items(), key=lambda d: d[1], reverse=True)):
                if distribution[scenario_name] <= 1:
                    continue

                distribution[scenario_name] -= 1
                user_overflow -= 1

                if user_overflow < 1:
                    break

    for scenario in scenarios:
        # Given a user of type "" load testing ""
        assert 'host' in scenario.context, f'variable "host" is not found in the context for {scenario.name}'
        assert len(scenario.tasks) > 0, f'no tasks has been added to {scenario.name}'

        fixed_count = distribution.get(scenario.name, None)
        user_class_type = create_user_class_type(scenario, grizzly.setup.global_context, fixed_count=fixed_count)
        user_class_type.host = scenario.context['host']

        external_dependencies.update(user_class_type.__dependencies__)

        # @TODO: how do we specify other type of grizzly.scenarios?
        scenario_type = create_scenario_class_type('IteratorScenario', scenario)
        scenario.name = scenario_type.__name__
        for task in scenario.tasks:
            scenario_type.populate(task)

            dependencies = getattr(task, '__dependencies__', None)
            if dependencies is not None:
                external_dependencies.update(dependencies)

        logger.debug(
            '%s/%s: tasks=%d, weight=%d, fixed_count=%d, sticky_tag=%s',
            user_class_type.__name__,
            scenario_type.__name__,
            len(scenario.tasks),
            user_class_type.weight,
            user_class_type.fixed_count,
            user_class_type.sticky_tag,
        )

        user_class_type.tasks = [scenario_type]
        user_classes.append(user_class_type)

    return user_classes, external_dependencies


def setup_resource_limits(context: Context) -> None:
    if sys.platform != 'win32' and not on_master(context):
        try:
            import resource
            minimum_open_file_limit = 10000
            current_open_file_limit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)

            if current_open_file_limit < minimum_open_file_limit:
                resource.setrlimit(resource.RLIMIT_NOFILE, (minimum_open_file_limit, resource.RLIM_INFINITY))
        except (ValueError, OSError):
            logger.warning(
                (
                    "system open file limit '%d' is below minimum setting '%d'. "
                    "it's not high enough for load testing, and the OS didn't allow locust to increase it by itself. "
                    "see https://github.com/locustio/locust/wiki/Installation#increasing-maximum-number-of-open-files-limit for more info."
                ), current_open_file_limit, minimum_open_file_limit,
            )


def setup_environment_listeners(context: Context) -> Tuple[Set[str], Dict[str, MessageHandler]]:
    grizzly = cast(GrizzlyContext, context.grizzly)

    environment = grizzly.state.locust.environment

    # make sure we don't have any listeners
    environment.events.init._handlers = []
    environment.events.test_start._handlers = []
    environment.events.test_stop._handlers = []
    environment.events.quitting._handlers = []

    # add standard listeners
    testdata: Optional[TestdataType] = None
    external_dependencies: Set[str] = set()
    message_handlers: Dict[str, MessageHandler] = {}

    # initialize testdata
    try:
        testdata, external_dependencies, message_handlers = initialize_testdata(grizzly)
    except TemplateError as e:
        message = f'error parsing request payload: {e}'
        logger.exception(message)
        raise AssertionError(message) from e

    if not on_worker(context):
        validate_results = False

        # only add the listener if there are any rules for validating results
        for scenario in grizzly.scenarios():
            validate_results = scenario.should_validate()
            if validate_results:
                break

        logger.debug('validate_results=%r', validate_results)

        if validate_results:
            environment.events.quitting.add_listener(validate_result(grizzly))

        environment.events.quitting.add_listener(quitting)

    environment.events.init.add_listener(init(grizzly, testdata))
    environment.events.test_start.add_listener(locust_test_start(grizzly))
    environment.events.test_stop.add_listener(locust_test_stop)

    if not on_master(context):
        environment.events.spawning_complete.add_listener(spawning_complete(grizzly))

    # And save statistics to "..."
    if grizzly.setup.statistics_url is not None:
        environment.events.init.add_listener(init_statistics_listener(grizzly.setup.statistics_url))

    return external_dependencies, message_handlers


def print_scenario_summary(grizzly: GrizzlyContext) -> None:
    def create_separator(max_length_iterations: int, max_length_status: int, max_length_description: int) -> str:
        separator: List[str] = []
        separator.append('-' * 5)
        separator.append('-|-')
        separator.append('-' * max_length_iterations)
        separator.append('|-')
        separator.append('-' * max_length_status)
        separator.append('-|-')
        separator.append('-' * max_length_description)
        separator.append('-|')

        return ''.join(separator)

    rows: List[str] = []
    max_length_description = len('description')
    max_length_iterations = len('iter')
    max_length_status = len('status')

    stats = grizzly.state.locust.environment.stats

    for scenario in grizzly.scenarios():
        stat = stats.get(scenario.locust_name, RequestType.SCENARIO())
        max_length_description = max(len(scenario.description or 'unknown'), max_length_description)
        max_length_iterations = max(len(f'{stat.num_requests}/{scenario.iterations or 0}'), max_length_iterations)
        max_length_status = max(len(Status.undefined.name) if stat.num_requests < 1 else len(Status.passed.name), max_length_status)

    for scenario in grizzly.scenarios():
        total_errors = 0
        for error in stats.errors.values():
            if error.name.startswith(scenario.identifier):
                total_errors += 1

        stat = stats.get(scenario.locust_name, RequestType.SCENARIO())
        if stat.num_requests > 0:
            if abort_test:
                status = Status.skipped
                stat.num_requests -= 1
            elif stat.num_failures == 0 and stat.num_requests == scenario.iterations and total_errors == 0:
                status = Status.passed
            else:
                status = Status.failed
        else:
            status = Status.undefined

        description = scenario.description or 'unknown'
        row = '{:5}   {:>{}}  {:{}}   {}'.format(
            scenario.identifier,
            f'{stat.num_requests}/{scenario.iterations}',
            max_length_iterations,
            status.name,
            max_length_status,
            description,
        )
        rows.append(row)

    print('Scenario')
    print('{:5}   {:>{}}  {:{}}   {}'.format('ident', 'iter', max_length_iterations, 'status', max_length_status, 'description'))
    separator = create_separator(max_length_iterations, max_length_status, max_length_description)
    print(separator)
    for row in rows:
        print(row)
    print(separator)


def grizzly_test_abort(*_args: Any, **_kwargs: Any) -> None:
    global abort_test  # noqa: PLW0603

    if not abort_test:
        abort_test = True

def shutdown_external_processes(processes: Dict[str, subprocess.Popen], greenlet: Optional[gevent.Greenlet]) -> None:
    if len(processes) < 1:
        return

    if greenlet is not None:
        greenlet.kill(block=False)

    stop_method = 'killing' if abort_test else 'stopping'

    for dependency, process in processes.items():
        logger.info('%s %s', stop_method, dependency)
        if sys.platform == 'win32':
            from signal import CTRL_BREAK_EVENT
            process.send_signal(CTRL_BREAK_EVENT)
        else:
            process.terminate()

        if not abort_test:
            process.wait()
        else:
            process.kill()
            process.returncode = 1

        logger.debug('process.returncode=%d', process.returncode)

    processes.clear()


def run(context: Context) -> int:  # noqa: C901, PLR0915, PLR0912
    grizzly = cast(GrizzlyContext, context.grizzly)

    log_level = 'DEBUG' if context.config.verbose else grizzly.setup.log_level

    csv_prefix: Optional[str] = context.config.userdata.get('csv-prefix', None)
    csv_interval: int = int(context.config.userdata.get('csv-interval', '1'))
    csv_flush_interval: int = int(context.config.userdata.get('csv-flush-iterval', '10'))

    # And locust log level is
    setup_logging(log_level, None)

    # make sure the user hasn't screwed up
    if on_master(context) and on_worker(context):
        logger.error('seems to be a problem with "behave" arguments, cannot be both master and worker')
        return 254

    if grizzly.setup.spawn_rate is None:
        logger.error('spawn rate is not set')
        return 254

    if grizzly.setup.dispatcher_class in [UsersDispatcher, None] and (grizzly.setup.user_count is None or grizzly.setup.user_count < 1):
        logger.error("step 'Given \"user_count\" users' is not in the feature file")
        return 254

    greenlet_exception_handler = greenlet_exception_logger(logger)

    watch_running_external_processes_greenlet: Optional[gevent.Greenlet] = None

    external_processes: Dict[str, subprocess.Popen] = {}

    user_classes, external_dependencies = setup_locust_scenarios(grizzly)

    assert len(user_classes) > 0, 'no users specified in feature'

    try:
        setup_resource_limits(context)

        if grizzly.setup.dispatcher_class is None:
            grizzly.setup.dispatcher_class = UsersDispatcher

        environment = Environment(
            user_classes=cast(List[Type[User]], user_classes),
            shape_class=None,
            events=events,
            stop_timeout=300,  # only wait at most?
            dispatcher_class=grizzly.setup.dispatcher_class,
        )

        runner: LocustRunner

        if on_master(context):
            host = '0.0.0.0'
            port = int(context.config.userdata.get('master-port', 5557))
            runner = environment.create_master_runner(
                master_bind_host=host,
                master_bind_port=port,
            )
            logger.debug('started master runner: %s:%d', host, port)
        elif on_worker(context):
            try:
                host = context.config.userdata.get('master-host', 'master')
                port = context.config.userdata.get('master-port', 5557)
                logger.debug('trying to connect to locust master: %s:%d', host, port)
                runner = environment.create_worker_runner(
                    host,
                    port,
                )
                logger.debug('connected to locust master: %s:%d', host, port)
            except OSError:
                logger.exception('failed to connect to locust master at %s:%d', host, port)
                return 1
        else:
            runner = environment.create_local_runner()

        grizzly.state.locust = runner

        variable_dependencies, message_handlers = setup_environment_listeners(context)
        external_dependencies.update(variable_dependencies)

        environment.events.init.fire(environment=environment, runner=runner, web_ui=None)

        if not on_master(context) and len(external_dependencies) > 0:
            env = environ.copy()
            if grizzly.state.verbose:
                env['GRIZZLY_EXTRAS_LOGLEVEL'] = 'DEBUG'

            parameters: Dict[str, Any] = {}
            if sys.platform == 'win32':
                parameters.update({'creationflags': subprocess.CREATE_NEW_PROCESS_GROUP})
            else:
                def preexec() -> None:
                    import os
                    os.setpgrp()

                parameters.update({'preexec_fn': preexec})

            for external_dependency in external_dependencies:
                logger.info('starting %s', external_dependency)
                external_processes.update({external_dependency: subprocess.Popen(
                    [external_dependency],
                    env=env,
                    shell=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    **parameters,
                )})

            def start_watching_external_processes(processes: Dict[str, subprocess.Popen]) -> Callable[[], None]:
                logger.info('making sure external processes are alive every 10 seconds')

                def watch_running_external_processes() -> None:
                    while runner.user_count > 0:
                        _processes = processes.copy()
                        for dependency, process in _processes.items():
                            if process.poll() is not None:
                                logger.error('%s is not running, restarting', dependency)
                                raise SystemExit(1)

                        gevent.sleep(10.0)

                    logger.info('stop watching external processes')

                return watch_running_external_processes

            watch_running_external_processes_greenlet = gevent.spawn_later(10, start_watching_external_processes(external_processes))
            watch_running_external_processes_greenlet.link_exception(greenlet_exception_handler)

        if not isinstance(runner, WorkerRunner):
            for message_type, callback in message_handlers.items():
                grizzly.state.locust.register_message(message_type, callback)
                logger.info('registered callback for message type "%s"', message_type)

            runner.register_message('client_aborted', grizzly_test_abort)

        main_greenlet = runner.greenlet

        # And run for maximum
        run_time: Optional[int] = None
        if grizzly.setup.timespan is not None and not on_worker(context):
            try:
                run_time = parse_timespan(grizzly.setup.timespan)
            except ValueError:
                logger.exception('invalid timespan "%s" expected: 20, 20s, 3m, 2h, 1h20m, 3h30m10s, etc.', grizzly.setup.timespan)
                return 1

        stats_printer_greenlet: Optional[gevent.Greenlet] = None

        @dataclass
        class LocustOption:
            headless: bool
            num_users: int
            spawn_rate: float
            tags: List[str]
            exclude_tags: List[str]
            enable_rebalancing: bool

        environment.parsed_options = LocustOption(
            headless=True,
            num_users=grizzly.setup.user_count or 0,
            spawn_rate=grizzly.setup.spawn_rate,
            tags=[],
            exclude_tags=[],
            enable_rebalancing=False,
        )

        if isinstance(runner, MasterRunner):
            expected_workers = int(context.config.userdata.get('expected-workers', 1))
            if grizzly.setup.user_count is not None:
                assert expected_workers <= grizzly.setup.user_count, f'there are more workers ({expected_workers}) than users ({grizzly.setup.user_count}), which is not supported'

            while len(runner.clients.ready) < expected_workers:
                logger.debug(
                    'waiting for workers to be ready, %d of %d', len(runner.clients.ready), expected_workers,
                )
                gevent.sleep(1)

            logger.info(
                'all %d workers have connected and are ready', expected_workers,
            )

        if not isinstance(runner, WorkerRunner):
            logger.info('starting locust-%s via grizzly-%s', __locust_version__, __version__)
            # user_count == -1 means that the dispatcher will use use class properties `fixed_count`
            user_count = grizzly.setup.user_count or 0 if runner.environment.dispatcher_class == UsersDispatcher else -1

            try:
                runner.start(user_count, grizzly.setup.spawn_rate)
            except:
                if isinstance(runner, MasterRunner):
                    runner.send_message('grizzly_worker_quit', None)
                raise

            stats_printer_greenlet = gevent.spawn(grizzly_stats_printer(environment.stats))
            stats_printer_greenlet.link_exception(greenlet_exception_handler)

        def spawn_run_time_limit_greenlet() -> None:
            def timelimit_stop() -> None:
                logger.info('time limit reached. stopping locust.')
                runner.quit()

            gevent.spawn_later(run_time, timelimit_stop).link_exception(greenlet_exception_handler)

        if run_time is not None:
            logger.info('run time limit set to %d seconds', run_time)
            spawn_run_time_limit_greenlet()

        gevent.spawn(lstats.stats_history, environment.runner)

        if csv_prefix is not None:
            lstats.CSV_STATS_INTERVAL_SEC = csv_interval
            lstats.CSV_STATS_FLUSH_INTERVAL_SEC = csv_flush_interval
            stats_csv_writer = lstats.StatsCSVFileWriter(
                environment, lstats.PERCENTILES_TO_REPORT, csv_prefix, full_history=True,
            )
            gevent.spawn(stats_csv_writer.stats_writer).link_exception(greenlet_exception_handler)

        if not isinstance(runner, WorkerRunner):
            watch_running_users_greenlet: Optional[gevent.Greenlet] = None

            def watch_running_users() -> None:
                count = 0
                while runner.user_count > 0:
                    gevent.sleep(1.0)
                    count += 1
                    if count % 10 == 0:
                        logger.debug('runner.user_count=%d, runner.user_classes_count=%r', runner.user_count, runner.user_classes_count)
                        count = 0

                logger.info('runner.user_count=%d, quit %s, abort_test=%r', runner.user_count, runner.__class__.__name__, abort_test)
                # has already been fired if abort_test = True
                if not abort_test:
                    runner.environment.events.quitting.fire(environment=runner.environment, reverse=True)

                if isinstance(runner, MasterRunner):
                    runner.send_message('grizzly_worker_quit', None)
                    runner.stop(send_stop_to_client=False)

                    # wait for all clients to quit
                    while len(runner.clients.all) > 0:
                        gevent.sleep(0.5)

                    runner.greenlet.kill(block=True)
                else:
                    runner.quit()

                if stats_printer_greenlet is not None:
                    stats_printer_greenlet.kill(block=False)

                if watch_running_users_greenlet is not None:
                    watch_running_users_greenlet.kill(block=False)

                grizzly_print_stats(runner.stats, current=False)
                grizzly_print_percentile_stats(runner.stats)
                lstats.print_error_report(runner.stats)
                print_scenario_summary(grizzly)

                # make sure everything is flushed
                for handler in stats_logger.handlers:
                    handler.flush()

            def spawning_complete() -> bool:
                if isinstance(runner, MasterRunner):
                    return runner.spawning_completed

                return grizzly.state.spawning_complete

            while not spawning_complete():
                logger.debug('spawning not completed...')
                gevent.sleep(1.0)

            logger.info('all users spawn, start watching user count')

            watch_running_users_greenlet = gevent.spawn(watch_running_users)
            watch_running_users_greenlet.link_exception(greenlet_exception_handler)

            # stop when user_count reaches 0
            main_greenlet = watch_running_users_greenlet

        def sig_handler(signum: int) -> Callable[[], None]:
            try:
                signame = Signals(signum).name
            except ValueError:
                signame = 'UNKNOWN'

            def wrapper() -> None:
                global abort_test  # noqa: PLW0603
                if abort_test:
                    return

                logger.info('handling signal %s (%d)', signame, signum)
                abort_test = True
                if isinstance(runner, WorkerRunner):
                    runner._send_stats()
                    runner.client.send(Message('client_aborted', None, runner.client_id))

                runner.environment.events.quitting.fire(environment=runner.environment, reverse=True, abort=True)

            return wrapper

        gevent.signal_handler(SIGTERM, sig_handler(SIGTERM))
        gevent.signal_handler(SIGINT, sig_handler(SIGINT))

        try:
            main_greenlet.join()
            logger.debug('main greenlet finished')
        finally:
            if abort_test:
                code = SIGTERM.value
            elif unhandled_greenlet_exception:
                code = 2
            elif environment.process_exit_code is not None:
                code = environment.process_exit_code
            elif len(runner.errors) > 0 or len(runner.exceptions) > 0:
                code = 3
            else:
                code = 0

        return code
    finally:
        shutdown_external_processes(external_processes, watch_running_external_processes_greenlet)


def _grizzly_sort_stats(stats: lstats.RequestStats) -> List[Tuple[str, str, int]]:
    locust_keys: list[tuple[str, str | None]] = sorted(stats.entries.keys())

    previous_ident: str | None = None
    scenario_keys: list[tuple[str, str | None]] = []
    scenario_sorted_keys: list[tuple[str, str, int]] = []
    for index, key in enumerate(locust_keys):
        try:
            ident, _ = key[0].split(' ', 1)
        except ValueError:
            ident = '999'

        is_last = index == len(locust_keys) - 1
        if (previous_ident is not None and previous_ident != ident) or is_last:
            if is_last:
                scenario_keys.append(key[:2])

            scenario_sorted_keys += sorted([
                (name, method or '', RequestType.get_method_weight(method or 'empty')) for name, method in scenario_keys
            ], key=itemgetter(2, 0))
            scenario_keys.clear()

        previous_ident = ident
        scenario_keys.append(key[:2])

    return scenario_sorted_keys


def grizzly_stats_printer(stats: lstats.RequestStats) -> Callable[[], NoReturn]:
    def _grizzly_stats_printer() -> NoReturn:
        while True:
            grizzly_print_stats(stats)
            gevent.sleep(lstats.CONSOLE_STATS_INTERVAL_SEC)

    return _grizzly_stats_printer


def grizzly_print_stats(stats: lstats.RequestStats, *, current: bool = True, grizzly_style: bool = True) -> None:
    if not grizzly_style:
        lstats.print_stats(stats, current=current)
        return

    name_column_width = (lstats.STATS_NAME_WIDTH - lstats.STATS_TYPE_WIDTH) + 4  # saved characters by compacting other columns
    row = (
        ("%-" + str(lstats.STATS_TYPE_WIDTH) + "s %-" + str(name_column_width) + "s %7s %12s |%7s %7s %7s%7s | %7s %11s")
        % ("Type", "Name", "# reqs", "# fails", "Avg", "Min", "Max", "Med", "req/s", "failures/s")
    )
    stats_logger.info(row)
    separator = f'{"-" * lstats.STATS_TYPE_WIDTH}|{"-" * (name_column_width)}|{"-" * 7}|{"-" * 13}|{"-" * 7}|{"-" * 7}|{"-" * 7}|{"-" * 7}|{"-" * 8}|{"-" * 11}'
    stats_logger.info(separator)

    keys = _grizzly_sort_stats(stats)

    for key in keys:
        r = stats.entries[key[:2]]
        stats_logger.info(r.to_string(current=current))

    stats_logger.info(separator)
    stats_logger.info(stats.total.to_string(current=current))
    stats_logger.info('')


def grizzly_print_percentile_stats(stats: lstats.RequestStats, *, grizzly_style: bool = True) -> None:
    if not grizzly_style:
        lstats.print_percentile_stats(stats)
        return

    stats_logger.info('Response time percentiles (approximated)')
    headers = ('Type', 'Name', *tuple(lstats.get_readable_percentiles(lstats.PERCENTILES_TO_REPORT)), '# reqs')
    row = (
        (
            f'%-{lstats.STATS_TYPE_WIDTH}s %-{lstats.STATS_NAME_WIDTH}s %8s '
            f'{" ".join(["%6s"] * len(lstats.PERCENTILES_TO_REPORT))}'
        )
        % headers
    )
    stats_logger.info(row)
    separator = (
        f'{"-" * lstats.STATS_TYPE_WIDTH}|{"-" * lstats.STATS_NAME_WIDTH}|{"-" * 8}|{("-" * 6 + "|") * len(lstats.PERCENTILES_TO_REPORT)}'
    )[:-1]
    stats_logger.info(separator)

    keys = _grizzly_sort_stats(stats)

    for key in keys:
        r = stats.entries[key[:2]]
        if r.response_times:
            stats_logger.info(r.percentile())
    stats_logger.info(separator)

    if stats.total.response_times:
        stats_logger.info(stats.total.percentile())
    stats_logger.info('')
