"""Publishing and consuming events, whatever the transport.

A channel is identified by its prefix — ``kafka:order-events`` or
``rabbitmq:sms.send`` — and it is this library that decides where it goes;
neither the emitter nor the service code knows whether something travels over
Kafka or over RabbitMQ. That is why there is a single API for both transports.

Same shape as the sibling libraries ``tt-lib-go/events`` and
``tt-lib-node/src/events.ts``, adapted to asyncio: a publisher with
``publish``/``close``, a consumer with ``start``/``close`` and a function that
builds the handler that records what is received. Whoever reads one should be
able to understand the others.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, TypeVar

import aio_pika
import asyncpg
from aio_pika.abc import AbstractChannel, AbstractRobustConnection
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

logger = logging.getLogger(__name__)

# Wait between two connection attempts of a consume loop when the broker is
# not ready yet or the connection drops. There is no growing backoff (it is
# noted as a pending improvement, the same as in tt-lib-go): a fixed value is
# enough not to block startup, which is what matters here.
_RETRY_DELAY_SECONDS = 2.0

# Processes a received message. The channel arrives with its prefix, so the
# handler knows where it came from without the consumer having to say it apart.
Handler = Callable[[str, bytes], Awaitable[None]]

T = TypeVar("T")

# Sentinel to tell "not resolved yet" apart from a result that is legitimately
# None. Comparing against None would not do.
_UNSET: Any = object()


def split_channel(channel: str) -> tuple[str, str]:
    """Split the transport from the channel name.

    Returns an empty transport if the channel carries no prefix, which is a
    declaration error in system.yaml and not something to be guessed here.
    """
    transport, separator, name = channel.partition(":")
    if not separator:
        return "", channel
    return transport, name


def _kafka_brokers() -> str:
    return os.getenv("KAFKA_BROKERS") or "kafka:9092"


def _rabbit_url() -> str:
    return os.getenv("RABBITMQ_URL") or "amqp://tt:tt@rabbitmq:5672"


async def _close_quietly(closing: Awaitable[Any]) -> None:
    """Await a close, ignoring its error.

    At close time there is nothing useful left to do with the failure, and
    letting it escape would hide the real error that caused the close.
    """
    try:
        await closing
    except Exception:
        logger.debug("error ignored while closing a connection", exc_info=True)


def _memoize_async(op: Callable[[], Awaitable[T]]) -> Callable[[], Awaitable[T]]:
    """Memoize an async operation, storing ONLY the success.

    The failure is NOT cached, and that is deliberate. Caching connection and
    error together (what ``tt-lib-go`` did with a ``sync.Once``) breaks the
    normal docker compose case, which starts the services without waiting for
    Kafka, RabbitMQ or Postgres to accept connections: if the first message
    arrives before the database, the error stays cached forever and no later
    message tries again, even if the database recovers seconds afterwards. It
    is the same pattern that already caused an incident with Redis in an
    earlier phase.

    Here ``result`` is only assigned if ``op()`` finished well; if it raises,
    the state is left untouched at ``_UNSET`` and the next call retries from
    scratch. The lock stops two cold coroutines from firing two connections
    at once, without turning the failure of one into the failure of all.
    """
    lock = asyncio.Lock()
    result: Any = _UNSET

    async def ensure() -> T:
        nonlocal result
        if result is not _UNSET:
            return result
        async with lock:
            # Another coroutine may have resolved it while we waited on the lock.
            if result is not _UNSET:
                return result
            value = await op()
            # We only get here if `op()` did not raise: the failure is never stored.
            result = value
            return value

    return ensure


class Publisher:
    """Publishes messages on the channels the service declares."""

    def __init__(self, service_name: str) -> None:
        """Create the service publisher.

        It does not connect yet: the connection to each transport is opened
        on the first publish that uses it, so the service startup is not
        blocked if the broker is not ready yet.
        """
        self._service_name = service_name
        self._producer: AIOKafkaProducer | None = None
        self._rabbit_connection: AbstractRobustConnection | None = None
        self._ensure_producer = _memoize_async(self._open_producer)
        self._ensure_rabbit_channel = _memoize_async(self._open_rabbit_channel)

    async def _open_producer(self) -> AIOKafkaProducer:
        producer = AIOKafkaProducer(
            bootstrap_servers=_kafka_brokers(),
            client_id=self._service_name,
        )
        try:
            await producer.start()
        except Exception:
            # The half-started producer is closed so no sockets are left
            # hanging, and it is re-raised without storing anything:
            # `_memoize_async` leaves the state untouched, so the next
            # publish retries.
            await _close_quietly(producer.stop())
            raise
        self._producer = producer
        return producer

    async def _open_rabbit_channel(self) -> AbstractChannel:
        connection = await aio_pika.connect_robust(_rabbit_url())
        try:
            channel = await connection.channel()
        except Exception:
            await _close_quietly(connection.close())
            raise
        self._rabbit_connection = connection
        return channel

    async def publish(self, channel: str, payload: Any) -> None:
        """Send the payload to the given channel, serialized as JSON."""
        transport, name = split_channel(channel)
        try:
            body = json.dumps(payload).encode()
        except (TypeError, ValueError) as err:
            raise ValueError(f"serializing the message for {channel}: {err}") from err

        if transport == "kafka":
            producer = await self._ensure_producer()
            await producer.send_and_wait(name, body)
        elif transport == "rabbitmq":
            rabbit_channel = await self._ensure_rabbit_channel()
            await rabbit_channel.declare_queue(name, durable=True)
            await rabbit_channel.default_exchange.publish(
                aio_pika.Message(body=body, content_type="application/json"),
                routing_key=name,
            )
        else:
            raise ValueError(f'channel "{channel}" has no recognized transport')

    async def close(self) -> None:
        """Close the open connections."""
        if self._producer is not None:
            await _close_quietly(self._producer.stop())
            self._producer = None
        if self._rabbit_connection is not None:
            await _close_quietly(self._rabbit_connection.close())
            self._rabbit_connection = None


class Consumer:
    """Listens on the channels the service declares."""

    def __init__(self, service_name: str, channels: Sequence[str], handler: Handler) -> None:
        """Create the service consumer. It listens to nothing until `start()`."""
        self._service_name = service_name
        self._channels = list(channels)
        self._handler = handler
        self._tasks: list[asyncio.Task[None]] = []

    async def start(self) -> None:
        """Launch one loop per channel as a task and return IMMEDIATELY.

        These consumers live inside long-lived FastAPI applications. If
        `start()` waited for each broker to accept the connection, a broker
        that is down would delay the startup of the whole service — and with
        several consumers, that of the whole stack. Each channel retries on
        its own while the service is already serving requests.

        The channels are validated BEFORE any task is launched: if one does
        not carry a recognized transport, `start()` raises without having
        started anything, so the caller does not have to call `close()` to
        clean up a half-done startup.
        """
        for channel in self._channels:
            transport, _ = split_channel(channel)
            if transport not in ("kafka", "rabbitmq"):
                raise ValueError(f'channel "{channel}" has no recognized transport')

        for channel in self._channels:
            transport, name = split_channel(channel)
            loop = self._run_kafka if transport == "kafka" else self._run_rabbit
            self._tasks.append(
                asyncio.create_task(loop(channel, name), name=f"{self._service_name}:{channel}")
            )

    async def _dispatch(self, channel: str, payload: bytes) -> bool:
        """Hand a message to the handler. Its failure does not stop the loop."""
        try:
            await self._handler(channel, payload)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("%s: processing %s", self._service_name, channel)
            return False
        return True

    async def _run_kafka(self, channel: str, topic: str) -> None:
        while True:
            consumer = AIOKafkaConsumer(
                topic,
                bootstrap_servers=_kafka_brokers(),
                client_id=self._service_name,
                group_id=self._service_name,
            )
            try:
                await consumer.start()
                async for message in consumer:
                    await self._dispatch(channel, message.value or b"")
            except asyncio.CancelledError:
                raise  # orderly stop from close()
            except Exception:
                logger.exception("%s: reading from %s", self._service_name, channel)
            finally:
                await _close_quietly(consumer.stop())
            await asyncio.sleep(_RETRY_DELAY_SECONDS)

    async def _run_rabbit(self, channel: str, queue_name: str) -> None:
        while True:
            connection: AbstractRobustConnection | None = None
            try:
                connection = await aio_pika.connect_robust(_rabbit_url())
                rabbit_channel = await connection.channel()
                queue = await rabbit_channel.declare_queue(queue_name, durable=True)
                async with queue.iterator() as messages:
                    async for message in messages:
                        # Explicit ack after processing: if the handler
                        # fails, the message goes back on the queue instead
                        # of being lost.
                        if await self._dispatch(channel, message.body):
                            await message.ack()
                        else:
                            await message.nack(requeue=True)
            except asyncio.CancelledError:
                raise  # orderly stop from close()
            except Exception:
                logger.exception("%s: listening on %s", self._service_name, channel)
            finally:
                if connection is not None:
                    await _close_quietly(connection.close())
            await asyncio.sleep(_RETRY_DELAY_SECONDS)

    async def close(self) -> None:
        """Cancel the loops and wait for them to finish in an orderly way.

        Every task is awaited: each loop closes its connection in its own
        `finally`, so when `close()` returns there is no open connection and
        no live task left.
        """
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()


_CREATE_TABLE_POSTGRES = """CREATE TABLE IF NOT EXISTS received_events (
    id SERIAL PRIMARY KEY,
    channel VARCHAR(255) NOT NULL,
    payload TEXT NOT NULL,
    received_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
)"""

_INSERT_POSTGRES = "INSERT INTO received_events (channel, payload) VALUES ($1, $2)"


def _engine_of(database_url: str | None) -> str:
    """Work out the supported SQL engine from the connection string.

    Returns an empty string if there is no database, or if the engine is not a
    SQL one this library supports.

    Only PostgreSQL is covered, and that is deliberate, not an oversight: the
    only Python consumers in the system are audit-service and
    fraud-detection-service (Postgres), schedule-optimizer-service and
    analytics-query-service (no database). No Python consumer uses MySQL — the
    only Python service with MySQL is consign-service, which only publishes and
    never records what it receives. Adding an `aiomysql` path here would be
    dead code: if one day a Python consumer takes up MySQL, it is added then
    (the statements are not portable — MySQL uses `?` and `AUTO_INCREMENT`
    where Postgres uses `$1` and `SERIAL`, see tt-lib-go and tt-lib-node, which
    do need it).
    """
    if not database_url:
        return ""
    if database_url.startswith(("postgresql://", "postgres://")):
        return "postgres"
    return ""


def _log_recorder(service_name: str) -> Handler:
    async def record(channel: str, payload: bytes) -> None:
        logger.info(
            "%s: received from %s: %s",
            service_name,
            channel,
            payload.decode("utf-8", errors="replace"),
        )

    return record


def make_recorder(service_name: str, database_url: str | None) -> Handler:
    """Return the handler that records what is received.

    If the service has a PostgreSQL database, it writes a row in
    `received_events`; if it has no database, or the engine is not a supported
    SQL one (today MongoDB), it writes to the log instead of failing. See
    `_engine_of` for why MySQL is not here.

    The connection and the `CREATE TABLE` happen on the first message, not when
    the handler is built, and their failure is NOT cached: if Postgres is not
    accepting connections yet, the next message retries from scratch (see
    `_memoize_async`).
    """
    if _engine_of(database_url) != "postgres":
        return _log_recorder(service_name)

    async def open_pool() -> asyncpg.Pool:
        pool = await asyncpg.create_pool(database_url)
        try:
            await pool.execute(_CREATE_TABLE_POSTGRES)
        except Exception:
            # Neither the connection nor the table counts as good if the
            # CREATE fails: the pool is closed and the error re-raised,
            # leaving the memo untouched so the next message tries again.
            await _close_quietly(pool.close())
            raise
        return pool

    ensure_pool = _memoize_async(open_pool)

    async def record(channel: str, payload: bytes) -> None:
        pool = await ensure_pool()
        await pool.execute(
            _INSERT_POSTGRES,
            channel,
            payload.decode("utf-8", errors="replace"),
        )

    return record
