import asyncio

import pytest

from tt_lib.events import Consumer, Publisher, make_recorder, split_channel


def test_split_channel_separates_transport():
    assert split_channel("kafka:order-events") == ("kafka", "order-events")
    assert split_channel("rabbitmq:sms.send") == ("rabbitmq", "sms.send")


def test_split_channel_without_prefix_has_no_transport():
    transport, _ = split_channel("order-events")
    assert transport == ""


@pytest.mark.asyncio
async def test_recorder_without_database_logs_instead_of_failing():
    record = make_recorder("audit-service", None)
    await record("kafka:order-events", b'{"a":1}')


@pytest.mark.asyncio
async def test_recorder_with_unsupported_engine_falls_back_to_log():
    record = make_recorder("catalog-sync-service", "mongodb://mongo:27017/catalog")
    await record("kafka:travel-events", b"{}")


# --------------------------------------------------------------------------
# The connection failure is NOT cached.
#
# These tests cover the Critical bug found in tt-lib-go: its recorder memoized
# connection and error together with a sync.Once, so if the first message
# arrived before Postgres accepted connections —the normal case when compose
# starts the database and the service at the same time— the error stayed
# cached forever and no later message tried again. The opposite behaviour is
# pinned here so it is not lost.
# --------------------------------------------------------------------------


class _FakePool:
    """A fake asyncpg pool: it notes down what is executed against it."""

    def __init__(self, fail_on_create_table: bool = False) -> None:
        self.executed: list[tuple] = []
        self.closed = False
        self._fail_on_create_table = fail_on_create_table

    async def execute(self, sql: str, *args: object) -> None:
        if self._fail_on_create_table and sql.startswith("CREATE TABLE"):
            raise ConnectionError("the database is not accepting connections yet")
        self.executed.append((sql, *args))

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_recorder_retries_when_the_first_connection_fails(monkeypatch):
    """A connection failure must not leave the consumer mute forever."""
    pool = _FakePool()
    attempts = []

    async def fake_create_pool(url: str):
        attempts.append(url)
        if len(attempts) == 1:
            raise ConnectionError("connection refused")
        return pool

    monkeypatch.setattr("tt_lib.events.asyncpg.create_pool", fake_create_pool)
    record = make_recorder("audit-service", "postgresql://tt:tt@postgres:5432/audit")

    # First message: Postgres is not up yet.
    with pytest.raises(ConnectionError):
        await record("kafka:order-events", b'{"a":1}')

    # Second message: the database answers now and it must retry.
    await record("kafka:order-events", b'{"a":2}')

    assert len(attempts) == 2, "the second message must retry the connection"
    assert pool.executed[0][0].startswith("CREATE TABLE IF NOT EXISTS received_events")
    assert pool.executed[1] == (
        "INSERT INTO received_events (channel, payload) VALUES ($1, $2)",
        "kafka:order-events",
        '{"a":2}',
    )


@pytest.mark.asyncio
async def test_recorder_retries_when_create_table_fails(monkeypatch):
    """If the CREATE TABLE fails the connection is not taken as good either."""
    pools = [_FakePool(fail_on_create_table=True), _FakePool()]

    failed_pool, good_pool = pools

    async def fake_create_pool(url: str):
        return pools.pop(0)

    monkeypatch.setattr("tt_lib.events.asyncpg.create_pool", fake_create_pool)
    record = make_recorder("fraud-detection-service", "postgres://tt:tt@postgres:5432/fraud")

    with pytest.raises(ConnectionError):
        await record("kafka:order-events", b"{}")

    await record("kafka:order-events", b'{"ok":true}')

    assert pools == [], "the second attempt must open a new pool, not reuse the failed one"
    assert failed_pool.closed, "the pool whose CREATE TABLE failed is closed"
    assert good_pool.executed[-1][1:] == ("kafka:order-events", '{"ok":true}')


@pytest.mark.asyncio
async def test_recorder_memoizes_the_successful_connection(monkeypatch):
    """The success is memoized: a single connection and a single CREATE TABLE."""
    pool = _FakePool()
    attempts = []

    async def fake_create_pool(url: str):
        attempts.append(url)
        return pool

    monkeypatch.setattr("tt_lib.events.asyncpg.create_pool", fake_create_pool)
    record = make_recorder("audit-service", "postgresql://tt:tt@postgres:5432/audit")

    await record("kafka:order-events", b"{}")
    await record("kafka:order-events", b"{}")

    assert len(attempts) == 1
    creates = [sql for sql, *_ in pool.executed if sql.startswith("CREATE TABLE")]
    assert len(creates) == 1


class _FakeProducer:
    """A fake aiokafka producer, with a configurable start."""

    instances: list["_FakeProducer"] = []

    def __init__(self, *, bootstrap_servers: str, client_id: str) -> None:
        self.sent: list[tuple[str, bytes]] = []
        self.stopped = False
        self.fail_start = False
        _FakeProducer.instances.append(self)

    async def start(self) -> None:
        if self.fail_start:
            raise ConnectionError("the broker is not accepting connections yet")

    async def send_and_wait(self, topic: str, body: bytes) -> None:
        self.sent.append((topic, body))

    async def stop(self) -> None:
        self.stopped = True


@pytest.mark.asyncio
async def test_publisher_retries_when_the_producer_fails_to_start(monkeypatch):
    """Same as the recorder: a failed start does not stay cached."""
    _FakeProducer.instances = []
    fail_first = {"pending": True}

    def factory(**kwargs):
        producer = _FakeProducer(**kwargs)
        if fail_first["pending"]:
            producer.fail_start = True
            fail_first["pending"] = False
        return producer

    monkeypatch.setattr("tt_lib.events.AIOKafkaProducer", factory)
    publisher = Publisher("admin-user-service")

    with pytest.raises(ConnectionError):
        await publisher.publish("kafka:audit-events", {"actor": "ana"})

    await publisher.publish("kafka:audit-events", {"actor": "ana"})

    assert len(_FakeProducer.instances) == 2, "the second publish must retry"
    assert _FakeProducer.instances[0].stopped, "the half-started producer is closed"
    assert _FakeProducer.instances[1].sent == [("audit-events", b'{"actor": "ana"}')]


@pytest.mark.asyncio
async def test_publisher_rejects_a_channel_without_transport():
    publisher = Publisher("admin-user-service")

    with pytest.raises(ValueError, match="has no recognized transport"):
        await publisher.publish("audit-events", {})


# --------------------------------------------------------------------------
# Consumer lifecycle: start() does not block, close() waits.
# --------------------------------------------------------------------------


class _FakeKafkaConsumer:
    """A consumer that keeps waiting for the broker and never connects."""

    started = None
    stopped: list[bool] = []

    def __init__(self, *topics: str, **kwargs: object) -> None:
        pass

    async def start(self) -> None:
        _FakeKafkaConsumer.started.set()
        await asyncio.Event().wait()  # the broker does not answer: waits forever

    async def stop(self) -> None:
        _FakeKafkaConsumer.stopped.append(True)

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration


@pytest.mark.asyncio
async def test_consumer_start_returns_without_waiting_for_the_broker(monkeypatch):
    """start() must return right away even if the broker does not answer.

    These consumers live inside FastAPI applications: if start() waited for
    the connection, a broker that is down would delay the startup of several
    services at once.
    """
    _FakeKafkaConsumer.started = asyncio.Event()
    _FakeKafkaConsumer.stopped = []
    monkeypatch.setattr("tt_lib.events.AIOKafkaConsumer", _FakeKafkaConsumer)

    async def handler(channel: str, payload: bytes) -> None:
        pass

    consumer = Consumer("audit-service", ["kafka:order-events"], handler)

    await asyncio.wait_for(consumer.start(), timeout=1)

    # The consume task runs on its own after start() returned.
    await asyncio.wait_for(_FakeKafkaConsumer.started.wait(), timeout=1)

    # close() cancels and waits: when it returns, the loop already closed its connection.
    await asyncio.wait_for(consumer.close(), timeout=1)
    assert _FakeKafkaConsumer.stopped == [True]


@pytest.mark.asyncio
async def test_consumer_rejects_a_channel_without_transport_before_starting(monkeypatch):
    """A badly declared channel fails without leaving half-started tasks."""
    _FakeKafkaConsumer.started = asyncio.Event()
    _FakeKafkaConsumer.stopped = []
    monkeypatch.setattr("tt_lib.events.AIOKafkaConsumer", _FakeKafkaConsumer)

    async def handler(channel: str, payload: bytes) -> None:
        pass

    consumer = Consumer("audit-service", ["kafka:order-events", "order-events"], handler)

    with pytest.raises(ValueError, match="has no recognized transport"):
        await consumer.start()

    await consumer.close()
    assert not _FakeKafkaConsumer.started.is_set(), "it must not start any channel"


# --------------------------------------------------------------------------
# Message dispatch: what the handler receives and what is acked/nacked.
#
# The doubles deliver a list of messages and then keep waiting, without
# finishing the iterator: that way the consume loop neither reconnects nor
# sleeps, and the test does not depend on timers.
# --------------------------------------------------------------------------


class _FakeKafkaMessage:
    def __init__(self, value: bytes) -> None:
        self.value = value


class _DeliveringKafkaConsumer:
    """A Kafka consumer that delivers `payloads` and then stays still."""

    payloads: list[bytes] = []
    drained: asyncio.Event | None = None
    instances = 0

    def __init__(self, *topics: str, **kwargs: object) -> None:
        _DeliveringKafkaConsumer.instances += 1
        self._pending = list(_DeliveringKafkaConsumer.payloads)

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def __aiter__(self):
        return self

    async def __anext__(self) -> _FakeKafkaMessage:
        if self._pending:
            return _FakeKafkaMessage(self._pending.pop(0))
        _DeliveringKafkaConsumer.drained.set()
        # The iterator is not exhausted: the loop keeps waiting for messages,
        # which is what a real consumer does between one delivery and the next.
        await asyncio.Event().wait()


def _install_kafka(monkeypatch, payloads: list[bytes]) -> asyncio.Event:
    _DeliveringKafkaConsumer.payloads = payloads
    _DeliveringKafkaConsumer.drained = asyncio.Event()
    _DeliveringKafkaConsumer.instances = 0
    monkeypatch.setattr("tt_lib.events.AIOKafkaConsumer", _DeliveringKafkaConsumer)
    return _DeliveringKafkaConsumer.drained


@pytest.mark.asyncio
async def test_kafka_message_reaches_the_handler_with_its_channel_prefix(monkeypatch):
    """The handler receives the FULL channel, with prefix, and the body as it is.

    The prefix matters: audit-service listens on three topics with the same
    handler and tells the origin apart by it alone.
    """
    drained = _install_kafka(monkeypatch, [b'{"id":1}'])
    received: list[tuple[str, bytes]] = []

    async def handler(channel: str, payload: bytes) -> None:
        received.append((channel, payload))

    consumer = Consumer("audit-service", ["kafka:order-events"], handler)
    await consumer.start()
    await asyncio.wait_for(drained.wait(), timeout=1)
    await consumer.close()

    assert received == [("kafka:order-events", b'{"id":1}')]


@pytest.mark.asyncio
async def test_kafka_handler_failure_does_not_kill_the_consume_loop(monkeypatch):
    """A handler failure must not bring the loop down, nor reconnect.

    In Kafka there is no explicit ack, so the only thing to guarantee is that
    the next message is still delivered.
    """
    drained = _install_kafka(monkeypatch, [b"poison", b"good"])
    received: list[bytes] = []

    async def handler(channel: str, payload: bytes) -> None:
        received.append(payload)
        if payload == b"poison":
            raise RuntimeError("the handler blows up on this message")

    consumer = Consumer("audit-service", ["kafka:order-events"], handler)
    await consumer.start()
    await asyncio.wait_for(drained.wait(), timeout=1)
    await consumer.close()

    assert received == [b"poison", b"good"], "the loop goes on after the exception"
    assert _DeliveringKafkaConsumer.instances == 1, "it must not reconnect over a handler failure"


class _FakeRabbitMessage:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.acked = False
        self.nacked_requeue: bool | None = None

    async def ack(self) -> None:
        self.acked = True

    async def nack(self, requeue: bool = False) -> None:
        self.nacked_requeue = requeue


class _FakeQueueIterator:
    def __init__(self, messages: list[_FakeRabbitMessage], drained: asyncio.Event) -> None:
        self._pending = list(messages)
        self._drained = drained

    async def __aenter__(self) -> "_FakeQueueIterator":
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    def __aiter__(self):
        return self

    async def __anext__(self) -> _FakeRabbitMessage:
        if self._pending:
            return self._pending.pop(0)
        self._drained.set()
        await asyncio.Event().wait()


class _FakeQueue:
    def __init__(self, messages: list[_FakeRabbitMessage], drained: asyncio.Event) -> None:
        self._messages = messages
        self._drained = drained

    def iterator(self) -> _FakeQueueIterator:
        return _FakeQueueIterator(self._messages, self._drained)


class _FakeRabbitChannel:
    def __init__(self, queue: _FakeQueue) -> None:
        self._queue = queue
        self.declared: tuple[str, bool] | None = None

    async def declare_queue(self, name: str, durable: bool = False) -> _FakeQueue:
        self.declared = (name, durable)
        return self._queue


class _FakeRabbitConnection:
    def __init__(self, channel: _FakeRabbitChannel) -> None:
        self._channel = channel
        self.closed = False

    async def channel(self) -> _FakeRabbitChannel:
        return self._channel

    async def close(self) -> None:
        self.closed = True


def _install_rabbit(monkeypatch, messages: list[_FakeRabbitMessage]):
    drained = asyncio.Event()
    channel = _FakeRabbitChannel(_FakeQueue(messages, drained))
    connection = _FakeRabbitConnection(channel)

    async def fake_connect_robust(url: str) -> _FakeRabbitConnection:
        return connection

    monkeypatch.setattr("tt_lib.events.aio_pika.connect_robust", fake_connect_robust)
    return drained, channel


@pytest.mark.asyncio
async def test_rabbit_message_reaches_the_handler_and_is_acked(monkeypatch):
    """A good delivery: the handler receives channel with prefix and body, and it is acked."""
    message = _FakeRabbitMessage(b'{"to":"+34600"}')
    drained, channel = _install_rabbit(monkeypatch, [message])
    received: list[tuple[str, bytes]] = []

    async def handler(channel_name: str, payload: bytes) -> None:
        received.append((channel_name, payload))

    consumer = Consumer("sms-gateway-service", ["rabbitmq:sms.send"], handler)
    await consumer.start()
    await asyncio.wait_for(drained.wait(), timeout=1)
    await consumer.close()

    assert received == [("rabbitmq:sms.send", b'{"to":"+34600"}')]
    assert channel.declared == ("sms.send", True), "the queue is declared durable"
    assert message.acked is True
    assert message.nacked_requeue is None, "a processed message is not requeued"


@pytest.mark.asyncio
async def test_rabbit_nacks_with_requeue_when_the_handler_fails(monkeypatch):
    """If the handler fails, the message goes back on the queue instead of being lost.

    Without the nack with requeue the event would disappear silently; with a
    wrong ack, it would too. That is why both things are checked at once.
    """
    message = _FakeRabbitMessage(b"{}")
    drained, _ = _install_rabbit(monkeypatch, [message])

    async def handler(channel_name: str, payload: bytes) -> None:
        raise RuntimeError("the handler could not process it")

    consumer = Consumer("sms-gateway-service", ["rabbitmq:sms.send"], handler)
    await consumer.start()
    await asyncio.wait_for(drained.wait(), timeout=1)
    await consumer.close()

    assert message.nacked_requeue is True, "it must be requeued"
    assert message.acked is False, "a message that failed must not be acked"
