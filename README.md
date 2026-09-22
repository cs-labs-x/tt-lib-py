# tt-lib-py

Shared library for the Python services of the train ticket sales system. It
gives every service the same common base, so the infrastructure logic is not
rewritten 20 times:

| Module (`tt_lib/`) | What it solves                                                                            |
| -------------------- | ---------------------------------------------------------------------------------------- |
| `config`             | Reads the service configuration from the environment.                                   |
| `client`             | A uniform HTTP client for service-to-service calls.                                      |
| `events`             | Publish and consume messages with a single API, whether the transport is Kafka or RabbitMQ — the channel decides by its prefix (`kafka:...` / `rabbitmq:...`), not the service. |
| `health`             | A FastAPI router with `GET /health`, shared by every Python service.                    |

Same shape as its siblings [`tt-lib-go`](https://github.com/lucas-test-repos/tt-lib-go)
and [`tt-lib-node`](https://github.com/lucas-test-repos/tt-lib-node) — a
publisher with `publish`/`close`, a consumer with `start`/`close`, and a
builder for the health handler — so that a developer who knows one recognizes
it in the other two.

## Why it is public

The 69 Go, Node and Python services of the system —the 70 of the generated
tree minus the frontend, which uses no library— depend on these three
libraries by their version tag (`v0.1.0`): 24 in Go, 25 in Node and 20 in
Python. Publishing them as **public** repositories is what lets the CI of each
of those 69 services resolve the dependency with no credential at all. It is
the only deliberate visibility asymmetry in the whole set of repositories.

## Usage

```python
from tt_lib import ServiceClient, ServiceConfig, load_config, health_router
```

## Development

```bash
pip install -e ".[dev]"
pytest
```
