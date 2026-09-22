from tt_lib.config import load_config


def test_load_config_uses_environment(monkeypatch):
    monkeypatch.setenv("SERVICE_NAME", "audit-service")
    monkeypatch.setenv("PORT", "8080")
    monkeypatch.setenv("DATABASE_URL", "postgresql://db/audit")

    cfg = load_config()

    assert cfg.service_name == "audit-service"
    assert cfg.port == 8080
    assert cfg.database_url == "postgresql://db/audit"


def test_load_config_defaults(monkeypatch):
    for key in ("SERVICE_NAME", "PORT", "DATABASE_URL", "REDIS_URL"):
        monkeypatch.delenv(key, raising=False)

    cfg = load_config()

    assert cfg.service_name == "unnamed-service"
    assert cfg.port == 8080
    assert cfg.database_url is None
    assert cfg.redis_url is None


def test_load_config_treats_empty_as_unset(monkeypatch):
    """Policy shared by the three libraries: an empty variable == an unset one.

    Python already followed it (`os.getenv(...) or <default>`) and Go did too
    (the `env` helper of tt-lib-go compares against ""), but Node did not: with
    PORT="", `Number(process.env.PORT ?? 8080)` gave 0 — which for listen()
    means "a random ephemeral port", so the service started on a port nobody
    knows. This test pins the policy here so it is not lost.
    """
    for key in ("SERVICE_NAME", "PORT", "DATABASE_URL", "REDIS_URL"):
        monkeypatch.setenv(key, "")

    cfg = load_config()

    assert cfg.service_name == "unnamed-service"
    assert cfg.port == 8080
    assert cfg.database_url is None
    assert cfg.redis_url is None
