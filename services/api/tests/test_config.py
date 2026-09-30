import pytest
from conftest import BASE_ENV, make_config

from api.envconfig import Config


def test_defaults_match_env_example():
    cfg = make_config()
    assert (cfg.max_question_chars, cfg.rate_limit_per_min, cfg.max_concurrency, cfg.max_queue) == (300, 10, 2, 6)
    assert (cfg.max_tool_iterations, cfg.max_repairs, cfg.total_timeout_s, cfg.llm_timeout_s) == (6, 2, 45.0, 30.0)
    assert cfg.agent_mode == "tools" and cfg.llm_order == ("local", "internal") and cfg.llm_health_interval_s == 15.0


def test_provider_with_empty_base_url_is_disabled():
    cfg = make_config()
    assert [p.name for p in cfg.providers] == ["local"]
    assert cfg.providers[0].model == "fake-local"
    both = make_config(LLM_INTERNAL_BASE_URL="https://gw/v1", LLM_INTERNAL_API_KEY="k", LLM_INTERNAL_MODEL="m")
    assert [p.name for p in both.providers] == ["local", "internal"]


def test_rejects_unknown_mode():
    with pytest.raises(ValueError, match="AGENT_MODE"):
        make_config(AGENT_MODE="magic")


def test_passwords_are_required():
    env = {k: v for k, v in BASE_ENV.items() if k != "CLICKHOUSE_AGENT_PASSWORD"}
    with pytest.raises(ValueError, match="CLICKHOUSE_AGENT_PASSWORD"):
        Config.from_env(env)


def test_ride_and_trace_settings_have_spec_defaults():
    cfg = make_config()
    assert cfg.trace_user == "trace_ro" and cfg.trace_password == "t"
    assert cfg.rides_rate_limit_per_min == 3 and cfg.rides_global_per_min == 30
    assert cfg.trace_history_ttl_s == 3600 and cfg.trace_queryable_poll_ms == 250


def test_trace_password_is_required():
    env = {k: v for k, v in BASE_ENV.items() if k != "CLICKHOUSE_TRACE_PASSWORD"}
    with pytest.raises(ValueError, match="CLICKHOUSE_TRACE_PASSWORD"):
        Config.from_env(env)
