import pytest
from fake_llm import FakeLLM

from agent.providers import ProviderPool
from api.envconfig import ProviderConfig

LOCAL = ProviderConfig("local", "http://fake-local/v1", "ollama", "qwen")
INTERNAL = ProviderConfig("internal", "http://fake-gw/v1", "k", "gpt")


def pool(order=("local", "internal"), fakes=None) -> tuple[ProviderPool, dict[str, FakeLLM]]:
    fakes = fakes or {"local": FakeLLM(), "internal": FakeLLM()}
    return ProviderPool([LOCAL, INTERNAL], order, lambda pc: fakes[pc.name].client()), fakes


def test_candidates_follow_order_and_health():
    p, _ = pool()
    assert [llm.name for llm in p.candidates()] == ["local", "internal"]
    p.record("local", ok=False)
    assert [llm.name for llm in p.candidates()] == ["local", "internal"]  # one failure isn't enough
    p.record("local", ok=False)
    assert [llm.name for llm in p.candidates()] == ["internal"]
    p.record("local", ok=True)
    assert [llm.name for llm in p.candidates()] == ["local", "internal"]  # one success is


def test_llm_carries_the_json_mode_setting():
    fakes = {"local": FakeLLM(), "internal": FakeLLM()}
    gateway = ProviderConfig("internal", "http://fake-gw/v1", "k", "gpt", json_mode=False)
    p = ProviderPool([LOCAL, gateway], ("local", "internal"), lambda pc: fakes[pc.name].client())
    assert [(llm.name, llm.json_mode) for llm in p.candidates()] == [("local", True), ("internal", False)]


def test_set_order_live_and_validation():
    p, _ = pool()
    p.set_order(["internal", "local"])
    assert [llm.name for llm in p.candidates()] == ["internal", "local"]
    p.set_order([])
    assert p.candidates() == [] and p.status()["active"] == "cache"
    with pytest.raises(ValueError, match="unknown"):
        p.set_order(["cloud"])
    with pytest.raises(ValueError, match="duplicate"):
        p.set_order(["local", "local"])


def test_order_ignores_disabled_providers():
    fakes = {"local": FakeLLM()}
    p = ProviderPool([LOCAL], ("local", "internal"), lambda pc: fakes[pc.name].client())
    assert p.order == ["local"]


def test_status_shape_and_p50():
    p, _ = pool()
    p.record("local", ok=True, latency_ms=100)
    p.record("local", ok=True, latency_ms=300)
    p.record("local", ok=True, latency_ms=200)
    s = p.status()
    assert s["active"] == "local" and s["order"] == ["local", "internal"]
    local = s["providers"][0]
    assert local["name"] == "local" and local["model"] == "qwen" and local["healthy"] is True
    assert local["p50_ms"] == 200 and local["last_ok_at"] is not None
    assert s["providers"][1]["p50_ms"] is None


@pytest.mark.anyio
async def test_health_check_marks_down_after_two_failures():
    p, fakes = pool()
    fakes["local"].down = True
    await p.check()
    assert p.status()["providers"][0]["healthy"] is True
    await p.check()
    assert p.status()["providers"][0]["healthy"] is False and p.status()["active"] == "internal"
    fakes["local"].down = False
    await p.check()
    assert p.status()["active"] == "local"
