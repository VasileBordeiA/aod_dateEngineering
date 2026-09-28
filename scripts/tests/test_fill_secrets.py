import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fill_secrets import fill

TEMPLATE = """ADMIN_TOKEN=
CLICKHOUSE_ADMIN_PASSWORD=
CLICKHOUSE_AGENT_PASSWORD=keepme
GRAFANA_ADMIN_PASSWORD=
LLM_INTERNAL_API_KEY=
STAND_WIFI_PASSWORD=
REPLAY_SPEED=60
"""


def test_fills_only_empty_generated_secrets():
    text, filled = fill(TEMPLATE)
    values = dict(line.split("=", 1) for line in text.strip().splitlines())
    assert filled == ["ADMIN_TOKEN", "CLICKHOUSE_ADMIN_PASSWORD", "GRAFANA_ADMIN_PASSWORD"]
    assert len(values["ADMIN_TOKEN"]) == 32 and len(values["CLICKHOUSE_ADMIN_PASSWORD"]) == 32
    assert values["CLICKHOUSE_AGENT_PASSWORD"] == "keepme"
    assert values["LLM_INTERNAL_API_KEY"] == "" and values["STAND_WIFI_PASSWORD"] == ""
    assert values["REPLAY_SPEED"] == "60"


def test_is_idempotent():
    once, _ = fill(TEMPLATE)
    twice, filled = fill(once)
    assert twice == once and filled == []
