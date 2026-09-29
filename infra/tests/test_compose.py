"""Only Caddy is on the stand LAN (spec 09, ARCHITECTURE §5)."""

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def test_only_caddy_binds_the_lan():
    services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    for name, svc in services.items():
        for port in svc.get("ports", []):
            if name == "caddy":
                assert port == "0.0.0.0:80:80"
            else:
                assert str(port).startswith("127.0.0.1:"), f"{name} publishes {port} beyond localhost"


def test_api_and_web_are_behind_caddy():
    services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    assert {"api", "web"} <= set(services["caddy"]["depends_on"])
    caddyfile = (ROOT / "infra" / "caddy" / "Caddyfile").read_text()
    assert "reverse_proxy api:8000" in caddyfile and "flush_interval -1" in caddyfile
    assert "reverse_proxy web:3000" in caddyfile
