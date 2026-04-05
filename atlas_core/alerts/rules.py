"""
Alert rule definitions.

Each rule is a callable that takes the current signal results and returns
zero or more AtlasAlert objects.  Rules are pure functions — no I/O.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from atlas_core.schemas.events import AtlasAlert, EventDomain, SignalComponent
from atlas_core.signals.base import SignalResult

RuleFn = Callable[..., list[AtlasAlert]]


def make_alert_id() -> str:
    return uuid.uuid4().hex[:16]


def storage_surprise_rule(result: SignalResult) -> list[AtlasAlert]:
    """Fire when storage surprise z-score exceeds ±1.5."""
    alerts = []
    z = result.metadata.get("z_score", 0.0)
    if abs(z) < 1.5:
        return alerts

    commodity = result.metadata.get("commodity", "crude")
    region = result.metadata.get("region", "US")
    direction = result.direction
    severity_map = {
        abs(z) >= 3.0: AtlasAlert.Severity.HIGH,
        abs(z) >= 2.0: AtlasAlert.Severity.MEDIUM,
        abs(z) >= 1.5: AtlasAlert.Severity.LOW,
    }
    severity = next((v for k, v in severity_map.items() if k), AtlasAlert.Severity.INFO)

    sign_str = "bearish draw" if direction == "bullish" else "bearish build"
    alerts.append(
        AtlasAlert(
            alert_id=make_alert_id(),
            created_at=datetime.now(tz=timezone.utc),
            domain=EventDomain.ENERGY,
            severity=severity,
            title=f"{commodity.title()} Storage {sign_str.title()} — z={z:.2f}",
            summary=(
                f"{region} {commodity} storage reported a {sign_str} "
                f"of {result.metadata.get('actual_change', 0):.1f} "
                f"vs. seasonal expectation of {result.metadata.get('consensus_change', 0):.1f}. "
                f"z-score: {z:.2f}"
            ),
            score=min(1.0, abs(z) / 3.0),
            affected_regions=[region],
            signal_components=result.components,
        )
    )
    return alerts


def weather_risk_rule(result: SignalResult) -> list[AtlasAlert]:
    """Fire when any tracked asset has weather risk score > 0.5."""
    alerts = []
    max_score = result.metadata.get("max_asset_score", 0.0)
    alert_count = result.metadata.get("active_alerts", 0)

    if max_score < 0.5:
        return alerts

    severity = (
        AtlasAlert.Severity.HIGH if max_score >= 0.8
        else AtlasAlert.Severity.MEDIUM if max_score >= 0.6
        else AtlasAlert.Severity.LOW
    )

    alerts.append(
        AtlasAlert(
            alert_id=make_alert_id(),
            created_at=datetime.now(tz=timezone.utc),
            domain=EventDomain.WEATHER,
            severity=severity,
            title=f"Weather Risk Elevated — {alert_count} active NWS alerts",
            summary=(
                f"Weather-risk score reached {max_score:.2f}/1.0 for tracked energy assets. "
                f"{alert_count} active NWS alerts within 150 km of infrastructure."
            ),
            score=max_score,
            signal_components=result.components,
        )
    )
    return alerts


def fire_exposure_rule(result: SignalResult) -> list[AtlasAlert]:
    """Fire when fire exposure score > 0.3 near any tracked asset."""
    alerts = []
    max_score = abs(result.value)
    if max_score < 0.3:
        return alerts

    most_exposed = result.metadata.get("most_exposed_asset", "unknown")
    det_count = result.metadata.get("total_detections", 0)

    severity = (
        AtlasAlert.Severity.HIGH if max_score >= 0.7
        else AtlasAlert.Severity.MEDIUM if max_score >= 0.5
        else AtlasAlert.Severity.LOW
    )

    alerts.append(
        AtlasAlert(
            alert_id=make_alert_id(),
            created_at=datetime.now(tz=timezone.utc),
            domain=EventDomain.FIRE,
            severity=severity,
            title=f"Active Fire Detections Near {most_exposed}",
            summary=(
                f"{det_count} NASA FIRMS detections near tracked energy infrastructure. "
                f"Highest exposure: {most_exposed} (score {max_score:.2f})."
            ),
            score=max_score,
            signal_components=result.components,
        )
    )
    return alerts


def congestion_rule(result: SignalResult) -> list[AtlasAlert]:
    """Fire when export terminal congestion index > 0.5."""
    alerts = []
    avg_cong = result.metadata.get("avg_congestion_index", 0.0)
    if avg_cong < 0.5:
        return alerts

    terminal = result.metadata.get("most_congested_terminal", "unknown")
    tankers = result.metadata.get("total_tankers", 0)

    alerts.append(
        AtlasAlert(
            alert_id=make_alert_id(),
            created_at=datetime.now(tz=timezone.utc),
            domain=EventDomain.SHIPPING,
            severity=AtlasAlert.Severity.MEDIUM if avg_cong >= 0.7 else AtlasAlert.Severity.LOW,
            title=f"LNG Export Terminal Congestion — {terminal}",
            summary=(
                f"Average congestion index: {avg_cong:.2f}. "
                f"{tankers} tankers near tracked terminals, {result.metadata.get('vessels_at_anchor', 0)} at anchor."
            ),
            score=avg_cong,
            signal_components=result.components,
        )
    )
    return alerts


def news_spike_rule(result: SignalResult) -> list[AtlasAlert]:
    """Fire on sharp negative-tone news acceleration."""
    alerts = []
    neg_z = result.metadata.get("negative_z_today", 0.0)
    if neg_z < 1.5:
        return alerts

    topic = result.metadata.get("topic", "energy")
    count = result.metadata.get("article_count_today", 0)
    tone = result.metadata.get("avg_tone_today", 0.0)

    alerts.append(
        AtlasAlert(
            alert_id=make_alert_id(),
            created_at=datetime.now(tz=timezone.utc),
            domain=EventDomain.GEOPOLITICS,
            severity=AtlasAlert.Severity.MEDIUM if neg_z >= 2.5 else AtlasAlert.Severity.LOW,
            title=f"Negative News-Flow Spike — {topic.replace('_', ' ').title()}",
            summary=(
                f"GDELT negative-tone z-score: {neg_z:.2f}. "
                f"{count} articles today, avg tone {tone:.2f}. "
                f"Topic: {topic}."
            ),
            score=min(1.0, neg_z / 4.0),
            signal_components=result.components,
        )
    )
    return alerts


# Registry of all default rules
DEFAULT_RULES: list[tuple[str, RuleFn]] = [
    ("storage_surprise", storage_surprise_rule),
    ("weather_risk", weather_risk_rule),
    ("fire_exposure", fire_exposure_rule),
    ("congestion", congestion_rule),
    ("news_spike", news_spike_rule),
]
