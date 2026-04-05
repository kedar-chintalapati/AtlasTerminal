"""
Alert engine.

Orchestrates signal computation → rule evaluation → alert deduplication
→ optional LLM explanation → store write.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from loguru import logger

from atlas_core.alerts.rules import DEFAULT_RULES, RuleFn
from atlas_core.schemas.events import AtlasAlert
from atlas_core.signals.base import SignalResult
from atlas_core.signals.composite import CompositeRiskSignal


class AlertEngine:
    """
    Central alert orchestrator.

    Usage
    -----
    engine = AlertEngine(store=my_store)
    alerts = await engine.run_cycle(signal_inputs)
    """

    def __init__(
        self,
        store: Optional[Any] = None,
        rules: Optional[list[tuple[str, RuleFn]]] = None,
        dedup_window_hours: int = 4,
        min_score: float = 0.0,
        llm_client: Optional[Any] = None,   # optional Gemini client
    ) -> None:
        self._store = store
        self._rules = rules or DEFAULT_RULES
        self._dedup_window = timedelta(hours=dedup_window_hours)
        self._min_score = min_score
        self._llm = llm_client
        self._seen_titles: dict[str, datetime] = {}   # simple in-process dedup

    def add_rule(self, name: str, fn: RuleFn) -> None:
        self._rules.append((name, fn))

    def remove_rule(self, name: str) -> None:
        self._rules = [(n, f) for n, f in self._rules if n != name]

    # ------------------------------------------------------------------ #
    # Core cycle                                                           #
    # ------------------------------------------------------------------ #

    async def run_cycle(
        self,
        signal_results: dict[str, SignalResult],
    ) -> list[AtlasAlert]:
        """
        Evaluate all rules against the current signal values.

        Parameters
        ----------
        signal_results : mapping of signal_name → SignalResult
        """
        raw_alerts: list[AtlasAlert] = []

        for rule_name, rule_fn in self._rules:
            # Find the matching signal result
            result = signal_results.get(rule_name) or signal_results.get(
                rule_name.replace("_rule", "")
            )
            if result is None:
                continue
            try:
                new = rule_fn(result)
                raw_alerts.extend(new)
            except Exception as exc:
                logger.warning(f"[AlertEngine] rule '{rule_name}' failed: {exc}")

        # Filter and deduplicate
        alerts = self._dedup_and_filter(raw_alerts)

        # Optional: LLM explanation
        if self._llm is not None:
            alerts = await self._add_llm_explanations(alerts)

        # Persist to store
        if self._store is not None and alerts:
            self._persist(alerts)

        logger.info(f"[AlertEngine] cycle complete: {len(alerts)} alerts")
        return alerts

    def _dedup_and_filter(self, alerts: list[AtlasAlert]) -> list[AtlasAlert]:
        now = datetime.now(tz=timezone.utc)
        result = []
        for alert in alerts:
            if alert.score < self._min_score:
                continue
            key = f"{alert.domain.value}:{alert.title[:60]}"
            last_seen = self._seen_titles.get(key)
            if last_seen and (now - last_seen) < self._dedup_window:
                logger.debug(f"[AlertEngine] dedup '{key}'")
                continue
            self._seen_titles[key] = now
            result.append(alert)
        return result

    async def _add_llm_explanations(self, alerts: list[AtlasAlert]) -> list[AtlasAlert]:
        """Attempt to add Gemini-generated explanation to each alert."""
        for alert in alerts:
            try:
                explanation = await self._llm.explain_alert(alert)
                alert.llm_explanation = explanation
            except Exception as exc:
                logger.debug(f"[AlertEngine] LLM explanation failed: {exc}")
        return alerts

    def _persist(self, alerts: list[AtlasAlert]) -> None:
        import pandas as pd
        rows = [
            {
                "alert_id": a.alert_id,
                "created_at": a.created_at,
                "domain": a.domain.value,
                "severity": a.severity.value,
                "title": a.title,
                "summary": a.summary,
                "score": a.score,
                "lat": a.lat,
                "lon": a.lon,
                "region": a.region,
            }
            for a in alerts
        ]
        df = pd.DataFrame(rows)
        try:
            self._store.upsert_dataframe("atlas_alerts", df)
        except Exception as exc:
            logger.warning(f"[AlertEngine] persist failed: {exc}")


class LLMClient:
    """
    Thin wrapper around Gemini for alert explanation.
    Only instantiated if the user provides a Gemini API key.
    """

    def __init__(self, api_key: str, model: str = "gemini-1.5-flash") -> None:
        self._api_key = api_key
        self._model = model
        self._client: Optional[Any] = None
        self._init_client()

    def _init_client(self) -> None:
        try:
            import google.generativeai as genai   # type: ignore
            genai.configure(api_key=self._api_key)
            self._client = genai.GenerativeModel(self._model)
        except ImportError:
            logger.warning("google-generativeai not installed; LLM layer disabled")
            self._client = None

    async def explain_alert(self, alert: AtlasAlert) -> str:
        if self._client is None:
            return ""
        prompt = (
            f"In 2-3 sentences, explain the commodity-market implication of this alert "
            f"to a professional energy trader:\n\n"
            f"Title: {alert.title}\n"
            f"Summary: {alert.summary}\n"
            f"Domain: {alert.domain.value}\n"
            f"Score: {alert.score:.2f}/1.0\n"
            f"Keep it factual and concise. No disclaimers."
        )
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self._client.generate_content(prompt),
            )
            return str(response.text)[:600]
        except Exception as exc:
            logger.debug(f"[LLMClient] generation failed: {exc}")
            return ""
