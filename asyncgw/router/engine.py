"""Policy-based routing engine with capability matching, content rules, and failover management."""

import asyncio
import logging
import re
import time
from typing import Any, Callable, Dict, List, Optional

from asyncgw.backends.base import BackendExecutionResult, BaseLLMBackend
from asyncgw.backends.health import HealthMonitor
from asyncgw.config import BackendConfig, FailoverConfig, PoliciesConfig, RoutingStrategy
from asyncgw.models.request import AsyncRequestEnvelope, RequestType

logger = logging.getLogger(__name__)


class RoutingDecision:
    """Outcome of policy evaluation for a given request envelope."""

    def __init__(
        self,
        primary_backend: BackendConfig,
        backup_backends: List[BackendConfig],
        strategy_id: str,
        failover_config: FailoverConfig,
        requires_batch_breakdown: bool = False,
        reason: str = "Default strategy selection",
    ):
        self.primary_backend = primary_backend
        self.backup_backends = backup_backends
        self.strategy_id = strategy_id
        self.failover_config = failover_config
        self.requires_batch_breakdown = requires_batch_breakdown
        self.reason = reason

    @property
    def all_candidate_backends(self) -> List[BackendConfig]:
        return [self.primary_backend] + self.backup_backends


class RoutingEngine:
    """Routes incoming inference requests to appropriate backends using configured policies."""

    def __init__(
        self,
        backends: List[BackendConfig],
        policies: PoliciesConfig,
        health_monitor: HealthMonitor,
        backend_clients: Dict[str, BaseLLMBackend],
    ):
        self.backends = backends
        self.backends_map: Dict[str, BackendConfig] = {b.id: b for b in backends}
        self.policies = policies
        self.strategies_map: Dict[str, RoutingStrategy] = {
            s.id: s for s in policies.routing_strategies
        }
        self.health_monitor = health_monitor
        self.backend_clients = backend_clients

    def update_config(self, backends: List[BackendConfig], policies: PoliciesConfig) -> None:
        self.backends = backends
        self.backends_map = {b.id: b for b in backends}
        self.policies = policies
        self.strategies_map = {s.id: s for s in policies.routing_strategies}
        self.health_monitor.update_backends(backends)

    def _estimate_tokens(self, envelope: AsyncRequestEnvelope) -> int:
        """Rough token estimation for routing purposes."""
        messages = envelope.payload.get("messages", [])
        total_chars = 0
        for m in messages:
            content = m.get("content", "")
            if isinstance(content, str):
                total_chars += len(content)
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and "text" in part:
                        total_chars += len(part["text"])
        if total_chars == 0 and "prompt" in envelope.payload:
            prompt = envelope.payload["prompt"]
            total_chars = len(str(prompt))
        return total_chars // 4

    def select_strategy(self, envelope: AsyncRequestEnvelope) -> tuple[str, str]:
        """Determine which routing strategy or direct backend to use."""
        # 1. Direct routing override from request envelope
        if envelope.target_backend and envelope.target_backend in self.backends_map:
            return "direct_override", f"Direct backend target specified: {envelope.target_backend}"

        if envelope.routing_strategy and envelope.routing_strategy in self.strategies_map:
            return envelope.routing_strategy, f"Explicit strategy specified: {envelope.routing_strategy}"

        est_tokens = self._estimate_tokens(envelope)

        # 2. Evaluate content rules
        for rule in self.policies.content_rules:
            # Model mapping rules
            if rule.model_mappings and envelope.model:
                for pattern, target in rule.model_mappings.items():
                    if re.fullmatch(pattern, envelope.model) or pattern in envelope.model:
                        if target in self.backends_map:
                            return "model_rule", f"Matched model rule pattern '{pattern}' -> backend '{target}'"
                        elif target in self.strategies_map:
                            return target, f"Matched model rule pattern '{pattern}' -> strategy '{target}'"

            # Condition rules
            if rule.condition:
                cond = rule.condition
                if cond.min_estimated_tokens and est_tokens >= cond.min_estimated_tokens:
                    if rule.target_backend:
                        return "token_rule", f"Estimated tokens ({est_tokens}) >= {cond.min_estimated_tokens} -> target backend '{rule.target_backend}'"
                    if rule.target_policy:
                        return rule.target_policy, f"Estimated tokens ({est_tokens}) >= {cond.min_estimated_tokens} -> policy '{rule.target_policy}'"

                if cond.max_wait_seconds_under and envelope.max_wait_seconds:
                    if envelope.max_wait_seconds < cond.max_wait_seconds_under:
                        if rule.target_policy:
                            return rule.target_policy, f"Max wait time ({envelope.max_wait_seconds}s) < {cond.max_wait_seconds_under}s -> policy '{rule.target_policy}'"

        # 3. Default policy
        return self.policies.default_policy, "Default system routing policy"

    def route_request(self, envelope: AsyncRequestEnvelope) -> RoutingDecision:
        """Calculate routing decision and candidate backend order."""
        strat_key, reason = self.select_strategy(envelope)

        # Check if direct backend was targeted
        if strat_key in ["direct_override", "token_rule", "model_rule"]:
            target_id = envelope.target_backend
            if strat_key == "token_rule":
                # Find matching target backend from rule
                for rule in self.policies.content_rules:
                    if rule.target_backend:
                        target_id = rule.target_backend
                        break
            elif strat_key == "model_rule":
                for rule in self.policies.content_rules:
                    if rule.model_mappings:
                        for pattern, target in rule.model_mappings.items():
                            if (re.fullmatch(pattern, envelope.model) or pattern in envelope.model) and target in self.backends_map:
                                target_id = target
                                break

            primary = self.backends_map.get(target_id) or self.backends[0]
            # Backups from default strategy
            default_strat = self.strategies_map.get(self.policies.default_policy)
            failover_cfg = default_strat.failover if default_strat else FailoverConfig()
            backups = [
                self.backends_map[b_id]
                for b_id in (default_strat.preference_order if default_strat else [])
                if b_id in self.backends_map and b_id != primary.id
            ]

            requires_breakdown = False
            if envelope.request_type == RequestType.BATCH:
                requires_breakdown = not primary.capabilities.supports_batch

            return RoutingDecision(
                primary_backend=primary,
                backup_backends=backups,
                strategy_id=strat_key,
                failover_config=failover_cfg,
                requires_batch_breakdown=requires_breakdown,
                reason=reason,
            )

        strategy = self.strategies_map.get(self.policies.default_policy)
        if strat_key in self.strategies_map:
            strategy = self.strategies_map[strat_key]

        if not strategy or not strategy.preference_order:
            # Fallback to first available backend
            primary = self.backends[0]
            return RoutingDecision(
                primary_backend=primary,
                backup_backends=[],
                strategy_id="fallback",
                failover_config=FailoverConfig(),
                requires_batch_breakdown=(envelope.request_type == RequestType.BATCH and not primary.capabilities.supports_batch),
                reason="No strategy configured; using fallback backend",
            )

        # Sort candidate backends by preference order, checking health status
        ordered_candidates = []
        unhealthy_candidates = []

        for b_id in strategy.preference_order:
            b_cfg = self.backends_map.get(b_id)
            if not b_cfg or not b_cfg.is_active:
                continue

            if self.health_monitor.is_backend_available(b_id):
                ordered_candidates.append(b_cfg)
            else:
                unhealthy_candidates.append(b_cfg)

        # Append unhealthy at the very tail as last-resort failover
        candidates = ordered_candidates + unhealthy_candidates
        if not candidates:
            candidates = list(self.backends_map.values())

        primary = candidates[0]
        backups = candidates[1:]

        requires_breakdown = False
        if envelope.request_type == RequestType.BATCH:
            requires_breakdown = not primary.capabilities.supports_batch

        return RoutingDecision(
            primary_backend=primary,
            backup_backends=backups,
            strategy_id=strategy.id,
            failover_config=strategy.failover,
            requires_batch_breakdown=requires_breakdown,
            reason=reason,
        )

    async def execute_with_failover(
        self,
        envelope: AsyncRequestEnvelope,
        execute_fn: Callable[[BaseLLMBackend, BackendConfig], Any],
    ) -> tuple[BackendExecutionResult, BackendConfig]:
        """Execute request across candidate backends with automatic failover and exponential backoff."""
        decision = self.route_request(envelope)
        candidates = decision.all_candidate_backends
        failover_cfg = decision.failover_config

        strat_obj = self.strategies_map.get(decision.strategy_id)
        strategy_name = strat_obj.name if strat_obj else decision.strategy_id

        policy_info = {
            "strategy_id": decision.strategy_id,
            "strategy_name": strategy_name,
            "selection_reason": decision.reason,
            "preference_order": [b.id for b in candidates],
        }

        backends_tried: List[Dict[str, Any]] = []

        def _attach_trace(target_result: BackendExecutionResult) -> None:
            target_result.routing_metadata = {
                "routing_policy": policy_info,
                "strategy_id": decision.strategy_id,
                "selection_reason": decision.reason,
                "backends_tried": backends_tried,
                "failover_trace": backends_tried,
            }

        last_result: Optional[BackendExecutionResult] = None
        last_backend_cfg = candidates[0]

        for backend_cfg in candidates:
            client = self.backend_clients.get(backend_cfg.id)
            if not client:
                logger.warning(f"No backend client registered for {backend_cfg.id}; skipping.")
                backends_tried.append({
                    "backend_service_id": backend_cfg.id,
                    "backend_name": backend_cfg.name,
                    "attempt": 0,
                    "status_code": 503,
                    "success": False,
                    "error": "No backend client registered",
                    "reason": f"Skipped '{backend_cfg.id}': no backend client registered. Falling back to next candidate according to policy '{decision.strategy_id}'.",
                })
                continue

            max_retries = failover_cfg.max_retries_per_backend if failover_cfg.enabled else 1
            delay = failover_cfg.retry_delay_seconds

            for attempt in range(max_retries):
                # Check envelope deadline before each attempt
                if envelope.is_expired():
                    reason = f"Request exceeded maximum wait deadline ({envelope.max_wait_seconds}s) before/during call on '{backend_cfg.id}'."
                    backends_tried.append({
                        "backend_service_id": backend_cfg.id,
                        "backend_name": backend_cfg.name,
                        "attempt": attempt + 1,
                        "status_code": 408,
                        "success": False,
                        "error": f"Request expired (max wait {envelope.max_wait_seconds}s)",
                        "reason": reason,
                    })
                    res = BackendExecutionResult(
                        success=False,
                        status_code=408,
                        error_message=f"Request expired before/during backend call (max wait {envelope.max_wait_seconds}s)",
                    )
                    _attach_trace(res)
                    return res, backend_cfg

                try:
                    result: BackendExecutionResult = await execute_fn(client, backend_cfg)
                    last_result = result
                    last_backend_cfg = backend_cfg

                    # Record outcome for circuit breaker / health
                    self.health_monitor.record_execution_outcome(
                        backend_cfg.id,
                        success=result.success,
                        status_code=result.status_code,
                        error=result.error_message,
                    )

                    if result.success:
                        backends_tried.append({
                            "backend_service_id": backend_cfg.id,
                            "backend_name": backend_cfg.name,
                            "attempt": attempt + 1,
                            "status_code": result.status_code,
                            "success": True,
                            "error": None,
                            "reason": f"Request served successfully by '{backend_cfg.id}'.",
                        })
                        _attach_trace(result)
                        return result, backend_cfg

                    err_msg = result.error_message or f"Backend returned status {result.status_code}"
                    # Check if status code warrants retry / failover
                    if result.status_code in failover_cfg.failover_on_statuses or "TIMEOUT" in str(result.error_message):
                        if attempt < max_retries - 1:
                            reason = (
                                f"Attempt {attempt + 1}/{max_retries} failed on '{backend_cfg.id}' with status {result.status_code}: {err_msg}. "
                                f"Retrying with backoff ({delay}s) according to policy '{decision.strategy_id}'."
                            )
                        else:
                            reason = (
                                f"Exhausted {max_retries} attempt(s) on '{backend_cfg.id}' with status {result.status_code}: {err_msg}. "
                                f"Failing over to next candidate backend according to policy '{decision.strategy_id}'."
                            )
                        backends_tried.append({
                            "backend_service_id": backend_cfg.id,
                            "backend_name": backend_cfg.name,
                            "attempt": attempt + 1,
                            "status_code": result.status_code,
                            "success": False,
                            "error": err_msg,
                            "reason": reason,
                        })
                        logger.warning(
                            f"Backend {backend_cfg.id} attempt {attempt + 1}/{max_retries} failed with "
                            f"status {result.status_code}. Retrying or failing over..."
                        )
                        if attempt < max_retries - 1:
                            await asyncio.sleep(delay)
                            delay *= failover_cfg.backoff_multiplier
                    else:
                        # Non-retryable client error (e.g. 400 Bad Request, 401 Unauthorized)
                        reason = (
                            f"Backend '{backend_cfg.id}' returned status {result.status_code} ({err_msg}) which is not configured for retry/failover in policy '{decision.strategy_id}'. Halting failover."
                        )
                        backends_tried.append({
                            "backend_service_id": backend_cfg.id,
                            "backend_name": backend_cfg.name,
                            "attempt": attempt + 1,
                            "status_code": result.status_code,
                            "success": False,
                            "error": err_msg,
                            "reason": reason,
                        })
                        _attach_trace(result)
                        return result, backend_cfg

                except Exception as e:
                    err_msg = str(e)
                    logger.error(f"Exception calling backend {backend_cfg.id}: {e}")
                    self.health_monitor.record_execution_outcome(
                        backend_cfg.id, success=False, status_code=500, error=err_msg
                    )
                    last_result = BackendExecutionResult(
                        success=False,
                        status_code=500,
                        error_message=f"Exception during backend call: {err_msg}",
                    )
                    last_backend_cfg = backend_cfg
                    if attempt < max_retries - 1:
                        reason = (
                            f"Attempt {attempt + 1}/{max_retries} encountered exception on '{backend_cfg.id}': {err_msg}. "
                            f"Retrying with backoff ({delay}s) according to policy '{decision.strategy_id}'."
                        )
                    else:
                        reason = (
                            f"Exhausted {max_retries} attempt(s) with exception on '{backend_cfg.id}': {err_msg}. "
                            f"Failing over to next candidate backend according to policy '{decision.strategy_id}'."
                        )
                    backends_tried.append({
                        "backend_service_id": backend_cfg.id,
                        "backend_name": backend_cfg.name,
                        "attempt": attempt + 1,
                        "status_code": 500,
                        "success": False,
                        "error": err_msg,
                        "reason": reason,
                    })
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay)
                        delay *= failover_cfg.backoff_multiplier

            # If we exhausted retries for this backend, continue to next backend in candidate list
            logger.info(f"Failing over from {backend_cfg.id} to next candidate backend...")

        # If all backends failed
        if last_result is None:
            last_result = BackendExecutionResult(
                success=False,
                status_code=503,
                error_message="All configured backends failed or were unavailable",
            )
        _attach_trace(last_result)
        return last_result, last_backend_cfg
