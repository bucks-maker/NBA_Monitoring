"""
RebalanceTracker: 이벤트별 모든 outcome의 best_ask 합계를 실시간 추적

negativeRisk 멀티 아웃컴 이벤트(NBA 챔피언, MVP 등)에서
모든 YES outcome의 best_ask 합이 $1.00 미만이 되면 차익거래 기회.
"""
from __future__ import annotations

import threading
import time
from typing import Callable, Dict, List, Optional, Any


class RebalanceTracker:
    """이벤트별 모든 outcome의 best_ask 합계를 실시간 추적"""

    def __init__(
        self,
        threshold: float = 1.0,
        strong_threshold: float = 0.995,
        min_depth: float = 100.0,
        on_opportunity: Optional[Callable[[Dict[str, Any]], None]] = None,
    ):
        self.threshold = threshold
        self.strong_threshold = strong_threshold
        self.min_depth = min_depth
        self._on_opportunity = on_opportunity

        # 토큰 → 이벤트 매핑
        self.token_to_event: Dict[str, str] = {}
        # 토큰 → outcome 이름
        self.token_to_outcome: Dict[str, str] = {}
        # 토큰별 최신 best_ask
        self.best_asks: Dict[str, float] = {}
        # 토큰별 ask depth (best_ask 가격의 총 size)
        self.ask_depths: Dict[str, float] = {}
        # 이벤트 → 소속 토큰 목록
        self.event_tokens: Dict[str, List[str]] = {}
        # 이벤트 정보
        self.event_info: Dict[str, Dict[str, Any]] = {}
        # 이벤트별 마지막 합계 캐시
        self._event_sums: Dict[str, float] = {}

        # 알림 중복 방지: {event_id: (last_alert_time, last_alert_sum)}
        self._alert_cooldown: Dict[str, tuple] = {}
        self._alert_cooldown_sec = 60.0  # 같은 이벤트 재알림 최소 간격
        self._alert_sum_delta = 0.005  # sum이 0.5%p 이상 변해야 재알림

        self.stats = {
            "book_updates": 0,
            "opportunities_found": 0,
            "strong_opportunities": 0,
        }

        self._lock = threading.Lock()

    def register_event(
        self,
        event_id: str,
        title: str,
        tokens: List[Dict[str, str]],
    ):
        """Gamma API에서 발견된 이벤트 등록

        Args:
            event_id: 이벤트 ID
            title: 이벤트 제목
            tokens: [{"token_id": ..., "outcome": ..., "price": optional float}, ...]
                    price가 있으면 초기 best_ask seed로 사용
        """
        with self._lock:
            self.event_info[event_id] = {
                "title": title,
                "n_outcomes": len(tokens),
            }
            self.event_tokens[event_id] = []

            for t in tokens:
                tid = t["token_id"]
                outcome = t["outcome"]
                self.token_to_event[tid] = event_id
                self.token_to_outcome[tid] = outcome
                self.event_tokens[event_id].append(tid)

                # Gamma API 가격을 초기 seed로 사용
                price = t.get("price")
                if price is not None and price > 0:
                    self.best_asks[tid] = price

            # 초기 seed로 합계 계산
            self._recalculate_event(event_id)

    def update_best_ask(self, token_id: str, best_ask: float):
        """price_change 이벤트에서 best_ask 직접 업데이트

        WebSocket price_change 메시지에 best_ask 필드가 포함되어 있을 때 사용.
        book 이벤트보다 훨씬 빈번하게 발생하므로 주 데이터 소스.
        """
        if best_ask <= 0:
            return

        opportunity = None

        with self._lock:
            if token_id not in self.token_to_event:
                return

            self.stats["book_updates"] += 1
            self.best_asks[token_id] = best_ask
            # price_change에는 depth 정보 없음 — 기존 값 유지

            event_id = self.token_to_event[token_id]
            opportunity = self._recalculate_event(event_id)

        if opportunity and self._on_opportunity:
            try:
                self._on_opportunity(opportunity)
            except Exception:
                pass

    def update_book(self, token_id: str, data: Dict[str, Any]):
        """WebSocket book 이벤트 수신 시 호출

        Args:
            token_id: asset_id (YES 토큰)
            data: {"asks": [{"price": "0.56", "size": "100"}, ...], "bids": [...]}
        """
        asks = data.get("asks", [])
        if not asks:
            return

        opportunity = None

        with self._lock:
            if token_id not in self.token_to_event:
                return

            self.stats["book_updates"] += 1

            # best_ask = asks에서 가장 낮은 가격
            best_ask = None
            best_ask_depth = 0.0  # $ value at best ask level

            for ask in asks:
                try:
                    price = float(ask.get("price") or 0)
                    size = float(ask.get("size") or 0)
                except (TypeError, ValueError):
                    continue
                if price > 0 and size > 0:
                    if best_ask is None or price < best_ask:
                        best_ask = price
                        best_ask_depth = size * price
                    elif abs(price - best_ask) < 1e-9:
                        best_ask_depth += size * price

            if best_ask is None:
                return

            self.best_asks[token_id] = best_ask
            self.ask_depths[token_id] = best_ask_depth

            # 해당 이벤트 합계 재계산
            event_id = self.token_to_event[token_id]
            opportunity = self._recalculate_event(event_id)

        # 콜백은 lock 밖에서 호출 (deadlock 방지)
        if opportunity and self._on_opportunity:
            try:
                self._on_opportunity(opportunity)
            except Exception:
                pass

    def _recalculate_event(self, event_id: str) -> Optional[Dict[str, Any]]:
        """이벤트 합계 재계산. lock 보유 상태에서 호출.
        기회 발견 시 opportunity dict 반환, 없으면 None."""
        tokens = self.event_tokens.get(event_id, [])
        if not tokens:
            return None

        asks = []
        for tid in tokens:
            ask = self.best_asks.get(tid)
            if ask is None:
                return None  # 아직 모든 토큰 데이터가 없음
            asks.append(ask)

        total = sum(asks)

        # dead market 필터: 최대 best_ask가 0.02 이하면 정산된 마켓
        if max(asks) <= 0.02:
            self._event_sums[event_id] = None
            return None

        self._event_sums[event_id] = total

        if total >= self.threshold:
            return None

        # 중복 알림 방지: 쿨다운 내에서 sum 변화 미미하면 스킵
        now = time.time()
        prev = self._alert_cooldown.get(event_id)
        if prev:
            prev_time, prev_sum = prev
            if (now - prev_time < self._alert_cooldown_sec
                    and abs(total - prev_sum) < self._alert_sum_delta):
                return None

        self._alert_cooldown[event_id] = (now, total)

        # 기회 발견
        info = self.event_info.get(event_id, {})
        depths = [self.ask_depths.get(tid, 0) for tid in tokens]
        min_d = min(depths) if depths else 0

        is_strong = total < self.strong_threshold
        is_executable = min_d >= self.min_depth

        self.stats["opportunities_found"] += 1
        if is_strong:
            self.stats["strong_opportunities"] += 1

        opportunity = {
            "timestamp": time.time(),
            "event_id": event_id,
            "title": info.get("title", "?"),
            "n_outcomes": info.get("n_outcomes", 0),
            "sum": total,
            "gap": 1.0 - total,
            "gap_pct": (1.0 - total) * 100,
            "is_strong": is_strong,
            "is_executable": is_executable,
            "min_depth": min_d,
            "verified": False,  # CLOB 검증 전
            "outcomes": [],
        }

        for tid in tokens:
            opportunity["outcomes"].append({
                "token_id": tid,
                "outcome": self.token_to_outcome.get(tid, "?"),
                "best_ask": self.best_asks.get(tid, 0),
                "depth": self.ask_depths.get(tid, 0),
            })

        return opportunity

    def get_event_summary(self, event_id: str) -> Optional[Dict[str, Any]]:
        """이벤트의 현재 상태 반환"""
        with self._lock:
            if event_id not in self.event_info:
                return None

            info = self.event_info[event_id]
            tokens = self.event_tokens.get(event_id, [])

            outcomes = []
            total = 0.0
            all_have_data = True

            for tid in tokens:
                ask = self.best_asks.get(tid)
                if ask is None:
                    all_have_data = False
                    outcomes.append({
                        "outcome": self.token_to_outcome.get(tid, "?"),
                        "best_ask": None,
                        "depth": 0,
                    })
                else:
                    total += ask
                    outcomes.append({
                        "outcome": self.token_to_outcome.get(tid, "?"),
                        "best_ask": ask,
                        "depth": self.ask_depths.get(tid, 0),
                    })

            return {
                "event_id": event_id,
                "title": info.get("title", "?"),
                "n_outcomes": info.get("n_outcomes", 0),
                "sum": total if all_have_data else None,
                "gap": (1.0 - total) if all_have_data else None,
                "all_have_data": all_have_data,
                "outcomes": outcomes,
            }

    def get_all_event_sums(self) -> List[Dict[str, Any]]:
        """모든 이벤트의 합계를 반환 (정렬: 합계 오름차순)"""
        with self._lock:
            results = []
            for event_id, info in self.event_info.items():
                total = self._event_sums.get(event_id)
                tokens = self.event_tokens.get(event_id, [])
                n_with_data = sum(1 for tid in tokens if tid in self.best_asks)

                results.append({
                    "event_id": event_id,
                    "title": info.get("title", "?"),
                    "n_outcomes": info.get("n_outcomes", 0),
                    "n_with_data": n_with_data,
                    "sum": total,
                    "gap": (1.0 - total) if total is not None else None,
                })

            results.sort(key=lambda x: (x["sum"] is None, x["sum"] or 999))
            return results

    @property
    def registered_token_ids(self) -> List[str]:
        """등록된 모든 토큰 ID"""
        with self._lock:
            return list(self.token_to_event.keys())

    @property
    def n_events(self) -> int:
        with self._lock:
            return len(self.event_info)

    @property
    def n_tokens(self) -> int:
        with self._lock:
            return len(self.token_to_event)
