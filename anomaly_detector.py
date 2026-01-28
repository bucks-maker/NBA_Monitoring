"""
Polymarket 실시간 데이터 기반 이상 감지

Pinnacle 호출 트리거 조건 (모델 없이 강한 신호만):
1. 가격 급변: |Δprice| >= 5%p (5분 내)
2. 오더북 스프레드: bid-ask 스프레드 >= 5%p (thin book)
3. Yes/No 합계 불일치: |1 - (yes + no)| >= 3%p (차익거래 기회 또는 유동성 이상)
4. 대량 거래: 거래량 급증 감지

NOTE: 교차 마켓 불일치 (ML vs Spread vs Total)는 모델 없이 직접 비교 불가능하므로 제외
"""
from __future__ import annotations

import time
import threading
from typing import Dict, List, Optional, Any, Callable
from collections import defaultdict
from dataclasses import dataclass, field


# 트리거 임계값
PRICE_CHANGE_THRESHOLD = 0.05      # 5%p 가격 변동
PRICE_WINDOW_SECONDS = 300         # 5분 윈도우
BID_ASK_SPREAD_THRESHOLD = 0.05    # 5%p bid-ask 스프레드
YES_NO_DEVIATION_THRESHOLD = 0.03  # Yes+No가 1에서 3%p 이상 벗어남
PINNACLE_COOLDOWN_SECONDS = 1800   # 게임당 최소 30분 쿨다운


@dataclass
class AnomalyEvent:
    """이상 감지 이벤트"""
    game_id: str
    market_type: str  # 'total', 'spread', 'moneyline'
    anomaly_type: str  # 'price_change', 'orderbook_spread', 'yes_no_deviation'
    timestamp: float
    details: Dict[str, Any] = field(default_factory=dict)

    def __str__(self):
        return (
            f"[{self.anomaly_type}] game={self.game_id} market={self.market_type} "
            f"details={self.details}"
        )


class AnomalyDetector:
    """
    Polymarket 실시간 데이터 기반 이상 감지기

    WebSocket으로 수신된 가격/오더북 데이터를 분석하여
    Pinnacle 호출 트리거 여부 결정
    """

    def __init__(
        self,
        price_threshold: float = PRICE_CHANGE_THRESHOLD,
        price_window: int = PRICE_WINDOW_SECONDS,
        spread_threshold: float = BID_ASK_SPREAD_THRESHOLD,
        yes_no_threshold: float = YES_NO_DEVIATION_THRESHOLD,
        cooldown_seconds: int = PINNACLE_COOLDOWN_SECONDS,
    ):
        self.price_threshold = price_threshold
        self.price_window = price_window
        self.spread_threshold = spread_threshold
        self.yes_no_threshold = yes_no_threshold
        self.cooldown_seconds = cooldown_seconds

        # 가격 히스토리: (game_id, market_type, outcome) → [(ts, price), ...]
        self._price_history: Dict[tuple, List[tuple]] = defaultdict(list)

        # 최신 오더북: (game_id, market_type, outcome) → {bid, ask}
        self._orderbook: Dict[tuple, Dict[str, float]] = {}

        # 최신 가격 쌍: (game_id, market_type) → {yes_price, no_price}
        self._price_pairs: Dict[tuple, Dict[str, float]] = defaultdict(dict)

        # Pinnacle 호출 쿨다운: game_id → last_call_time
        self._pinnacle_cooldown: Dict[str, float] = {}

        # 이상 감지 콜백
        self._anomaly_callbacks: List[Callable[[AnomalyEvent], None]] = []

        # 통계
        self._stats = {
            "price_anomalies": 0,
            "spread_anomalies": 0,
            "yes_no_anomalies": 0,
            "pinnacle_triggers": 0,
            "cooldown_blocks": 0,
        }

        self._lock = threading.Lock()

    def on_anomaly(self, callback: Callable[[AnomalyEvent], None]):
        """이상 감지 콜백 등록"""
        self._anomaly_callbacks.append(callback)
        return self

    def update_price(
        self,
        game_id: str,
        market_type: str,
        outcome: str,  # 'yes', 'no', 'over', 'under', 'home', 'away'
        price: float,
        timestamp: Optional[float] = None,
    ) -> Optional[AnomalyEvent]:
        """
        가격 업데이트 및 이상 감지

        Returns:
            AnomalyEvent if anomaly detected, else None
        """
        ts = timestamp or time.time()
        key = (game_id, market_type, outcome)
        pair_key = (game_id, market_type)

        with self._lock:
            # 가격 히스토리 기록
            self._price_history[key].append((ts, price))
            self._cleanup_history(key, ts)

            # 가격 쌍 업데이트
            normalized_outcome = self._normalize_outcome(outcome)
            self._price_pairs[pair_key][normalized_outcome] = price

            # 1. 가격 급변 감지
            anomaly = self._check_price_anomaly(game_id, market_type, key, price, ts)
            if anomaly:
                return anomaly

            # 2. Yes/No 합계 불일치 감지
            anomaly = self._check_yes_no_anomaly(game_id, market_type, pair_key, ts)
            if anomaly:
                return anomaly

        return None

    def update_orderbook(
        self,
        game_id: str,
        market_type: str,
        outcome: str,
        best_bid: float,
        best_ask: float,
        timestamp: Optional[float] = None,
    ) -> Optional[AnomalyEvent]:
        """
        오더북 업데이트 및 이상 감지

        Returns:
            AnomalyEvent if anomaly detected, else None
        """
        ts = timestamp or time.time()
        key = (game_id, market_type, outcome)

        with self._lock:
            self._orderbook[key] = {"bid": best_bid, "ask": best_ask}

            # 3. 오더북 스프레드 이상 감지
            spread = best_ask - best_bid
            if spread >= self.spread_threshold:
                self._stats["spread_anomalies"] += 1
                event = AnomalyEvent(
                    game_id=game_id,
                    market_type=market_type,
                    anomaly_type="orderbook_spread",
                    timestamp=ts,
                    details={
                        "outcome": outcome,
                        "best_bid": best_bid,
                        "best_ask": best_ask,
                        "spread": spread,
                    },
                )
                self._fire_anomaly(event)
                return event

        return None

    def _check_price_anomaly(
        self,
        game_id: str,
        market_type: str,
        key: tuple,
        current_price: float,
        now: float,
    ) -> Optional[AnomalyEvent]:
        """가격 급변 감지 (5분 윈도우)"""
        history = self._price_history[key]
        if len(history) < 2:
            return None

        # 윈도우 시작 가격 찾기
        cutoff = now - self.price_window
        old_price = None
        for ts, price in history:
            if ts >= cutoff:
                break
            old_price = price

        if old_price is None:
            # 윈도우 내 데이터만 있으면 가장 오래된 것
            if len(history) >= 2:
                old_price = history[0][1]
            else:
                return None

        delta = current_price - old_price

        if abs(delta) >= self.price_threshold:
            self._stats["price_anomalies"] += 1
            event = AnomalyEvent(
                game_id=game_id,
                market_type=market_type,
                anomaly_type="price_change",
                timestamp=now,
                details={
                    "outcome": key[2],
                    "old_price": old_price,
                    "new_price": current_price,
                    "delta": delta,
                    "window_seconds": self.price_window,
                },
            )
            self._fire_anomaly(event)
            return event

        return None

    def _check_yes_no_anomaly(
        self,
        game_id: str,
        market_type: str,
        pair_key: tuple,
        now: float,
    ) -> Optional[AnomalyEvent]:
        """Yes/No 합계 불일치 감지 (즉시 엣지 기회)"""
        prices = self._price_pairs.get(pair_key, {})
        yes_price = prices.get("yes")
        no_price = prices.get("no")

        if yes_price is None or no_price is None:
            return None

        total = yes_price + no_price
        deviation = abs(1.0 - total)

        if deviation >= self.yes_no_threshold:
            self._stats["yes_no_anomalies"] += 1
            event = AnomalyEvent(
                game_id=game_id,
                market_type=market_type,
                anomaly_type="yes_no_deviation",
                timestamp=now,
                details={
                    "yes_price": yes_price,
                    "no_price": no_price,
                    "total": total,
                    "deviation": deviation,
                    # Yes+No < 1 이면 두 쪽 다 사서 확정 이득 가능
                    "arbitrage_opportunity": total < 1.0 - 0.01,  # 수수료 고려
                },
            )
            self._fire_anomaly(event)
            return event

        return None

    def should_call_pinnacle(self, game_id: str) -> bool:
        """
        Pinnacle API 호출 여부 결정 (쿨다운 체크)

        Returns:
            True if should call, False if still in cooldown
        """
        now = time.time()
        with self._lock:
            last_call = self._pinnacle_cooldown.get(game_id, 0)
            if now - last_call < self.cooldown_seconds:
                self._stats["cooldown_blocks"] += 1
                return False
            return True

    def mark_pinnacle_called(self, game_id: str):
        """Pinnacle 호출 기록 (쿨다운 시작)"""
        with self._lock:
            self._pinnacle_cooldown[game_id] = time.time()
            self._stats["pinnacle_triggers"] += 1

    def get_anomaly_summary(self, game_id: str) -> Dict[str, Any]:
        """게임별 이상 감지 요약"""
        with self._lock:
            result = {
                "game_id": game_id,
                "markets": {},
            }

            for (gid, mtype, outcome), history in self._price_history.items():
                if gid != game_id:
                    continue
                if mtype not in result["markets"]:
                    result["markets"][mtype] = {}
                if history:
                    result["markets"][mtype][outcome] = {
                        "current_price": history[-1][1],
                        "history_count": len(history),
                    }

            return result

    def get_stats(self) -> Dict[str, Any]:
        """통계 반환"""
        return dict(self._stats)

    def _normalize_outcome(self, outcome: str) -> str:
        """outcome을 yes/no로 정규화"""
        outcome_lower = outcome.lower()
        if outcome_lower in ("yes", "over", "home"):
            return "yes"
        elif outcome_lower in ("no", "under", "away"):
            return "no"
        return outcome_lower

    def _cleanup_history(self, key: tuple, now: float):
        """오래된 히스토리 정리"""
        cutoff = now - self.price_window - 60
        history = self._price_history[key]
        while history and history[0][0] < cutoff:
            history.pop(0)

    def _fire_anomaly(self, event: AnomalyEvent):
        """이상 감지 콜백 호출"""
        for cb in self._anomaly_callbacks:
            try:
                cb(event)
            except Exception:
                pass


class TriggerManager:
    """
    트리거 관리자

    이상 감지 → Pinnacle 호출 → 갭 기록 파이프라인 관리
    """

    def __init__(
        self,
        detector: AnomalyDetector,
        pinnacle_callback: Optional[Callable[[str], None]] = None,
    ):
        self.detector = detector
        self.pinnacle_callback = pinnacle_callback

        # 대기 중인 트리거: game_id → [AnomalyEvent, ...]
        self._pending_triggers: Dict[str, List[AnomalyEvent]] = defaultdict(list)

        # 처리 완료 트리거
        self._processed_triggers: List[Dict[str, Any]] = []

        self._lock = threading.Lock()

    def process_anomaly(self, event: AnomalyEvent):
        """이상 감지 이벤트 처리"""
        game_id = event.game_id

        with self._lock:
            # 대기열에 추가
            self._pending_triggers[game_id].append(event)

        # Pinnacle 호출 필요 여부 확인
        if self.detector.should_call_pinnacle(game_id):
            self._trigger_pinnacle(game_id)

    def _trigger_pinnacle(self, game_id: str):
        """Pinnacle API 호출 트리거"""
        self.detector.mark_pinnacle_called(game_id)

        if self.pinnacle_callback:
            try:
                self.pinnacle_callback(game_id)
            except Exception:
                pass

        # 대기열 정리
        with self._lock:
            events = self._pending_triggers.pop(game_id, [])
            self._processed_triggers.append({
                "game_id": game_id,
                "timestamp": time.time(),
                "events": [str(e) for e in events],
            })

    def get_pending_triggers(self) -> Dict[str, int]:
        """대기 중인 트리거 수"""
        with self._lock:
            return {gid: len(events) for gid, events in self._pending_triggers.items()}

    def get_processed_count(self) -> int:
        """처리 완료 트리거 수"""
        return len(self._processed_triggers)
