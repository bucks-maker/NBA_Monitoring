"""
고해상도 gap 캡처 모듈

Forward Test v2: 트리거 발생 시 t+3s, t+10s, t+30s 스케줄링
핵심 질문: "Oracle move 후 3초 딜레이 이후에도 gap >= 4%p가 남아 체결 가능한가?"
"""
from __future__ import annotations

import sqlite3
import threading
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Callable, List, Any
from collections import defaultdict

try:
    from ws_client import AssetPriceTracker
except ImportError:
    AssetPriceTracker = None


class HiResCapture:
    """
    고해상도 gap 캡처 관리자

    트리거 발생 시 Poly 가격과 Oracle implied의 gap을
    t0, t+3s, t+10s, t+30s에서 캡처
    """

    DEFAULT_OFFSETS = [3, 10, 30]  # 초 단위

    def __init__(
        self,
        conn: sqlite3.Connection,
        offsets: Optional[List[int]] = None,
    ):
        self.conn = conn
        self.offsets = offsets or self.DEFAULT_OFFSETS

        # 활성 캡처 작업
        self._active_captures: Dict[int, List[threading.Thread]] = {}

        # Poly 가격 조회 함수 (외부 주입)
        self._price_getter: Optional[Callable] = None
        self._orderbook_getter: Optional[Callable] = None

        self._lock = threading.Lock()

        # 통계
        self._stats = {
            "captures_scheduled": 0,
            "captures_completed": 0,
            "captures_failed": 0,
        }

    def set_price_getter(self, fn: Callable[[str, str, str], Optional[float]]):
        """
        Poly 현재 가격 조회 함수 설정

        Args:
            fn: (game_id, market_type, outcome) -> price
        """
        self._price_getter = fn

    def set_orderbook_getter(
        self,
        fn: Callable[[str, str, str], tuple[Optional[float], Optional[float], Optional[float]]],
    ):
        """
        Poly 오더북 조회 함수 설정

        Args:
            fn: (game_id, market_type, outcome) -> (bid, ask, depth)
        """
        self._orderbook_getter = fn

    def record_move_event(
        self,
        game_key: str,
        market_type: str,
        trigger_source: str,  # 'oracle_move' or 'poly_anomaly'
        oracle_prev_implied: Optional[float],
        oracle_new_implied: Optional[float],
        poly_t0: Optional[float],
        poly_line: Optional[float] = None,
        oracle_line: Optional[float] = None,
        outcome_name: Optional[str] = None,
        depth_t0: Optional[float] = None,
        spread_t0: Optional[float] = None,
    ) -> Optional[int]:
        """
        고해상도 무브 이벤트 기록 (t0)

        Returns:
            move_event_id (for scheduling follow-up captures)
        """
        move_ts = int(time.time())

        oracle_delta = None
        if oracle_prev_implied is not None and oracle_new_implied is not None:
            oracle_delta = oracle_new_implied - oracle_prev_implied

        gap_t0 = None
        if oracle_new_implied is not None and poly_t0 is not None:
            gap_t0 = abs(oracle_new_implied - poly_t0)

        try:
            cur = self.conn.execute("""
                INSERT INTO move_events_hi_res
                (game_key, market_type, poly_line, oracle_line, move_ts_unix,
                 oracle_prev_implied, oracle_new_implied, oracle_delta,
                 poly_t0, gap_t0, depth_t0, spread_t0,
                 trigger_source, outcome_name)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                game_key, market_type, poly_line, oracle_line, move_ts,
                oracle_prev_implied, oracle_new_implied, oracle_delta,
                poly_t0, gap_t0, depth_t0, spread_t0,
                trigger_source, outcome_name,
            ))
            self.conn.commit()

            move_event_id = cur.lastrowid

            # gap_series에 t0 기록
            self._record_gap_series(move_event_id, 0, poly_t0, gap_t0, None, None, depth_t0)

            return move_event_id

        except Exception as e:
            print(f"[HiResCapture] 이벤트 기록 실패: {e}")
            return None

    def schedule_captures(
        self,
        move_event_id: int,
        game_key: str,
        market_type: str,
        outcome: str,
        oracle_implied: float,
    ):
        """
        트리거 후 지정된 오프셋에서 gap 캡처 스케줄링

        Args:
            move_event_id: move_events_hi_res ID
            game_key: 게임 ID
            market_type: 'h2h', 'totals', 'spreads'
            outcome: 'Over', 'Under', 'Home', 'Away' 등
            oracle_implied: Oracle fair implied probability
        """
        if self._price_getter is None:
            print("[HiResCapture] price_getter 미설정, 스케줄 생략")
            return

        threads = []

        for offset in self.offsets:
            t = threading.Thread(
                target=self._capture_at_offset,
                args=(move_event_id, game_key, market_type, outcome, oracle_implied, offset),
                daemon=True,
            )
            t.start()
            threads.append(t)

        with self._lock:
            self._active_captures[move_event_id] = threads
            self._stats["captures_scheduled"] += len(self.offsets)

    def _capture_at_offset(
        self,
        move_event_id: int,
        game_key: str,
        market_type: str,
        outcome: str,
        oracle_implied: float,
        offset_sec: int,
    ):
        """특정 오프셋에서 gap 캡처 (별도 스레드)"""
        time.sleep(offset_sec)

        try:
            # Poly 현재 가격 조회
            poly_price = self._price_getter(game_key, market_type, outcome)

            bid = ask = depth = None
            if self._orderbook_getter:
                bid, ask, depth = self._orderbook_getter(game_key, market_type, outcome)

            if poly_price is None:
                self._stats["captures_failed"] += 1
                return

            gap = abs(oracle_implied - poly_price)

            # gap_series 기록
            self._record_gap_series(move_event_id, offset_sec, poly_price, gap, bid, ask, depth)

            # move_events_hi_res 업데이트
            col_poly = f"poly_t{offset_sec}s"
            col_gap = f"gap_t{offset_sec}s"

            self.conn.execute(f"""
                UPDATE move_events_hi_res
                SET {col_poly} = ?, {col_gap} = ?
                WHERE id = ?
            """, (poly_price, gap, move_event_id))
            self.conn.commit()

            self._stats["captures_completed"] += 1

            # 로그 (4%p 이상이면 강조)
            if gap >= 0.04:
                print(f"  [HiRes] t+{offset_sec}s: gap={gap*100:.1f}%p (poly={poly_price:.3f}) **ACTIONABLE**")
            else:
                print(f"  [HiRes] t+{offset_sec}s: gap={gap*100:.1f}%p (poly={poly_price:.3f})")

        except Exception as e:
            print(f"[HiResCapture] t+{offset_sec}s 캡처 실패: {e}")
            self._stats["captures_failed"] += 1

    def _record_gap_series(
        self,
        move_event_id: int,
        ts_offset: int,
        poly_price: Optional[float],
        gap: Optional[float],
        bid: Optional[float],
        ask: Optional[float],
        depth: Optional[float],
    ):
        """gap_series_hi_res에 기록"""
        try:
            self.conn.execute("""
                INSERT INTO gap_series_hi_res
                (move_event_id, ts_offset_sec, poly_price, gap, bid, ask, depth)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (move_event_id, ts_offset, poly_price, gap, bid, ask, depth))
            self.conn.commit()
        except Exception as e:
            print(f"[HiResCapture] gap_series 기록 실패: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """통계 반환"""
        return dict(self._stats)


def de_vig_implied(odds1: float, odds2: float) -> tuple[float, float]:
    """
    2-way 오즈에서 de-vig (fair probability) 계산

    Args:
        odds1: 첫 번째 outcome의 decimal odds
        odds2: 두 번째 outcome의 decimal odds

    Returns:
        (fair_prob1, fair_prob2)
    """
    if odds1 <= 0 or odds2 <= 0:
        return (0.5, 0.5)

    implied1 = 1 / odds1
    implied2 = 1 / odds2
    total = implied1 + implied2

    if total <= 0:
        return (0.5, 0.5)

    return (implied1 / total, implied2 / total)
