"""
Polymarket CLOB WebSocket Client

wss://ws-subscriptions-clob.polymarket.com/ws/market 채널 구독
Python 3.7+ 호환 (threading 기반, asyncio 미사용)

문서: https://docs.polymarket.com/developers/CLOB/websocket/market-channel
"""
from __future__ import annotations

import json
import threading
import time
from typing import Callable, Dict, List, Optional, Any
from collections import defaultdict

try:
    import websocket
except ImportError:
    raise ImportError("websocket-client 패키지 필요: pip install websocket-client")


# WebSocket 엔드포인트
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

# 재연결 설정
RECONNECT_DELAY_INITIAL = 1.0
RECONNECT_DELAY_MAX = 60.0
RECONNECT_DELAY_MULTIPLIER = 2.0

# Heartbeat 설정 (NAT/ALB idle timeout 방지)
PING_INTERVAL = 30  # 30초마다 ping
PING_TIMEOUT = 10   # 10초 내 pong 없으면 연결 끊김


class PolyWebSocket:
    """
    Polymarket CLOB WebSocket 클라이언트

    사용 예:
        ws = PolyWebSocket()
        ws.on_price_change(my_price_callback)
        ws.subscribe(["token_id_1", "token_id_2"])
        ws.run_forever()  # 백그라운드 스레드에서 실행
    """

    def __init__(self, url: str = WS_MARKET_URL):
        self.url = url
        self.ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._connected = False
        self._reconnect_delay = RECONNECT_DELAY_INITIAL

        # 구독 관리
        self._subscribed_assets: List[str] = []
        self._pending_subscribe: List[str] = []

        # 콜백 등록
        self._price_callbacks: List[Callable[[str, Dict], None]] = []
        self._book_callbacks: List[Callable[[str, Dict], None]] = []
        self._error_callbacks: List[Callable[[Exception], None]] = []
        self._connect_callbacks: List[Callable[[], None]] = []
        self._disconnect_callbacks: List[Callable[[], None]] = []

        # 가격 캐시 (asset_id → last_price)
        self._price_cache: Dict[str, float] = {}

        # 통계
        self._stats = {
            "messages_received": 0,
            "price_updates": 0,
            "book_updates": 0,
            "reconnects": 0,
            "errors": 0,
            "last_message_time": None,
        }

        # 락
        self._lock = threading.Lock()

    def on_price_change(self, callback: Callable[[str, Dict], None]):
        """가격 변동 콜백 등록 (asset_id, data)"""
        self._price_callbacks.append(callback)
        return self

    def on_book_update(self, callback: Callable[[str, Dict], None]):
        """오더북 업데이트 콜백 등록 (asset_id, data)"""
        self._book_callbacks.append(callback)
        return self

    def on_error(self, callback: Callable[[Exception], None]):
        """에러 콜백 등록"""
        self._error_callbacks.append(callback)
        return self

    def on_connect(self, callback: Callable[[], None]):
        """연결 성공 콜백 등록"""
        self._connect_callbacks.append(callback)
        return self

    def on_disconnect(self, callback: Callable[[], None]):
        """연결 끊김 콜백 등록"""
        self._disconnect_callbacks.append(callback)
        return self

    def subscribe(self, asset_ids: List[str]):
        """마켓 구독 (token_id/asset_id 리스트)"""
        with self._lock:
            new_assets = [a for a in asset_ids if a not in self._subscribed_assets]
            if not new_assets:
                return

            if self._connected and self.ws:
                # 즉시 구독
                self._send_subscribe(new_assets)
                self._subscribed_assets.extend(new_assets)
            else:
                # 연결 후 구독 대기
                self._pending_subscribe.extend(new_assets)

    def unsubscribe(self, asset_ids: List[str]):
        """마켓 구독 해제"""
        with self._lock:
            if self._connected and self.ws:
                msg = {
                    "type": "unsubscribe",
                    "channel": "market",
                    "assets_ids": asset_ids,  # 문서 기준 키
                }
                try:
                    self.ws.send(json.dumps(msg))
                except Exception:
                    pass

            for a in asset_ids:
                if a in self._subscribed_assets:
                    self._subscribed_assets.remove(a)

    def _send_subscribe(self, asset_ids: List[str]):
        """구독 메시지 전송 (문서 형식 준수)"""
        if not asset_ids:
            return

        # 문서: {"type": "market", "assets_ids": [...]}
        # https://docs.polymarket.com/quickstart/websocket/WSS-Quickstart
        msg = {
            "type": "market",
            "assets_ids": asset_ids,
        }
        try:
            self.ws.send(json.dumps(msg))
        except Exception as e:
            self._handle_error(e)

    def run_forever(self, background: bool = True):
        """
        WebSocket 실행

        Args:
            background: True면 백그라운드 스레드에서 실행
        """
        self._running = True

        if background:
            self._thread = threading.Thread(target=self._run_loop, daemon=True)
            self._thread.start()
        else:
            self._run_loop()

    def stop(self):
        """WebSocket 종료"""
        self._running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass

    def is_connected(self) -> bool:
        """연결 상태 확인"""
        return self._connected

    def get_stats(self) -> Dict[str, Any]:
        """통계 반환"""
        return dict(self._stats)

    def get_cached_price(self, asset_id: str) -> Optional[float]:
        """캐시된 가격 조회"""
        return self._price_cache.get(asset_id)

    def _run_loop(self):
        """메인 실행 루프 (재연결 포함)"""
        while self._running:
            try:
                self._connect()
            except Exception as e:
                self._handle_error(e)

            if not self._running:
                break

            # 재연결 대기 (exponential backoff)
            time.sleep(self._reconnect_delay)
            self._reconnect_delay = min(
                self._reconnect_delay * RECONNECT_DELAY_MULTIPLIER,
                RECONNECT_DELAY_MAX
            )
            self._stats["reconnects"] += 1

    def _connect(self):
        """WebSocket 연결"""
        self.ws = websocket.WebSocketApp(
            self.url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        # ping/pong 설정으로 연결 유지
        self.ws.run_forever(
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT,
        )

    def _on_open(self, ws):
        """연결 성공 핸들러"""
        self._connected = True
        self._reconnect_delay = RECONNECT_DELAY_INITIAL  # 재연결 딜레이 리셋

        # 대기 중인 구독 처리
        with self._lock:
            if self._pending_subscribe:
                self._send_subscribe(self._pending_subscribe)
                self._subscribed_assets.extend(self._pending_subscribe)
                self._pending_subscribe = []

            # 기존 구독 재구독 (재연결 시)
            if self._subscribed_assets:
                self._send_subscribe(self._subscribed_assets)

        # 콜백 호출
        for cb in self._connect_callbacks:
            try:
                cb()
            except Exception:
                pass

    def _on_message(self, ws, message: str):
        """메시지 수신 핸들러"""
        self._stats["messages_received"] += 1
        self._stats["last_message_time"] = time.time()

        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return

        # Polymarket WS can send arrays of events
        if isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    self._dispatch(item)
            return

        if isinstance(data, dict):
            self._dispatch(data)

    def _dispatch(self, data: dict):
        """단일 메시지 타입별 처리"""
        # 문서: https://docs.polymarket.com/developers/CLOB/websocket/market-channel
        event_type = data.get("event_type") or data.get("type")

        if event_type == "price_change":
            self._handle_price_change(data)
        elif event_type == "book":
            self._handle_book_update(data)
        elif event_type == "last_trade_price":
            self._handle_price_change(data)
        elif event_type == "tick_size_change":
            pass  # 무시

    def _handle_price_change(self, data: Dict):
        """가격 변동 처리"""
        asset_id = data.get("asset_id")
        if not asset_id:
            return

        # 가격 캐시 업데이트
        price = data.get("price")
        if price is not None:
            self._price_cache[asset_id] = float(price)

        self._stats["price_updates"] += 1

        # 콜백 호출
        for cb in self._price_callbacks:
            try:
                cb(asset_id, data)
            except Exception:
                pass

    def _handle_book_update(self, data: Dict):
        """오더북 업데이트 처리"""
        asset_id = data.get("asset_id")
        if not asset_id:
            return

        self._stats["book_updates"] += 1

        # 콜백 호출
        for cb in self._book_callbacks:
            try:
                cb(asset_id, data)
            except Exception:
                pass

    def _on_error(self, ws, error):
        """에러 핸들러"""
        self._handle_error(error if isinstance(error, Exception) else Exception(str(error)))

    def _on_close(self, ws, close_status_code, close_msg):
        """연결 종료 핸들러"""
        self._connected = False

        # 콜백 호출
        for cb in self._disconnect_callbacks:
            try:
                cb()
            except Exception:
                pass

    def _handle_error(self, error: Exception):
        """에러 처리"""
        self._stats["errors"] += 1

        for cb in self._error_callbacks:
            try:
                cb(error)
            except Exception:
                pass


class AssetPriceTracker:
    """
    자산별 가격 변동 추적기

    5분 윈도우 내 가격 변동 감지에 사용
    """

    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        # asset_id → [(timestamp, price), ...]
        self._history: Dict[str, List[tuple]] = defaultdict(list)
        self._lock = threading.Lock()

    def record(self, asset_id: str, price: float, timestamp: Optional[float] = None):
        """가격 기록"""
        ts = timestamp or time.time()
        with self._lock:
            self._history[asset_id].append((ts, price))
            self._cleanup(asset_id, ts)

    def get_price_delta(self, asset_id: str, lookback_seconds: Optional[int] = None) -> Optional[float]:
        """
        가격 변동 계산 (현재 - lookback)

        Returns:
            변동폭 (절대값 아님, 방향 포함) 또는 데이터 부족 시 None
        """
        lookback = lookback_seconds or self.window_seconds
        now = time.time()
        cutoff = now - lookback

        with self._lock:
            history = self._history.get(asset_id, [])
            if len(history) < 2:
                return None

            # 윈도우 시작점 가격
            old_price = None
            for ts, price in history:
                if ts >= cutoff:
                    break
                old_price = price

            if old_price is None:
                # 윈도우 시작 전 데이터 없음 → 가장 오래된 것 사용
                old_price = history[0][1]

            # 최신 가격
            new_price = history[-1][1]

            return new_price - old_price

    def get_current_price(self, asset_id: str) -> Optional[float]:
        """현재 가격 조회"""
        with self._lock:
            history = self._history.get(asset_id, [])
            return history[-1][1] if history else None

    def _cleanup(self, asset_id: str, now: float):
        """오래된 데이터 정리"""
        cutoff = now - self.window_seconds - 60  # 여유 1분
        history = self._history[asset_id]
        while history and history[0][0] < cutoff:
            history.pop(0)
