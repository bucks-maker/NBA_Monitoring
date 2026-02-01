# 전방 테스트 v2: 라인 매칭 + gap_t+3s 측정

## 목표
"Oracle move 후 **3초 딜레이 이후에도** gap ≥ 4%p가 남아 **체결 가능한가**?"

## 핵심 변경
1. totals/spreads: `alternate_*` 마켓으로 **Poly 고정 라인과 동일 라인** 비교
2. 측정: gap_t0, gap_t+3s, gap_t+10s, orderbook depth
3. 크레딧 절약: 트리거 시에만 event 단위 호출

---

## 1. 아키텍처

```
[Poly WebSocket] ──────────────────────────────────────┐
     │                                                  │
     ▼                                                  │
 poly_prices_live (1초 단위)                            │
     │                                                  │
     ▼                                                  ▼
[Anomaly Detector] ──(4%p 급변 감지)──> [Oracle 트리거 호출]
                                              │
                                              ▼
                                    /events/{eventId}/odds
                                    markets=h2h,alternate_totals,alternate_spreads
                                              │
                                              ▼
                                    [라인 매칭 + gap 계산]
                                              │
                                              ▼
                               ┌──────────────┴──────────────┐
                               ▼                             ▼
                         gap_t0 기록               schedule: t+3s, t+10s, t+30s
                                                           │
                                                           ▼
                                                   gap_series_hi_res
```

---

## 2. Odds API 호출 전략

### 기본 폴링 (h2h only)
- 간격: 60초
- 마켓: h2h
- 용도: h2h move 감지

### 트리거 호출 (event 단위)
- 조건: Poly 가격 4%p 급변 OR h2h move 감지
- 엔드포인트: `/events/{eventId}/odds`
- 마켓: `h2h,alternate_totals,alternate_spreads`
- 북메이커: `pinnacle`

### 크레딧 계산
| 항목 | 호출/시간 | 48시간 | 크레딧 |
|------|----------|--------|--------|
| h2h 폴링 (60초) | 60 | 2,880 | 2,880 |
| 트리거 호출 (예상 ~20/일) | ~1 | ~40 | 40 |
| **총계** | | | **~2,920** |

잔여: ~7,390 → 사용: ~2,920 → **여유: ~4,470**

---

## 3. DB 스키마 추가

```sql
-- 고정 라인 저장 (Poly)
ALTER TABLE market_mapping ADD COLUMN poly_line REAL;

-- 고해상도 이벤트
CREATE TABLE move_events_hi_res (
    id INTEGER PRIMARY KEY,
    game_key TEXT NOT NULL,
    market_type TEXT NOT NULL,  -- 'h2h', 'totals', 'spreads'
    poly_line REAL,             -- Poly 고정 라인 (totals/spreads용)
    oracle_line REAL,           -- Oracle 매칭된 라인
    move_ts_unix INTEGER NOT NULL,
    
    -- Oracle implied (de-vigged)
    oracle_prev_implied REAL,
    oracle_new_implied REAL,
    oracle_delta REAL,
    
    -- Poly 가격 시계열
    poly_t0 REAL,
    poly_t3s REAL,
    poly_t10s REAL,
    poly_t30s REAL,
    
    -- Gap 시계열
    gap_t0 REAL,
    gap_t3s REAL,
    gap_t10s REAL,
    gap_t30s REAL,
    
    -- Orderbook (체결 가능성)
    depth_t0 REAL,              -- best bid+ask size in $
    spread_t0 REAL,             -- bid-ask spread
    
    -- 메타
    trigger_source TEXT,        -- 'oracle_move' or 'poly_anomaly'
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- 1초 단위 gap 시계열
CREATE TABLE gap_series_hi_res (
    id INTEGER PRIMARY KEY,
    move_event_id INTEGER REFERENCES move_events_hi_res(id),
    ts_offset_sec INTEGER,      -- 0, 1, 2, 3, ..., 30
    poly_price REAL,
    gap REAL,
    bid REAL,
    ask REAL,
    depth REAL
);
```

---

## 4. 코드 변경

### A) `snapshot.py` 수정

```python
# 신규 함수: event 단위 Oracle 호출
def fetch_oracle_event_odds(event_id: str) -> dict:
    """
    /events/{eventId}/odds 호출
    markets=h2h,alternate_totals,alternate_spreads
    bookmakers=pinnacle
    """
    url = f"https://api.the-odds-api.com/v4/sports/basketball_nba/events/{event_id}/odds"
    params = {
        "apiKey": ODDS_API_KEY,
        "regions": "us",
        "markets": "h2h,alternate_totals,alternate_spreads",
        "bookmakers": "pinnacle",
    }
    resp = httpx.get(url, params=params)
    return resp.json()

def find_matching_line(oracle_data: dict, market_type: str, poly_line: float) -> tuple:
    """
    Oracle alternate_* 에서 poly_line과 매칭되는 라인 찾기
    Returns: (matched_line, outcome1_implied, outcome2_implied)
    """
    market_key = f"alternate_{market_type}"  # alternate_totals, alternate_spreads
    
    for bookmaker in oracle_data.get("bookmakers", []):
        if bookmaker["key"] != "pinnacle":
            continue
        
        for market in bookmaker.get("markets", []):
            if market["key"] != market_key:
                continue
            
            # 각 라인별 outcomes
            for outcome in market.get("outcomes", []):
                line = outcome.get("point")
                if line is None:
                    continue
                
                # 정확 매칭 또는 ±0.5
                if abs(line - poly_line) <= 0.5:
                    # 해당 라인의 implied 계산
                    odds = outcome.get("price", 2.0)
                    implied = 1 / odds
                    return (line, implied)
    
    return (None, None)
```

### B) `ws_client.py` 활용
- 기존 `AssetPriceTracker` 사용
- 1초 단위로 price/book 캐시

### C) `hi_res_capture.py` (신규)

```python
"""
고해상도 gap 캡처 모듈
트리거 발생 시 t+3s, t+10s, t+30s 스케줄링
"""
import threading
import time
from datetime import datetime

def schedule_gap_capture(
    conn,
    move_event_id: int,
    game_key: str,
    market_type: str,
    oracle_implied: float,
    poly_tracker: AssetPriceTracker,
    offsets: list = [3, 10, 30],
):
    """
    트리거 후 지정된 오프셋에서 gap 캡처
    """
    def capture_at_offset(offset_sec):
        time.sleep(offset_sec)
        
        # Poly 현재 가격
        poly_price = poly_tracker.get_current_price(game_key, market_type)
        bid, ask = poly_tracker.get_orderbook(game_key, market_type)
        depth = poly_tracker.get_depth(game_key, market_type)
        
        if poly_price is None:
            return
        
        gap = oracle_implied - poly_price
        
        # DB 저장
        conn.execute("""
            INSERT INTO gap_series_hi_res
            (move_event_id, ts_offset_sec, poly_price, gap, bid, ask, depth)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (move_event_id, offset_sec, poly_price, gap, bid, ask, depth))
        conn.commit()
        
        # move_events_hi_res 업데이트
        col_name = f"poly_t{offset_sec}s" if offset_sec < 60 else f"poly_t{offset_sec//60}m"
        gap_col = f"gap_t{offset_sec}s" if offset_sec < 60 else f"gap_t{offset_sec//60}m"
        conn.execute(f"""
            UPDATE move_events_hi_res
            SET {col_name} = ?, {gap_col} = ?
            WHERE id = ?
        """, (poly_price, abs(gap), move_event_id))
        conn.commit()
    
    # 각 오프셋에서 캡처 스케줄
    for offset in offsets:
        t = threading.Thread(target=capture_at_offset, args=(offset,))
        t.daemon = True
        t.start()
```

---

## 5. 트리거 조건

### Oracle Move 트리거 (h2h 폴링 기반)
```python
if abs(oracle_new_implied - oracle_prev_implied) >= 0.04:  # 4%p
    trigger_event_capture(game_key, 'h2h', 'oracle_move')
```

### Poly Anomaly 트리거 (WebSocket 기반)
```python
# 5분 윈도우 내 4%p 변동
if abs(current_price - price_5min_ago) >= 0.04:
    # Oracle event 단위 호출
    oracle_data = fetch_oracle_event_odds(event_id)
    trigger_event_capture(game_key, market_type, 'poly_anomaly')
```

---

## 6. 48시간 테스트 런북

### Day 1 (ET 기준)
| 시간 | 활동 |
|------|------|
| 12:00 PM | 서비스 시작, WebSocket 연결 |
| 7:00 PM - 1:00 AM | NBA 경기 시간대 집중 모니터링 |
| 매 60초 | h2h 폴링 |
| 트리거 시 | event 단위 alternate_* 호출 |

### Day 2
- Day 1 반복

### Day 3
| 시간 | 활동 |
|------|------|
| 오전 | 데이터 수집 종료 |
| 오후 | 분석 및 결론 |

---

## 7. 판정 기준

### 표본 수
| 조건 | 판정 |
|------|------|
| move_events_hi_res < 5 | 데이터 부족, 연장 필요 |
| move_events_hi_res ≥ 5 | 분석 가능 |

### 수익 가능성 (A/B)
| gap_t+3s ≥ 4%p 비율 | 판정 |
|---------------------|------|
| ≥ 30% | ✅ **A: 유망** |
| 10-30% | ⚠️ 약한 신호, 추가 검토 |
| < 10% | ❌ **B: 전략 불가** |

### 체결 가능성
| depth_t0 평균 | 판정 |
|---------------|------|
| ≥ $500 | 충분 |
| $100-500 | 제한적 |
| < $100 | 불가 |

---

## 8. 파일 구조

```
monitor/
├── snapshot.py          # 수정: fetch_oracle_event_odds 추가
├── ws_client.py         # 기존 유지
├── hi_res_capture.py    # 신규: gap 캡처 스케줄러
├── anomaly_detector.py  # 수정: Poly 트리거 조건 추가
└── schema.sql           # 수정: hi_res 테이블 추가
```
