# Pinnacle-Polymarket NBA Line Monitor

## 목적
"효율적 시장(Pinnacle)의 가격 변동을 비효율적 시장(Polymarket)이 반영하기 전에 사는 것"
이 가설을 전방 검증(forward validation)하기 위한 모니터링 파이프라인.

## 검증 대상 가설
- Pinnacle 라인/가격이 크게 움직이면 (|Δline| >= 1.5 or |Δimplied| >= 6%p)
- Polymarket 가격은 느리게 따라온다 (lag)
- 봇(0x6e82b93e)은 그 갭 구간에서 진입한다
- 갭이 클수록 승률/ROI가 높다

## 핵심 가설 수식
```
수익 = Σ (지연시간 × 가격왜곡폭) for each trigger event
```

## 아키텍처 (두 가지 모드)

### REST 폴링 모드 (기본)
```
[Pinnacle Snapshotter] → SQLite ← [Polymarket Snapshotter]
         ↓                              ↓
    [Move Detector] ──trigger──→ [Gap Recorder]
                                       ↓
                              [Bot Trade Checker]
                                       ↓
                                 [Report/Alert]
```

### WebSocket 모드 (--ws)
```
[Polymarket WebSocket] ──(실시간)──> [Anomaly Detector]
         │                                   │
         │                          (이상 감지 시에만)
         │                                   ↓
         │                          [Pinnacle Oracle]
         │                                   │
         ↓                                   ↓
   [SQLite DB] <────────────────────[Gap Recorder]
         │                                   │
         ↓                                   ↓
   [Bot Trade Checker]              [Alert/Report]
```

## 모드 비교

| 항목 | REST 폴링 모드 | WebSocket 모드 |
|------|---------------|----------------|
| Polymarket | 30초 REST 폴링 | WebSocket 실시간 |
| Pinnacle | 1시간 (트리거 시 15분) | 이상 감지 시에만 |
| Odds API 크레딧 | ~400-600/월 | ~100/월 |
| 반응 지연 | 최대 30초 | <1초 |
| 트리거 방식 | Pinnacle 라인 변동 | Poly 이상 → Pinnacle 확인 |

## 데이터 소스
- Pinnacle: The Odds API (월 500회)
  - REST: 기본 1시간 간격, 트리거 발생 시 15분 간격
  - WebSocket: 이상 감지 시에만 호출 (30분 쿨다운)
- Polymarket: Gamma API (무료, 무제한) / WebSocket CLOB
  - REST: 30초 간격 서브폴링
  - WebSocket: wss://ws-subscriptions-clob.polymarket.com/ws/market
- 봇 거래: Polymarket Activity API (`/activity?user=BOT_ADDRESS`)

## DB: SQLite
- 파일: monitor/data/snapshots.db
- 테이블: pinnacle_snapshots, poly_snapshots, triggers, bot_trades, game_mapping

## 파일 구조
```
monitor/
  CLAUDE.md              - 이 파일
  schema.sql             - DB 스키마
  snapshot.py            - 메인 수집기 (REST/WebSocket 모드)
  ws_client.py           - WebSocket 클라이언트
  anomaly_detector.py    - 이상 감지 엔진
  hi_res_capture.py      - Forward Test v2: 고해상도 gap 캡처
  hi_res_analysis.py     - Forward Test v2: 결과 분석 스크립트
  report.py              - 분석 리포트 출력
  data/
    snapshots.db         - SQLite DB
```

## 이상 감지 트리거 (WebSocket 모드)

모델 없이 강한 신호만 사용 (오탐 최소화):

### 1. 가격 급변 (5분 윈도우)
```python
if abs(current_price - price_5min_ago) >= 0.05:  # 5%p
    call_pinnacle()
```

### 2. 오더북 스프레드 (thin book)
```python
if (best_ask - best_bid) >= 0.05:  # 5%p
    call_pinnacle()
```

### 3. Yes/No 합계 불일치 (차익거래 기회)
```python
total = yes_price + no_price
if abs(1.0 - total) >= 0.03:  # 3%p
    call_pinnacle()
```

**제외된 트리거:**
- ~~교차 마켓 불일치 (ML vs Spread vs Total)~~ - 모델 없이 직접 비교 불가

## 트리거 임계값

### REST 모드
- |Δline| >= 1.5 점
- |Δimplied_under| >= 6%p
- |Δimplied_over| >= 6%p

### WebSocket 모드
- |Δprice| >= 5%p (5분 윈도우)
- bid-ask spread >= 5%p
- |1 - (yes+no)| >= 3%p
- Pinnacle 쿨다운: 게임당 30분

## 타임존 스케줄링
- 모든 시간은 US Eastern Time (ET) 기준
- 활성 시간대: ET 10:00 ~ 03:00 (다음날)
- 비활성: ET 03:00 ~ 10:00 (자동 sleep)
- Polymarket slug 날짜도 ET 기준으로 생성 (EDT/EST 자동 처리)

## Odds API 크레딧 관리

### REST 모드
- 월 500회, 하루 ~16회
- 전략: ET 활성 시간대만, 1시간 간격, 트리거 시 15분 전환
  - ET 10:00~03:00 = 17시간, 1시간 간격 = ~17회/일
  - 트리거 시 15분 전환 (2시간 쿨다운) = 추가 ~8회
  - 하루 ~20-25회 → 월 ~400회 → 크레딧 내

### WebSocket 모드
- 이상 감지 시에만 호출, 30분 쿨다운
- 예상: 월 ~100회 이내 (하루 3-4회)

## 실행
```bash
cd /Users/parkgeonwoo/poly/monitor

# REST 폴링 모드 (기본)
python3 snapshot.py

# WebSocket 모드 (권장)
python3 snapshot.py --ws

# 리포트 출력
python3 report.py
```

## 의존성
```bash
# 필수
pip install httpx python-dotenv backports.zoneinfo

# WebSocket 모드 추가
pip install websocket-client
```

## WebSocket 메시지 형식 (참조)

### 구독 요청
```json
{"type": "market", "assets_ids": ["token_id_1", "token_id_2"]}
```

### 가격 변동 이벤트
```json
{
  "event_type": "price_change",
  "asset_id": "...",
  "price": "0.55"
}
```

### 오더북 이벤트
```json
{
  "event_type": "book",
  "asset_id": "...",
  "bids": [{"price": "0.54", "size": "100"}],
  "asks": [{"price": "0.56", "size": "100"}]
}
```

## Forward Test v2: 고해상도 Gap 캡처

### 목표
"Oracle move 후 **3초 딜레이 이후에도** gap >= 4%p가 남아 **체결 가능한가**?"

### 핵심 변경
1. Totals/Spreads: `alternate_*` 마켓으로 **Poly 고정 라인과 동일 라인** 비교
2. 측정: gap_t0, gap_t3s, gap_t10s, gap_t30s
3. 크레딧 절약: 트리거 시에만 event 단위 호출

### 새 테이블
- `move_events_hi_res`: 고해상도 무브 이벤트 (t0~t30s gap 시계열)
- `gap_series_hi_res`: 1초 단위 상세 gap 시계열

### 실행
```bash
# WebSocket 모드로 실행 (Forward Test v2 자동 활성화)
python3 snapshot.py --ws

# 48시간 후 분석
python3 hi_res_analysis.py
```

### 판정 기준
| gap_t+3s >= 4%p 비율 | 판정 |
|---------------------|------|
| >= 30% | A: 유망 |
| 10-30% | C: 약한 신호, 추가 검토 |
| < 10% | B: 전략 불가 |

### Odds API Event 엔드포인트
```python
# 트리거 발생 시 호출
GET /events/{eventId}/odds
  markets=h2h,alternate_totals,alternate_spreads
  bookmakers=pinnacle
```

### 크레딧 계산 (48시간)
- h2h 폴링 (60초): ~2,880
- 트리거 호출 (~20/일): ~40
- **총계: ~2,920** (여유 ~4,470)
