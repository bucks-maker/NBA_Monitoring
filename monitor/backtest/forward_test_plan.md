# 전방 테스트 설계: 24-48시간 검증

## 목표
- "Oracle move 후 3초 이내 Poly gap ≥ 4%p가 존재하는가?"를 실시간으로 검증
- 잔여 크레딧: ~7,390 (Odds API $30/월 플랜 기준)

---

## 1. 크레딧 예산표

| 폴링 간격 | 시간당 호출 | 24시간 호출 | 48시간 호출 | 크레딧/호출 | 24h 크레딧 | 48h 크레딧 |
|-----------|-------------|-------------|-------------|-------------|------------|------------|
| 60초      | 60          | 1,440       | 2,880       | 1           | 1,440      | 2,880      |
| 30초      | 120         | 2,880       | 5,760       | 1           | 2,880      | 5,760      |
| 15초      | 240         | 5,760       | 11,520      | 1           | 5,760      | ❌ 초과    |

**권장: 30초 간격 × 48시간 = 5,760 크레딧** (잔여 1,630 크레딧 여유)

---

## 2. 아키텍처

```
[Odds API 폴링 30초]          [Polymarket WebSocket]
        │                              │
        ▼                              ▼
   pinnacle_live                 poly_live
   (sqlite)                      (sqlite)
        │                              │
        └──────────┬───────────────────┘
                   ▼
            [Move Detector]
                   │
                   ▼ (move 감지 즉시)
         ┌────────┴────────┐
         ▼                 ▼
    poly@t0          poly@t0+3s
    poly@t0+10s      poly@t0+30s
         │                 │
         └────────┬────────┘
                  ▼
           [Gap Recorder]
                  │
                  ▼
          move_events_hi_res
          gap_series_hi_res
```

---

## 3. 스키마 추가 (hi-res)

```sql
-- 고해상도 무브 이벤트
CREATE TABLE move_events_hi_res (
    id INTEGER PRIMARY KEY,
    game_key TEXT,
    market_type TEXT,
    move_ts_unix INTEGER,
    oracle_prev_implied REAL,
    oracle_new_implied REAL,
    oracle_delta REAL,
    poly_at_t0 REAL,
    poly_at_t3s REAL,
    poly_at_t10s REAL,
    poly_at_t30s REAL,
    gap_t0 REAL,
    gap_t3s REAL,
    gap_t10s REAL,
    gap_t30s REAL,
    orderbook_depth_t0 REAL,  -- best bid+ask size
    created_at TEXT
);

-- 상세 갭 시계열 (1초 단위)
CREATE TABLE gap_series_hi_res (
    id INTEGER PRIMARY KEY,
    move_event_id INTEGER,
    ts_offset_sec INTEGER,  -- 0, 1, 2, 3, ... 30
    poly_price REAL,
    gap REAL,
    bid REAL,
    ask REAL
);
```

---

## 4. 코드 수정 사항

### snapshot.py 수정
```python
# 기존 --ws 모드에 hi-res 기록 추가
# 트리거 조건: |oracle_delta| >= 4%p

def on_oracle_move(game_key, market_type, prev_implied, new_implied):
    delta = new_implied - prev_implied
    if abs(delta) < 0.04:  # 4%p 미만 무시
        return
    
    # Poly 가격 즉시 캡처
    poly_t0 = get_poly_price_now(game_key, market_type)
    
    # 3초 후, 10초 후, 30초 후 캡처 스케줄
    schedule_capture(game_key, market_type, [3, 10, 30])
    
    # DB 저장
    save_hi_res_event(game_key, market_type, prev_implied, new_implied, poly_t0)
```

### ws_client.py 활용
- 이미 구현된 WebSocket 클라이언트 사용
- `AssetPriceTracker`로 실시간 가격 캐시

---

## 5. 실행 체크리스트

### 사전 준비
- [ ] EC2 인스턴스 확인 (기존 monitor 서비스 실행 중)
- [ ] .env에 ODDS_API_KEY 확인
- [ ] SQLite DB 백업

### 배포
```bash
# 1. 코드 업데이트
cd /Users/parkgeonwoo/poly/monitor
git add -A && git commit -m "Add hi-res forward test" && git push

# 2. EC2에서 서비스 재시작
ssh ec2-user@<IP> "sudo systemctl restart poly-monitor"

# 3. 로그 모니터링
ssh ec2-user@<IP> "journalctl -u poly-monitor -f"
```

### 48시간 후 분석
```sql
-- 1. 이벤트 수
SELECT COUNT(*) FROM move_events_hi_res;

-- 2. gap_t3s 분포
SELECT 
    COUNT(*) as n,
    AVG(gap_t3s) as mean_gap,
    -- median은 별도 계산 필요
FROM move_events_hi_res
WHERE gap_t3s IS NOT NULL;

-- 3. actionable 비율 (gap_t3s >= 4%p)
SELECT 
    COUNT(CASE WHEN gap_t3s >= 0.04 THEN 1 END) * 1.0 / COUNT(*) as actionable_ratio
FROM move_events_hi_res
WHERE gap_t3s IS NOT NULL;
```

---

## 6. 판정 기준 (48시간 후)

| 지표 | 기준 | 판정 |
|------|------|------|
| 이벤트 수 | < 5 | 데이터 부족, 연장 필요 |
| gap_t3s ≥ 4%p 비율 | < 10% | ❌ 전략 불가 |
| gap_t3s ≥ 4%p 비율 | 10-30% | ⚠️ 약한 신호 |
| gap_t3s ≥ 4%p 비율 | > 30% | ✅ 유망 |
| orderbook_depth | < $100 평균 | ⚠️ 유동성 부족 |

---

## 7. 운영 팁

### 로그 확인
```bash
# 실시간 로그
journalctl -u poly-monitor -f

# 에러만
journalctl -u poly-monitor --since "1 hour ago" | grep -i error
```

### WebSocket 재연결
- `ws_client.py`에 자동 재연결 구현됨 (exponential backoff)
- 5회 실패 시 1시간 대기 후 재시도

### DB 용량
```bash
# DB 크기 확인
ls -lh /home/ec2-user/poly-monitor/data/*.db

# 48시간 예상: ~50MB (기존 + hi-res)
```

---

## 8. 요약

| 항목 | 값 |
|------|-----|
| 테스트 기간 | 48시간 |
| Oracle 폴링 | 30초 |
| Poly 데이터 | WebSocket 실시간 |
| 예상 크레딧 | 5,760 |
| 잔여 크레딧 | ~1,630 |
| 트리거 임계값 | ≥4%p implied 변화 |
| 주요 지표 | gap_t3s 분포, actionable 비율 |
