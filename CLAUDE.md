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

## 아키텍처
```
[Pinnacle Snapshotter] → SQLite ← [Polymarket Snapshotter]
         ↓                              ↓
    [Move Detector] ──trigger──→ [Gap Recorder]
                                       ↓
                              [Bot Trade Checker]
                                       ↓
                                 [Report/Alert]
```

## 데이터 소스
- Pinnacle: The Odds API (api key: 5700da6b9fe3d555aa4dbb4ec2d00a60, 월 500회)
  - 기본 1시간 간격, 트리거 발생 시 15분 간격
- Polymarket: Gamma API (무료, 무제한)
  - 30초 간격 서브폴링 (Pinnacle 사이클 사이에도 계속 수집)
- 봇 거래: Polymarket Activity API (`/activity?user=BOT_ADDRESS`)
  - trigger 발생 후 윈도우 내 봇 진입 여부 확인

## DB: SQLite
- 파일: monitor/data/snapshots.db
- 테이블: pinnacle_snapshots, poly_snapshots, triggers, bot_trades, game_mapping

## 파일 구조
```
monitor/
  CLAUDE.md           - 이 파일
  schema.sql          - DB 스키마
  snapshot.py         - 메인 수집기 (Pinnacle + Poly + 변동감지 + 봇거래 + 갭추적)
  report.py           - 분석 리포트 출력
  data/
    snapshots.db      - SQLite DB
```

## 트리거 임계값 (초기값, 조정 예정)
- |Δline| >= 1.5 점
- |Δimplied_under| >= 6%p
- |Δimplied_over| >= 6%p

## 타임존 스케줄링
- 모든 시간은 US Eastern Time (ET) 기준
- 활성 시간대: ET 10:00 ~ 03:00 (다음날)
- 비활성: ET 03:00 ~ 10:00 (자동 sleep)
- Polymarket slug 날짜도 ET 기준으로 생성 (EDT/EST 자동 처리)

## Odds API 크레딧 관리
- 월 500회, 하루 ~16회
- **전략: ET 활성 시간대만, 1시간 간격, 트리거 시 15분 전환**
  - ET 10:00~03:00 = 17시간, 1시간 간격 = ~17회/일
  - 트리거 시 15분 전환 (2시간 쿨다운) = 추가 ~8회
  - 하루 ~20-25회 → 월 ~400회 → 크레딧 내

## 실행
```bash
cd /Users/parkgeonwoo/poly/monitor
python3 snapshot.py              # 메인 모니터 (ET 시간대 자동 관리)
python3 report.py                # 리포트 출력
```
