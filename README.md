# Polymarket Account Data Collector & Analyzer

공식 Polymarket API를 사용하여 특정 유저의 거래 데이터를 수집하고 분석하는 Python 도구입니다.

## 주요 기능

- ✅ **공식 API 전용**: Gamma API + Data API만 사용 (스크래핑 없음)
- 📊 **전체 데이터 수집**: Trades, Activity, Positions, Closed Positions
- 🔄 **자동 페이지네이션**: 모든 데이터를 자동으로 수집
- 💾 **다중 포맷 저장**: JSON, JSONL, CSV, Parquet
- 📈 **자동 분석 리포트**: 거래 패턴, PnL, 전략 탐지
- 🛡️ **Rate Limit 대응**: 지수 백오프 재시도 로직 내장

## 설치

### 1. Python 환경 설정 (3.11+ 권장)

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate  # Windows
```

### 2. 의존성 설치

```bash
pip install -r requirements.txt
```

## 사용법

### CLI 명령어

#### 1. 데이터 수집 (`fetch`)

```bash
# 핸들로 수집
python main.py fetch --handle "gabagool22" --out ./out

# 프로필 URL로 수집
python main.py fetch --profile-url "https://polymarket.com/@gabagool22?tab=activity" --out ./out

# Verbose 모드
python main.py fetch --handle "gabagool22" --out ./out --verbose
```

**출력 파일 구조:**
```
out/gabagool22/
├── resolved_profile.json          # 프로필 정보 + proxyWallet
├── metadata.json                  # 수집 메타데이터
├── trades_raw.jsonl               # 원본 거래 데이터
├── activity_raw.jsonl             # 원본 활동 데이터
├── positions_raw.json             # 원본 오픈 포지션
├── closed_positions_raw.jsonl     # 원본 클로즈 포지션
├── trades.csv / trades.parquet    # 정규화된 거래 데이터
├── activity.csv / activity.parquet
├── positions.csv / positions.parquet
└── report.md                      # (report 명령 후 생성)
```

#### 2. 리포트 생성 (`report`)

```bash
# 기본 리포트 생성
python main.py report --in ./out/gabagool22

# 커스텀 출력 경로
python main.py report --in ./out/gabagool22 --output ./my_report.md
```

#### 3. 원스텝 실행 (`run`)

```bash
# 수집 + 리포트를 한 번에
python main.py run --handle "gabagool22" --out ./out
```

### Python API 사용

```python
import asyncio
from pathlib import Path
from polymarket_collector import UserDataCollector, ReportAnalyzer

async def main():
    # 데이터 수집
    collector = UserDataCollector(Path("./out"))
    metadata = await collector.collect_all_data(handle="gabagool22")

    print(f"수집 완료: {metadata['total_trades']} trades")

    # 리포트 생성
    analyzer = ReportAnalyzer(Path("./out/gabagool22"))
    report_path = analyzer.save_report()
    print(f"리포트 저장: {report_path}")

asyncio.run(main())
```

## 데이터 스키마

### Normalized Trades
```python
{
    "timestamp": datetime,
    "transaction_hash": str,
    "condition_id": str,
    "slug": str,              # 시장 이름
    "event_slug": str,        # 이벤트 이름
    "outcome": str,           # 결과 (Yes/No 등)
    "outcome_index": int,
    "side": str,              # "BUY" or "SELL"
    "size": float,            # 거래량
    "price": float,           # 체결가
    "usdc_size": float,       # USDC 거래금액
    "proxy_wallet": str       # 지갑 주소
}
```

### Normalized Positions
```python
{
    "condition_id": str,
    "slug": str,
    "outcome": str,
    "size": float,
    "average_price": float,
    "usdc_value": float,
    "unrealized_pnl": float,  # 오픈 포지션
    "realized_pnl": float,    # 클로즈 포지션
    "is_closed": bool,
    "close_timestamp": datetime
}
```

## 리포트 내용

자동 생성되는 `report.md`에는 다음 내용이 포함됩니다:

### 1. 기본 통계
- 총 거래 수, 활동 수
- 거래 기간 (최초/최종 거래 시각)
- 유니크 마켓/이벤트 수
- Buy/Sell 비율 및 총 거래량

### 2. Top Markets
- 거래 횟수 기준 상위 10개 마켓
- 각 마켓별 총 거래량, 평균 체결가
- Buy/Sell 분포

### 3. PnL 분석
- 총 Realized PnL
- 승률 (Win Rate)
- 평균 수익/손실
- 최고/최악의 거래

### 4. 전략 패턴 탐지

#### 스캘핑/모멘텀 패턴
- 같은 마켓에서 1시간 내 연속 거래 (3회 이상)
- Buy + Sell이 모두 포함된 경우
- 거래 시간, 가격 범위 표시

#### 델타 뉴트럴/헤징 패턴
- 동일 이벤트 내 여러 결과(outcome)에 동시 포지션
- 24시간 내 양방향 거래 감지
- 포지션 크기 분포 표시

## API 엔드포인트

### Gamma API (프로필 검색)
```
Base: https://gamma-api.polymarket.com
GET /public-search?q=<handle>&search_profiles=true
```

### Data API (거래 데이터)
```
Base: https://data-api.polymarket.com
GET /trades?user=<wallet>&limit=<N>&offset=<K>&takerOnly=false
GET /activity?user=<wallet>&limit=<N>&offset=<K>&type=TRADE,SPLIT,MERGE,...
GET /positions?user=<wallet>&limit=<N>&offset=<K>
GET /closed-positions?user=<wallet>&limit=<N>&offset=<K>
```

## Rate Limiting 대응

- **자동 재시도**: 429 응답 시 Retry-After 헤더 기반 대기
- **지수 백오프**: 5회까지 재시도 (1초 → 2초 → 4초...)
- **요청 간 딜레이**: 기본 0.5초 대기
- **중간 저장**: JSONL로 페이지 단위 저장 (중단 시 복구 가능)

## 테스트 실행

```bash
# 전체 테스트
pytest

# 특정 테스트 파일
pytest polymarket_collector/tests/test_collectors.py

# Verbose 모드
pytest -v

# 커버리지 포함
pytest --cov=polymarket_collector
```

## 예제 출력

### 수집 진행 상황
```
🔍 Starting data collection...
📁 Output directory: ./out

INFO - Resolving handle: gabagool22
INFO - Resolved to wallet: 0x1234...
INFO - Fetching trades for user: 0x1234...
INFO - Progress: fetched 1000 trades total
INFO - Progress: fetched 2000 trades total
...
INFO - Fetching activity for user: 0x1234...
INFO - Fetching positions for user: 0x1234...
INFO - Fetching closed positions for user: 0x1234...

✅ Data collection completed!

📊 Summary:
  - Trades: 2,543
  - Activities: 3,102
  - Open Positions: 12
  - Closed Positions: 87

💾 Data saved to: ./out/gabagool22
```

### 리포트 샘플
```markdown
# Polymarket Trading Analysis Report

## Profile: @gabagool22

- **Proxy Wallet**: `0x1234...`
- **Data Collected**: 2024-01-15 10:30:00

## Summary Statistics

- **Total Trades**: 2,543
- **First Trade**: 2023-08-01 14:23:12 UTC
- **Last Trade**: 2024-01-15 09:45:33 UTC
- **Trading Period**: 167 days
- **Unique Markets Traded**: 87
- **Buy/Sell Ratio**: 1.05
- **Total Volume (USDC)**: $45,231.50

## Top Markets by Trade Count

| Market | Trades | Volume | Avg Price |
|--------|--------|--------|-----------|
| trump-wins-2024 | 342 | $8,234.50 | 0.6234 |
| ...

## Profit & Loss Analysis

- **Total Realized PnL**: $3,421.50
- **Win Rate**: 62.5%
- **Average Win**: $89.30
- **Average Loss**: -$52.10

## Detected Trading Patterns

### Potential Scalping/Momentum Trading

Found 12 potential scalping sequences:

1. **trump-wins-2024**
   - Duration: 45.3 minutes
   - Trades: 8 (Buy: 4, Sell: 4)
   - Price Range: 0.0234
   ...
```

## 아키텍처

```
polymarket_collector/
├── api/                 # API 클라이언트
│   ├── base.py          # 공통 재시도 로직
│   ├── gamma_client.py  # 프로필 검색
│   └── data_client.py   # 거래 데이터
├── collectors/          # 데이터 수집기
│   └── user_collector.py
├── models/              # Pydantic 모델
│   ├── api_models.py
│   └── normalized.py
├── reports/             # 분석 및 리포트
│   └── analyzer.py
├── utils/               # 유틸리티
│   └── storage.py       # 데이터 저장
└── tests/               # 단위 테스트
```

## 제한 사항

- ✅ 공식 공개 API만 사용 (인증 불필요)
- ❌ 브라우저 자동화/스크래핑 없음
- ❌ CLOB L2 헤더 기반 API 미사용 (읽기 전용만)
- ⚠️ Rate Limit에 따라 대용량 데이터 수집 시 시간 소요

## 트러블슈팅

### 429 Too Many Requests
```bash
# --verbose로 재시도 로그 확인
python main.py fetch --handle "user" --verbose
```
→ 자동으로 재시도하므로 대기 필요

### 핸들 검색 실패
```
ValueError: Could not resolve handle 'xxx' to wallet address
```
→ 핸들 스펠링 확인 또는 프로필 URL 직접 사용

### 데이터 파일 없음
```
FileNotFoundError: trades.parquet not found
```
→ `fetch` 명령 먼저 실행 필요

## 기여

이슈 및 PR 환영합니다!

## 라이선스

MIT License

## 참고 자료

- [Polymarket Docs](https://docs.polymarket.com/)
- [Gamma API](https://gamma-api.polymarket.com)
- [Data API](https://data-api.polymarket.com)

---

**Made with ❤️ for Polymarket traders**