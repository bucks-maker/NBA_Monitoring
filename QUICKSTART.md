# Quick Start Guide

## 빠른 시작

### 1. 가상환경 활성화

```bash
source venv/bin/activate
```

### 2. 데이터 수집 및 리포트 생성 (원스텝)

```bash
python main.py run --handle "gabagool22" --out ./out
```

이 명령어는 자동으로:
- gabagool22의 모든 거래 데이터 수집
- 정규화된 CSV/Parquet 파일 생성
- 상세 분석 리포트 생성

### 3. 또는 단계별로 실행

#### Step 1: 데이터 수집만

```bash
python main.py fetch --handle "gabagool22" --out ./out
```

#### Step 2: 리포트 생성만

```bash
python main.py report --in ./out/gabagool22
```

## 결과 확인

모든 데이터와 리포트는 `./out/gabagool22/` 디렉토리에 저장됩니다:

```
out/gabagool22/
├── resolved_profile.json       # 프로필 정보
├── metadata.json               # 수집 메타데이터
├── trades.csv                  # 거래 데이터 (CSV)
├── trades.parquet              # 거래 데이터 (Parquet)
├── activity.csv                # 활동 데이터
├── positions.csv               # 포지션 데이터
└── report.md                   # 📊 분석 리포트
```

## 리포트 내용

`report.md` 파일에서 확인할 수 있는 내용:

✅ **기본 통계**
- 총 거래 수, 거래 기간
- Buy/Sell 비율
- 총 거래량 (USDC)

✅ **Top 10 Markets**
- 거래 횟수가 가장 많은 마켓
- 각 마켓별 거래량, 평균가

✅ **PnL 분석**
- 총 수익/손실
- 승률 (Win Rate)
- 최고/최악의 거래

✅ **전략 패턴 탐지**
- 스캘핑/모멘텀 거래 패턴
- 델타 뉴트럴/헤징 패턴

## 다른 유저 분석

다른 Polymarket 유저를 분석하려면 핸들만 변경:

```bash
python main.py run --handle "다른핸들" --out ./out
```

또는 프로필 URL 사용:

```bash
python main.py run --profile-url "https://polymarket.com/@핸들?tab=activity" --out ./out
```

## 테스트 실행

```bash
pytest -v
```

## 문제 해결

### Rate Limit 에러
- 자동으로 재시도됩니다. 잠시 기다리세요.
- `--verbose` 옵션으로 상세 로그 확인 가능

### 핸들 검색 실패
- 핸들 스펠링 확인
- 또는 프로필 URL 직접 사용

## 전체 문서

자세한 내용은 [README.md](README.md) 참고