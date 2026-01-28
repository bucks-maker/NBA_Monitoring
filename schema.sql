-- Pinnacle-Polymarket NBA Line Monitor DB Schema

-- 경기 매핑 (Pinnacle event ↔ Polymarket slug)
CREATE TABLE IF NOT EXISTS game_mapping (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    odds_api_id TEXT NOT NULL,          -- The Odds API game ID
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    commence_time TEXT NOT NULL,         -- ISO8601 UTC
    poly_event_slug TEXT,               -- e.g. "nba-por-was-2026-01-27"
    poly_event_found INTEGER DEFAULT 0, -- 1 if poly event exists
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(odds_api_id)
);

-- Pinnacle 스냅샷 (라인 + 가격 + implied prob)
CREATE TABLE IF NOT EXISTS pinnacle_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,              -- odds_api_id
    snapshot_time TEXT NOT NULL,         -- ISO8601 UTC
    total_line REAL,                    -- e.g. 233.5
    over_price REAL,                    -- decimal odds e.g. 2.01
    under_price REAL,                   -- decimal odds e.g. 1.85
    over_implied REAL,                  -- 1/over_price (no-vig rough)
    under_implied REAL,                 -- 1/under_price
    UNIQUE(game_id, snapshot_time)
);

-- Polymarket 스냅샷 (가격 + 오더북)
CREATE TABLE IF NOT EXISTS poly_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,              -- odds_api_id (매핑 기준)
    poly_market_slug TEXT NOT NULL,     -- e.g. "nba-por-was-2026-01-27-total-233pt5"
    snapshot_time TEXT NOT NULL,
    total_line REAL,                    -- e.g. 233.5
    over_price REAL,                    -- outcome price 0~1
    under_price REAL,
    -- 오더북 (CLOB best bid/ask)
    over_best_bid REAL,
    over_best_ask REAL,
    under_best_bid REAL,
    under_best_ask REAL,
    UNIQUE(poly_market_slug, snapshot_time)
);

-- 트리거 이벤트 (Pinnacle 큰 변동 감지)
CREATE TABLE IF NOT EXISTS triggers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,
    trigger_time TEXT NOT NULL,
    trigger_type TEXT NOT NULL,         -- 'line_move', 'implied_move', 'both'
    -- 변동 전
    prev_line REAL,
    prev_over_implied REAL,
    prev_under_implied REAL,
    -- 변동 후
    new_line REAL,
    new_over_implied REAL,
    new_under_implied REAL,
    -- 변동 크기
    delta_line REAL,                   -- new - prev (양수=라인 상승)
    delta_under_implied REAL,          -- new - prev
    -- 트리거 시점 Polymarket 상태
    poly_over_price REAL,
    poly_under_price REAL,
    poly_gap_under REAL,               -- pinnacle_under_implied - poly_under_price
    poly_gap_over REAL,                -- pinnacle_over_implied - poly_over_price
    -- 후속 추적
    gap_closed_time TEXT,              -- 갭이 1%p 이내로 수렴한 시각
    lag_seconds INTEGER,               -- trigger_time → gap_closed_time
    bot_entered INTEGER DEFAULT 0,     -- 봇이 이 윈도우에 진입했는지
    bot_entry_time TEXT,
    bot_entry_side TEXT,               -- 'Over' or 'Under'
    bot_entry_price REAL
);

-- 봇 거래 (0x6e82b93e 실시간 모니터링)
CREATE TABLE IF NOT EXISTS bot_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_time TEXT NOT NULL,
    game_id TEXT,                       -- 매핑된 odds_api_id
    poly_market_slug TEXT,
    condition_id TEXT,
    outcome TEXT,                       -- 'Over', 'Under', team name
    side TEXT,                          -- 'BUY', 'SELL'
    price REAL,
    size REAL,
    tx_hash TEXT UNIQUE
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_pin_game_time ON pinnacle_snapshots(game_id, snapshot_time);
CREATE INDEX IF NOT EXISTS idx_poly_game_time ON poly_snapshots(game_id, snapshot_time);
CREATE INDEX IF NOT EXISTS idx_triggers_game ON triggers(game_id, trigger_time);
CREATE INDEX IF NOT EXISTS idx_bot_trades_time ON bot_trades(trade_time);

-- ===============================================================
-- Forward Test v2: 고해상도 이벤트 캡처 (gap_t+3s 측정)
-- ===============================================================

-- 고해상도 무브 이벤트 (Oracle move 또는 Poly anomaly 트리거 시 기록)
CREATE TABLE IF NOT EXISTS move_events_hi_res (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_key TEXT NOT NULL,               -- game_id (odds_api_id)
    market_type TEXT NOT NULL,            -- 'h2h', 'totals', 'spreads'
    poly_line REAL,                       -- Poly 고정 라인 (totals/spreads용)
    oracle_line REAL,                     -- Oracle 매칭된 라인
    move_ts_unix INTEGER NOT NULL,

    -- Oracle implied (de-vigged fair probability)
    oracle_prev_implied REAL,
    oracle_new_implied REAL,
    oracle_delta REAL,

    -- Poly 가격 시계열
    poly_t0 REAL,
    poly_t3s REAL,
    poly_t10s REAL,
    poly_t30s REAL,

    -- Gap 시계열 (|oracle_implied - poly_price|)
    gap_t0 REAL,
    gap_t3s REAL,
    gap_t10s REAL,
    gap_t30s REAL,

    -- Orderbook (체결 가능성)
    depth_t0 REAL,                        -- best bid+ask size in $
    spread_t0 REAL,                       -- bid-ask spread

    -- 메타
    trigger_source TEXT,                  -- 'oracle_move' or 'poly_anomaly'
    outcome_name TEXT,                    -- 'Over', 'Under', 'Home', 'Away'
    created_at TEXT DEFAULT (datetime('now'))
);

-- 1초 단위 gap 시계열 (상세 분석용)
CREATE TABLE IF NOT EXISTS gap_series_hi_res (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    move_event_id INTEGER REFERENCES move_events_hi_res(id),
    ts_offset_sec INTEGER,                -- 0, 1, 2, 3, ..., 30
    poly_price REAL,
    gap REAL,
    bid REAL,
    ask REAL,
    depth REAL
);

-- 마켓 매핑에 poly_line 추가 (Poly 고정 라인 저장)
-- NOTE: 실제 스키마 마이그레이션은 snapshot.py init_db()에서 수행

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_hi_res_game ON move_events_hi_res(game_key, move_ts_unix);
CREATE INDEX IF NOT EXISTS idx_gap_series_event ON gap_series_hi_res(move_event_id, ts_offset_sec);
