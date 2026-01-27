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
