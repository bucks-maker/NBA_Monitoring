-- Backtest Pipeline DB Schema
-- "Oracle move → Polymarket lag → gap 진입 가능?" 가설 검증용

-- Oracle(Pinnacle) 스냅샷: 5분 간격 히스토리컬
CREATE TABLE IF NOT EXISTS oracle_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_key TEXT NOT NULL,          -- odds_api event ID
    sport TEXT NOT NULL DEFAULT 'basketball_nba',
    market_type TEXT NOT NULL,       -- 'totals', 'spreads', 'h2h'
    ts TEXT NOT NULL,                -- ISO8601 UTC snapshot time
    ts_unix INTEGER NOT NULL,        -- unix timestamp
    line REAL,                       -- point line (totals: 233.5, spreads: -5.5)
    outcome1_name TEXT,              -- 'Over'/'Home'/'Team A'
    outcome2_name TEXT,              -- 'Under'/'Away'/'Team B'
    outcome1_odds REAL,             -- decimal odds
    outcome2_odds REAL,
    outcome1_implied REAL,          -- 1/odds (no-vig rough)
    outcome2_implied REAL,
    bookmaker TEXT NOT NULL DEFAULT 'pinnacle',
    UNIQUE(game_key, market_type, ts, bookmaker)
);

-- Oracle "무브" 이벤트: |Δimplied| >= threshold
CREATE TABLE IF NOT EXISTS move_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_key TEXT NOT NULL,
    market_type TEXT NOT NULL,
    move_ts TEXT NOT NULL,            -- 무브 감지 시점 (ISO8601)
    move_ts_unix INTEGER NOT NULL,
    metric TEXT NOT NULL,             -- 'implied_prob', 'line'
    prev_value REAL,
    new_value REAL,
    delta_value REAL,                 -- new - prev (방향 포함)
    prev_ts TEXT,                     -- 이전 스냅샷 시점
    -- 매핑 정보
    home_team TEXT,
    away_team TEXT,
    commence_time TEXT
);

-- Polymarket 가격 시계열: prices-history (분 단위)
CREATE TABLE IF NOT EXISTS poly_prices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_key TEXT NOT NULL,
    market_type TEXT NOT NULL,       -- 'total', 'spread', 'moneyline'
    token_id TEXT NOT NULL,          -- clobTokenId
    outcome TEXT NOT NULL,           -- 'Over', 'Under', 'Home', 'Away'
    ts_unix INTEGER NOT NULL,
    price REAL NOT NULL,             -- 0~1 probability
    source TEXT DEFAULT 'prices-history',
    UNIQUE(token_id, ts_unix)
);

-- 갭 시계열: 각 move_event 후 시간별 갭 측정
CREATE TABLE IF NOT EXISTS gap_series (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    move_event_id INTEGER NOT NULL REFERENCES move_events(id),
    ts_offset_sec INTEGER NOT NULL,  -- move 시점 기준 오프셋(초)
    oracle_implied REAL,             -- 해당 시점 oracle implied
    poly_price REAL,                 -- 해당 시점 poly price
    gap REAL,                        -- oracle_implied - poly_price
    gap_abs REAL,                    -- |gap|
    UNIQUE(move_event_id, ts_offset_sec)
);

-- 갭 요약: 각 move_event별 최종 지표
CREATE TABLE IF NOT EXISTS gap_summary (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    move_event_id INTEGER NOT NULL UNIQUE REFERENCES move_events(id),
    gap_0m REAL,                     -- t=0 갭 (가장 가까운 Poly 가격)
    gap_5m REAL,                     -- t+5분 갭
    gap_10m REAL,                    -- t+10분 갭
    gap_30m REAL,                    -- t+30분 갭
    gap_60m REAL,                    -- t+60분 갭
    half_life_sec REAL,              -- 갭이 50% 줄어든 시간(초)
    max_gap REAL,                    -- 최대 갭
    max_gap_offset_sec INTEGER,      -- 최대 갭 시점 오프셋
    actionable INTEGER DEFAULT 0,    -- gap_5m >= 4%p 여부
    notes TEXT
);

-- 마켓 매핑: Odds API game ↔ Polymarket market
CREATE TABLE IF NOT EXISTS market_mapping (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_key TEXT NOT NULL,          -- odds_api event ID
    home_team TEXT,
    away_team TEXT,
    commence_time TEXT,
    poly_event_slug TEXT,            -- e.g. "nba-dal-bos-2026-01-15"
    market_type TEXT NOT NULL,       -- 'total', 'spread', 'moneyline'
    poly_market_slug TEXT,           -- specific market slug
    token_id_1 TEXT,                 -- outcome1 clobTokenId
    token_id_2 TEXT,                 -- outcome2 clobTokenId
    outcome1_name TEXT,
    outcome2_name TEXT,
    UNIQUE(game_key, market_type)
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_oracle_game_ts ON oracle_snapshots(game_key, market_type, ts_unix);
CREATE INDEX IF NOT EXISTS idx_move_game ON move_events(game_key, move_ts_unix);
CREATE INDEX IF NOT EXISTS idx_poly_token_ts ON poly_prices(token_id, ts_unix);
CREATE INDEX IF NOT EXISTS idx_gap_event ON gap_series(move_event_id, ts_offset_sec);
