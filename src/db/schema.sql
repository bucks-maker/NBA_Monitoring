-- Pinnacle-Polymarket NBA Line Monitor DB Schema

-- Game mapping (Pinnacle event <-> Polymarket slug)
CREATE TABLE IF NOT EXISTS game_mapping (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    odds_api_id TEXT NOT NULL,
    home_team TEXT NOT NULL,
    away_team TEXT NOT NULL,
    commence_time TEXT NOT NULL,
    poly_event_slug TEXT,
    poly_event_found INTEGER DEFAULT 0,
    poly_line REAL,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(odds_api_id)
);

-- Pinnacle snapshots (line + price + implied prob)
CREATE TABLE IF NOT EXISTS pinnacle_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,
    snapshot_time TEXT NOT NULL,
    total_line REAL,
    over_price REAL,
    under_price REAL,
    over_implied REAL,
    under_implied REAL,
    UNIQUE(game_id, snapshot_time)
);

-- Polymarket snapshots (price + orderbook)
CREATE TABLE IF NOT EXISTS poly_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,
    poly_market_slug TEXT NOT NULL,
    snapshot_time TEXT NOT NULL,
    total_line REAL,
    over_price REAL,
    under_price REAL,
    over_best_bid REAL,
    over_best_ask REAL,
    under_best_bid REAL,
    under_best_ask REAL,
    market_type TEXT DEFAULT 'total',
    UNIQUE(poly_market_slug, snapshot_time)
);

-- Trigger events (Pinnacle big moves)
CREATE TABLE IF NOT EXISTS triggers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL,
    trigger_time TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    prev_line REAL,
    prev_over_implied REAL,
    prev_under_implied REAL,
    new_line REAL,
    new_over_implied REAL,
    new_under_implied REAL,
    delta_line REAL,
    delta_under_implied REAL,
    poly_over_price REAL,
    poly_under_price REAL,
    poly_gap_under REAL,
    poly_gap_over REAL,
    gap_closed_time TEXT,
    lag_seconds INTEGER,
    bot_entered INTEGER DEFAULT 0,
    bot_entry_time TEXT,
    bot_entry_side TEXT,
    bot_entry_price REAL,
    market_type TEXT DEFAULT 'totals'
);

-- Bot trades monitoring
CREATE TABLE IF NOT EXISTS bot_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_time TEXT NOT NULL,
    game_id TEXT,
    poly_market_slug TEXT,
    condition_id TEXT,
    outcome TEXT,
    side TEXT,
    price REAL,
    size REAL,
    tx_hash TEXT UNIQUE
);

-- Forward Test v2: Hi-Res move events
CREATE TABLE IF NOT EXISTS move_events_hi_res (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_key TEXT NOT NULL,
    market_type TEXT NOT NULL,
    poly_line REAL,
    oracle_line REAL,
    move_ts_unix INTEGER NOT NULL,
    oracle_prev_implied REAL,
    oracle_new_implied REAL,
    oracle_delta REAL,
    poly_t0 REAL,
    poly_t3s REAL,
    poly_t10s REAL,
    poly_t30s REAL,
    gap_t0 REAL,
    gap_t3s REAL,
    gap_t10s REAL,
    gap_t30s REAL,
    depth_t0 REAL,
    spread_t0 REAL,
    trigger_source TEXT,
    outcome_name TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

-- Hi-Res gap series (per-second detail)
CREATE TABLE IF NOT EXISTS gap_series_hi_res (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    move_event_id INTEGER REFERENCES move_events_hi_res(id),
    ts_offset_sec INTEGER,
    poly_price REAL,
    gap REAL,
    bid REAL,
    ask REAL,
    depth REAL
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_pin_game_time ON pinnacle_snapshots(game_id, snapshot_time);
CREATE INDEX IF NOT EXISTS idx_poly_game_time ON poly_snapshots(game_id, snapshot_time);
CREATE INDEX IF NOT EXISTS idx_triggers_game ON triggers(game_id, trigger_time);
CREATE INDEX IF NOT EXISTS idx_bot_trades_time ON bot_trades(trade_time);
CREATE INDEX IF NOT EXISTS idx_hi_res_game ON move_events_hi_res(game_key, move_ts_unix);
CREATE INDEX IF NOT EXISTS idx_gap_series_event ON gap_series_hi_res(move_event_id, ts_offset_sec);
