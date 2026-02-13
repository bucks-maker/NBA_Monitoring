"""
Pregame vs In-play 분류기

방법: Oracle implied probability의 변동성 기반
- 연속 스냅샷간 |Δimplied| > 5%가 처음 나타나는 시점 = 인게임 시작 추정
- 또는 Poly 가격이 극단(>95% or <5%)에 도달하기 3시간 전 = 인게임 시작

보수적 정의:
- pregame: 첫 큰 변동 이전의 모든 스냅샷
- in_play: 첫 큰 변동 이후
"""
import sqlite3
import re
from datetime import datetime, timezone
from typing import Optional, Dict, Tuple

def extract_game_date(poly_slug: str) -> Optional[str]:
    """poly_market_slug에서 날짜 추출 (YYYY-MM-DD)"""
    if not poly_slug:
        return None
    m = re.search(r'(\d{4}-\d{2}-\d{2})', poly_slug)
    return m.group(1) if m else None

def estimate_game_start(
    conn: sqlite3.Connection,
    game_key: str,
    swing_threshold: float = 0.05,  # 5% swing = in-game 시작 신호
) -> Tuple[Optional[int], str]:
    """
    경기 시작 시점 추정 (unix timestamp)
    
    Returns: (start_ts_unix, method)
    - method: 'swing' (변동성 기반) or 'resolution' (종료 역산) or 'slug_date' (날짜 기반)
    """
    # 방법 1: h2h implied 변동으로 감지
    rows = conn.execute("""
        SELECT ts_unix, outcome1_implied
        FROM oracle_snapshots
        WHERE game_key = ? AND market_type = 'h2h'
          AND outcome1_implied IS NOT NULL
        ORDER BY ts_unix
    """, (game_key,)).fetchall()
    
    if len(rows) >= 2:
        prev_imp = rows[0][1]
        for i in range(1, len(rows)):
            ts, imp = rows[i]
            if abs(imp - prev_imp) >= swing_threshold:
                # 첫 큰 변동 = 인게임 시작으로 추정
                # 실제 시작은 이전 스냅샷과 현재 사이 어딘가
                return rows[i-1][0], 'swing'
            prev_imp = imp
    
    # 방법 2: Poly resolution 시점에서 역산 (경기 시간 ~2.5h)
    resolve = conn.execute("""
        SELECT MIN(ts_unix)
        FROM poly_prices
        WHERE game_key = ? AND (price >= 0.99 OR price <= 0.01)
    """, (game_key,)).fetchone()
    
    if resolve and resolve[0]:
        # 종료 시점 - 2.5시간 = 경기 시작 추정
        return resolve[0] - 9000, 'resolution'  # 2.5h = 9000s
    
    # 방법 3: slug 날짜 기반 (ET 저녁 7시 = UTC 00:00)
    slug = conn.execute("""
        SELECT poly_market_slug FROM market_mapping WHERE game_key = ? LIMIT 1
    """, (game_key,)).fetchone()
    
    if slug and slug[0]:
        date_str = extract_game_date(slug[0])
        if date_str:
            # 날짜 + 1일 00:00 UTC = 대략적인 경기 시작
            dt = datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
            # ET 저녁 시작 = UTC 다음날 00:00~03:30
            start_ts = int(dt.timestamp()) + 86400  # +1일
            return start_ts, 'slug_date'
    
    return None, 'unknown'

def classify_snapshot(
    ts_unix: int,
    game_start_ts: Optional[int],
    pregame_buffer: int = 600,  # 경기 시작 10분 전까지를 pregame으로
) -> str:
    """
    스냅샷을 pregame/in_play/unknown으로 분류
    """
    if game_start_ts is None:
        return 'unknown'
    
    if ts_unix < game_start_ts - pregame_buffer:
        return 'pregame'
    else:
        return 'in_play'

def build_game_start_table(conn: sqlite3.Connection) -> Dict[str, Tuple[int, str]]:
    """
    모든 게임의 시작 시점 추정 테이블 생성
    Returns: {game_key: (start_ts_unix, method)}
    """
    games = conn.execute("""
        SELECT DISTINCT game_key FROM oracle_snapshots
    """).fetchall()
    
    result = {}
    for (gk,) in games:
        start_ts, method = estimate_game_start(conn, gk)
        result[gk] = (start_ts, method)
    
    return result

def classify_all_snapshots(conn: sqlite3.Connection) -> Dict[str, Dict]:
    """
    전체 스냅샷을 pregame/in_play로 분류하고 통계 반환
    """
    game_starts = build_game_start_table(conn)
    
    stats = {
        'total_snapshots': 0,
        'pregame': 0,
        'in_play': 0,
        'unknown': 0,
        'by_method': {'swing': 0, 'resolution': 0, 'slug_date': 0, 'unknown': 0},
        'games': len(game_starts),
    }
    
    snapshots = conn.execute("""
        SELECT game_key, ts_unix FROM oracle_snapshots
    """).fetchall()
    
    for gk, ts in snapshots:
        stats['total_snapshots'] += 1
        start_ts, method = game_starts.get(gk, (None, 'unknown'))
        status = classify_snapshot(ts, start_ts)
        stats[status] += 1
        stats['by_method'][method] += 1
    
    return stats, game_starts

if __name__ == '__main__':
    conn = sqlite3.connect('data/backtest.db')
    stats, game_starts = classify_all_snapshots(conn)
    
    print("=== Pregame/In-play 분류 결과 ===")
    print(f"총 스냅샷: {stats['total_snapshots']}")
    print(f"Pregame:   {stats['pregame']} ({stats['pregame']/stats['total_snapshots']:.1%})")
    print(f"In-play:   {stats['in_play']} ({stats['in_play']/stats['total_snapshots']:.1%})")
    print(f"Unknown:   {stats['unknown']}")
    print()
    print("=== 경기 시작 추정 방법별 ===")
    for method, cnt in stats['by_method'].items():
        print(f"  {method}: {cnt}")
    
    # 샘플 출력
    print()
    print("=== 샘플 게임별 경기 시작 추정 ===")
    for i, (gk, (start_ts, method)) in enumerate(list(game_starts.items())[:5]):
        if start_ts:
            dt = datetime.utcfromtimestamp(start_ts)
            print(f"  {gk[:30]}... start={dt} ({method})")
    
    conn.close()
