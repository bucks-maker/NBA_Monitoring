"""
Move Events v2: 제외 최소화 + pregame/in-play 태깅

변경점:
1. totals/spreads 라인 불일치로 제외하지 않음
2. 방향성만 평가 (line down → under 유리)
3. pregame/in_play 태그 추가
4. 다양한 임계값으로 이벤트 생성
"""
import sqlite3
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import sys
sys.path.insert(0, '/Users/parkgeonwoo/poly/monitor/backtest')
from pregame_classifier import build_game_start_table, classify_snapshot

def extract_move_events_v2(
    conn: sqlite3.Connection,
    implied_thresholds: List[float] = [0.04, 0.06, 0.08],  # 4%, 6%, 8%
    line_thresholds: Dict[str, List[float]] = None,
) -> int:
    """
    Move events v2 추출: 제외 최소화
    
    Returns: 생성된 이벤트 수
    """
    if line_thresholds is None:
        line_thresholds = {
            'totals': [1.0, 1.5, 2.0],
            'spreads': [0.5, 1.0, 1.5],
        }
    
    # 기존 테이블 클리어
    conn.execute("DROP TABLE IF EXISTS move_events_v2")
    conn.execute("""
        CREATE TABLE move_events_v2 (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_key TEXT NOT NULL,
            market_type TEXT NOT NULL,
            move_ts TEXT NOT NULL,
            move_ts_unix INTEGER NOT NULL,
            prev_ts_unix INTEGER NOT NULL,
            metric TEXT NOT NULL,         -- 'implied_prob' or 'line'
            threshold REAL NOT NULL,      -- 사용된 임계값
            prev_value REAL,
            new_value REAL,
            delta_value REAL,
            direction TEXT,               -- 'up', 'down'
            pregame_status TEXT,          -- 'pregame', 'in_play', 'unknown'
            game_start_ts INTEGER,
            outcome1_name TEXT,
            outcome2_name TEXT,
            line REAL
        )
    """)
    
    # 경기 시작 시점 테이블 구축
    game_starts = build_game_start_table(conn)
    
    # 모든 게임/마켓 조합 처리
    game_markets = conn.execute("""
        SELECT DISTINCT game_key, market_type
        FROM oracle_snapshots
        ORDER BY game_key, market_type
    """).fetchall()
    
    total_events = 0
    
    for game_key, market_type in game_markets:
        # 해당 게임/마켓의 스냅샷 가져오기
        snapshots = conn.execute("""
            SELECT ts, ts_unix, outcome1_implied, outcome2_implied,
                   outcome1_name, outcome2_name, line, outcome1_odds, outcome2_odds
            FROM oracle_snapshots
            WHERE game_key = ? AND market_type = ?
            ORDER BY ts_unix
        """, (game_key, market_type)).fetchall()
        
        if len(snapshots) < 2:
            continue
        
        game_start_ts, _ = game_starts.get(game_key, (None, 'unknown'))
        
        for i in range(1, len(snapshots)):
            prev = snapshots[i-1]
            curr = snapshots[i]
            
            prev_ts, prev_ts_unix, prev_imp1, prev_imp2, _, _, prev_line, _, _ = prev
            curr_ts, curr_ts_unix, curr_imp1, curr_imp2, o1_name, o2_name, curr_line, _, _ = curr
            
            # pregame/in_play 분류
            status = classify_snapshot(curr_ts_unix, game_start_ts)
            
            # === implied probability 변화 감지 (h2h, totals, spreads 모두) ===
            if prev_imp1 is not None and curr_imp1 is not None:
                # De-vig
                prev_total = (prev_imp1 or 0) + (prev_imp2 or 0)
                curr_total = (curr_imp1 or 0) + (curr_imp2 or 0)
                
                if prev_total > 0 and curr_total > 0:
                    prev_fair = prev_imp1 / prev_total
                    curr_fair = curr_imp1 / curr_total
                    delta_imp = curr_fair - prev_fair
                    
                    for thresh in implied_thresholds:
                        if abs(delta_imp) >= thresh:
                            direction = 'up' if delta_imp > 0 else 'down'
                            conn.execute("""
                                INSERT INTO move_events_v2
                                (game_key, market_type, move_ts, move_ts_unix, prev_ts_unix,
                                 metric, threshold, prev_value, new_value, delta_value,
                                 direction, pregame_status, game_start_ts,
                                 outcome1_name, outcome2_name, line)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                game_key, market_type, curr_ts, curr_ts_unix, prev_ts_unix,
                                'implied_prob', thresh, prev_fair, curr_fair, delta_imp,
                                direction, status, game_start_ts,
                                o1_name, o2_name, curr_line
                            ))
                            total_events += 1
                            break  # 가장 낮은 임계값만 기록 (중복 방지)
            
            # === 라인 변화 감지 (totals, spreads만) ===
            if market_type in line_thresholds and prev_line is not None and curr_line is not None:
                delta_line = curr_line - prev_line
                
                for thresh in line_thresholds[market_type]:
                    if abs(delta_line) >= thresh:
                        direction = 'up' if delta_line > 0 else 'down'
                        conn.execute("""
                            INSERT INTO move_events_v2
                            (game_key, market_type, move_ts, move_ts_unix, prev_ts_unix,
                             metric, threshold, prev_value, new_value, delta_value,
                             direction, pregame_status, game_start_ts,
                             outcome1_name, outcome2_name, line)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            game_key, market_type, curr_ts, curr_ts_unix, prev_ts_unix,
                            'line', thresh, prev_line, curr_line, delta_line,
                            direction, status, game_start_ts,
                            o1_name, o2_name, curr_line
                        ))
                        total_events += 1
                        break  # 가장 낮은 임계값만 기록
    
    conn.commit()
    return total_events

def report_move_events_v2(conn: sqlite3.Connection) -> Dict:
    """Move events v2 통계 리포트"""
    stats = {}
    
    # 전체 이벤트 수
    stats['total'] = conn.execute("SELECT COUNT(*) FROM move_events_v2").fetchone()[0]
    
    # pregame vs in_play
    status_dist = conn.execute("""
        SELECT pregame_status, COUNT(*) 
        FROM move_events_v2 
        GROUP BY pregame_status
    """).fetchall()
    stats['by_status'] = dict(status_dist)
    
    # market_type별
    market_dist = conn.execute("""
        SELECT market_type, pregame_status, COUNT(*)
        FROM move_events_v2
        GROUP BY market_type, pregame_status
    """).fetchall()
    stats['by_market_status'] = {}
    for mt, status, cnt in market_dist:
        key = f"{mt}_{status}"
        stats['by_market_status'][key] = cnt
    
    # metric별
    metric_dist = conn.execute("""
        SELECT metric, pregame_status, COUNT(*)
        FROM move_events_v2
        GROUP BY metric, pregame_status
    """).fetchall()
    stats['by_metric_status'] = {}
    for metric, status, cnt in metric_dist:
        key = f"{metric}_{status}"
        stats['by_metric_status'][key] = cnt
    
    # threshold별 이벤트 수
    thresh_dist = conn.execute("""
        SELECT metric, threshold, pregame_status, COUNT(*)
        FROM move_events_v2
        GROUP BY metric, threshold, pregame_status
        ORDER BY metric, threshold
    """).fetchall()
    stats['by_threshold'] = thresh_dist
    
    return stats

if __name__ == '__main__':
    conn = sqlite3.connect('data/backtest.db')
    
    print("=== Move Events v2 생성 중 ===")
    count = extract_move_events_v2(conn)
    print(f"생성된 이벤트: {count}")
    
    print()
    print("=== Move Events v2 통계 ===")
    stats = report_move_events_v2(conn)
    
    print(f"\n총 이벤트: {stats['total']}")
    
    print("\n[Pregame/In-play 분포]")
    for status, cnt in stats['by_status'].items():
        pct = cnt / stats['total'] * 100
        print(f"  {status:<10} {cnt:>5} ({pct:.1f}%)")
    
    print("\n[Market × Status]")
    for key, cnt in sorted(stats['by_market_status'].items()):
        print(f"  {key:<25} {cnt:>5}")
    
    print("\n[Metric × Status]")
    for key, cnt in sorted(stats['by_metric_status'].items()):
        print(f"  {key:<25} {cnt:>5}")
    
    print("\n[Threshold별 이벤트 수 (pregame만)]")
    print(f"  {'Metric':<15} {'Thresh':>8} {'Count':>8}")
    print(f"  {'-'*15} {'-'*8} {'-'*8}")
    for metric, thresh, status, cnt in stats['by_threshold']:
        if status == 'pregame':
            print(f"  {metric:<15} {thresh:>8.2f} {cnt:>8}")
    
    conn.close()
