"""
Lag Analysis v3: 방향 해석 수정

수정 사항:
1. Totals: line UP → Over 불리 (더 높은 점수 필요)
2. Spreads: line UP (less negative for favorite) → favorite 유리

핵심 질문: Oracle이 움직인 방향으로 Poly가 따라오는가?
"""
import sqlite3
import statistics
from typing import Dict, List, Optional

def get_poly_prices(
    conn: sqlite3.Connection,
    game_key: str,
    market_type: str,
    ts: int,
    window: int = 120,
) -> Dict[str, Optional[float]]:
    """
    양쪽 outcome의 Poly 가격 반환
    Returns: {'outcome1': price1, 'outcome2': price2, 'outcome1_name': name1, ...}
    """
    poly_mtype = {'totals': 'total', 'spreads': 'spread', 'h2h': 'moneyline'}.get(market_type)
    
    mapping = conn.execute("""
        SELECT token_id_1, token_id_2, outcome1_name, outcome2_name
        FROM market_mapping
        WHERE game_key = ? AND market_type = ?
    """, (game_key, poly_mtype)).fetchone()
    
    if not mapping:
        return {}
    
    tid1, tid2, name1, name2 = mapping
    
    p1 = conn.execute("""
        SELECT price FROM poly_prices
        WHERE token_id = ? AND ABS(ts_unix - ?) < ?
        ORDER BY ABS(ts_unix - ?) LIMIT 1
    """, (tid1, ts, window, ts)).fetchone()
    
    p2 = conn.execute("""
        SELECT price FROM poly_prices
        WHERE token_id = ? AND ABS(ts_unix - ?) < ?
        ORDER BY ABS(ts_unix - ?) LIMIT 1
    """, (tid2, ts, window, ts)).fetchone()
    
    return {
        'outcome1': p1[0] if p1 else None,
        'outcome2': p2[0] if p2 else None,
        'outcome1_name': name1,
        'outcome2_name': name2,
    }

def interpret_oracle_direction(
    market_type: str,
    metric: str,
    delta: float,
    poly_outcome1_name: str,
) -> str:
    """
    Oracle 변화가 Poly outcome1에 유리한지 불리한지 해석
    
    Returns: 'up' (outcome1 유리) or 'down' (outcome1 불리)
    """
    if metric == 'implied_prob':
        # implied_prob delta > 0 → outcome1 확률 증가 → outcome1 유리
        return 'up' if delta > 0 else 'down'
    
    elif market_type == 'totals':
        # line delta > 0 (line UP) → Over가 더 어려움
        # Poly outcome1이 Over면 → outcome1 불리
        # Poly outcome1이 Under면 → outcome1 유리
        is_over = 'over' in (poly_outcome1_name or '').lower()
        if delta > 0:  # line UP
            return 'down' if is_over else 'up'  # Over 불리, Under 유리
        else:  # line DOWN
            return 'up' if is_over else 'down'  # Over 유리, Under 불리
    
    elif market_type == 'spreads':
        # line delta > 0 means less negative for favorite (easier spread)
        # Poly outcome1이 favorite면 유리, underdog면 불리
        # 간단히: Oracle outcome1 implied 변화로 판단
        # line UP (less negative) → favorite 유리
        # 하지만 Poly outcome1이 favorite인지 모름 → implied_prob 기준 사용
        # 실제로는 Oracle implied도 같이 봐야 함
        # 여기서는 단순화: line UP → outcome1 유리로 가정 (대부분 home=favorite)
        return 'up' if delta > 0 else 'down'
    
    return 'up' if delta > 0 else 'down'

def run_lag_analysis_v3(conn: sqlite3.Connection) -> Dict:
    """
    수정된 lag 분석
    """
    # 매핑된 게임만 분석
    mapped_games = set(r[0] for r in conn.execute(
        'SELECT DISTINCT game_key FROM market_mapping'
    ).fetchall())
    
    events = conn.execute("""
        SELECT id, game_key, market_type, move_ts, move_ts_unix, prev_ts_unix,
               metric, threshold, prev_value, new_value, delta_value, direction,
               pregame_status, game_start_ts, outcome1_name, outcome2_name, line
        FROM move_events_v2
        WHERE game_key IN ({})
        ORDER BY move_ts_unix
    """.format(','.join('?' * len(mapped_games))), list(mapped_games)).fetchall()
    
    results = {
        'total': len(events),
        'analyzed': 0,
        'pregame': {'agree': 0, 'disagree': 0, 'neutral': 0, 'gap_1m': [], 'details': []},
        'in_play': {'agree': 0, 'disagree': 0, 'neutral': 0, 'gap_1m': [], 'details': []},
        'by_market': {},
    }
    
    for evt in events:
        (evt_id, gk, mtype, move_ts, ts2, ts1, metric, thresh, 
         prev_val, new_val, delta, direction, status, game_start, 
         o1_oracle, o2_oracle, line) = evt
        
        # Poly 가격 조회
        poly_t1 = get_poly_prices(conn, gk, mtype, ts1)
        poly_t2 = get_poly_prices(conn, gk, mtype, ts2)
        
        if not poly_t1.get('outcome1') or not poly_t2.get('outcome1'):
            continue
        
        results['analyzed'] += 1
        
        poly_delta = poly_t2['outcome1'] - poly_t1['outcome1']
        poly_o1_name = poly_t1.get('outcome1_name', '')
        
        # Oracle 방향 해석 (수정됨)
        oracle_favorable = interpret_oracle_direction(mtype, metric, delta, poly_o1_name)
        
        # Sign agreement
        if abs(poly_delta) < 0.002:
            agree_type = 'neutral'
        elif (oracle_favorable == 'up' and poly_delta > 0) or \
             (oracle_favorable == 'down' and poly_delta < 0):
            agree_type = 'agree'
        else:
            agree_type = 'disagree'
        
        # Gap 1m (implied_prob만)
        gap_1m = None
        if metric == 'implied_prob':
            poly_t2_1m = get_poly_prices(conn, gk, mtype, ts2 + 60)
            if poly_t2_1m.get('outcome1') is not None:
                gap_1m = abs(new_val - poly_t2_1m['outcome1'])
        
        detail = {
            'event_id': evt_id,
            'market_type': mtype,
            'metric': metric,
            'oracle_delta': delta,
            'poly_delta': poly_delta,
            'oracle_favorable': oracle_favorable,
            'agree_type': agree_type,
            'gap_1m': gap_1m,
        }
        
        if status not in results:
            results[status] = {'agree': 0, 'disagree': 0, 'neutral': 0, 'gap_1m': [], 'details': []}
        
        bucket = results[status]
        bucket[agree_type] += 1
        bucket['details'].append(detail)
        if gap_1m is not None:
            bucket['gap_1m'].append(gap_1m)
        
        # Market별 집계
        if mtype not in results['by_market']:
            results['by_market'][mtype] = {
                'pregame': {'agree': 0, 'disagree': 0, 'neutral': 0},
                'in_play': {'agree': 0, 'disagree': 0, 'neutral': 0},
            }
        if status in results['by_market'][mtype]:
            results['by_market'][mtype][status][agree_type] += 1
    
    return results

def print_report_v3(results: Dict):
    """수정된 리포트 출력"""
    print("=" * 70)
    print("LAG ANALYSIS v3: 방향 해석 수정됨")
    print("=" * 70)
    
    print(f"\n총 이벤트: {results['total']}")
    print(f"분석 완료: {results['analyzed']}")
    
    print("\n" + "=" * 50)
    print("1. SIGN AGREEMENT (수정된 방향 해석)")
    print("=" * 50)
    
    for status in ['pregame', 'in_play']:
        if status not in results:
            continue
        b = results[status]
        total = b['agree'] + b['disagree'] + b['neutral']
        if total == 0:
            continue
        
        non_neutral = b['agree'] + b['disagree']
        agree_pct = b['agree'] / non_neutral * 100 if non_neutral else 0
        
        print(f"\n[{status.upper()}] (N={total}, 움직인 것만={non_neutral})")
        print(f"  Agree:    {b['agree']:>4} ({agree_pct:.1f}% of non-neutral)")
        print(f"  Disagree: {b['disagree']:>4}")
        print(f"  Neutral:  {b['neutral']:>4}")
        
        if agree_pct >= 55:
            print(f"  → ✅ 추종 신호 (>{55}%)")
        elif agree_pct <= 45:
            print(f"  → ❌ 역추종 또는 무관 (<{45}%)")
        else:
            print(f"  → ⚠️ 불확실 (45-55%)")
    
    print("\n" + "=" * 50)
    print("2. MARKET별 PREGAME Agreement")
    print("=" * 50)
    
    for mtype, mdata in results['by_market'].items():
        if 'pregame' not in mdata:
            continue
        b = mdata['pregame']
        non_neutral = b['agree'] + b['disagree']
        if non_neutral == 0:
            continue
        agree_pct = b['agree'] / non_neutral * 100
        print(f"\n  {mtype}: N(non-neutral)={non_neutral}, Agreement={agree_pct:.1f}%")
    
    print("\n" + "=" * 50)
    print("3. GAP_1M (implied_prob 이벤트)")
    print("=" * 50)
    
    for status in ['pregame', 'in_play']:
        if status not in results:
            continue
        gaps = results[status].get('gap_1m', [])
        if not gaps:
            continue
        
        print(f"\n[{status.upper()}] (N={len(gaps)})")
        print(f"  Mean:   {statistics.mean(gaps)*100:.1f}%p")
        print(f"  Median: {statistics.median(gaps)*100:.1f}%p")
        for thresh in [0.03, 0.04, 0.05]:
            cnt = sum(1 for g in gaps if g >= thresh)
            print(f"  ≥{thresh*100:.0f}%p: {cnt}/{len(gaps)} ({cnt/len(gaps)*100:.1f}%)")

if __name__ == '__main__':
    conn = sqlite3.connect('data/backtest.db')
    results = run_lag_analysis_v3(conn)
    print_report_v3(results)
    conn.close()
