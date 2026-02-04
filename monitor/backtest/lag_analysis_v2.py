"""
Lag/추종성 분석 v2

핵심: Oracle이 움직인 뒤 Poly가 같은 방향으로 따라오는가?

분석 방법:
1. 보수적 테스트: sign agreement (Oracle 방향 = Poly 방향)
2. 방향성 이벤트 스터디: poly_move_during, poly_move_after
3. gap_1m: oracle@t2 - poly@t2+1m
"""
import sqlite3
import statistics
from typing import Dict, List, Tuple, Optional
from datetime import datetime

def get_poly_price_at(
    conn: sqlite3.Connection,
    game_key: str,
    market_type: str,
    target_ts: int,
    oracle_outcome1_name: str,
    window_sec: int = 120,
) -> Optional[float]:
    """
    특정 시점의 Poly 가격 조회 (outcome1에 해당하는 토큰)
    """
    poly_mtype = {
        'totals': 'total',
        'spreads': 'spread', 
        'h2h': 'moneyline',
    }.get(market_type, market_type)
    
    # 마켓 매핑에서 토큰 ID 찾기
    mapping = conn.execute("""
        SELECT token_id_1, token_id_2, outcome1_name, outcome2_name
        FROM market_mapping
        WHERE game_key = ? AND market_type = ?
        LIMIT 1
    """, (game_key, poly_mtype)).fetchone()
    
    if not mapping:
        return None
    
    tid1, tid2, poly_name1, poly_name2 = mapping
    
    # outcome 매칭
    token_id = tid1  # 기본값
    if oracle_outcome1_name:
        oracle_lower = oracle_outcome1_name.lower()
        poly_lower1 = (poly_name1 or "").lower()
        poly_lower2 = (poly_name2 or "").lower()
        
        # Over/Under 매칭
        if oracle_lower == "over":
            token_id = tid1 if "over" in poly_lower1 else tid2
        elif oracle_lower == "under":
            token_id = tid1 if "under" in poly_lower1 else tid2
        # 팀명 매칭
        elif oracle_lower in poly_lower1 or poly_lower1 in oracle_lower:
            token_id = tid1
        elif oracle_lower in poly_lower2 or poly_lower2 in oracle_lower:
            token_id = tid2
        else:
            # 부분 매칭
            parts = oracle_lower.split()
            if any(p in poly_lower1 for p in parts if len(p) > 3):
                token_id = tid1
            elif any(p in poly_lower2 for p in parts if len(p) > 3):
                token_id = tid2
    
    if not token_id:
        return None
    
    # 가격 조회
    row = conn.execute("""
        SELECT price
        FROM poly_prices
        WHERE token_id = ?
          AND ts_unix BETWEEN ? AND ?
        ORDER BY ABS(ts_unix - ?)
        LIMIT 1
    """, (token_id, target_ts - window_sec, target_ts + window_sec, target_ts)).fetchone()
    
    return row[0] if row else None

def analyze_lag_for_event(
    conn: sqlite3.Connection,
    event: Tuple,
) -> Optional[Dict]:
    """
    단일 이벤트에 대한 lag 분석
    
    Returns:
        {
            'oracle_direction': 'up'/'down',
            'poly_t1': float,
            'poly_t2': float,
            'poly_t2_plus_1m': float,
            'poly_t2_plus_30m': float,
            'poly_move_during': float,  # poly(t2) - poly(t1)
            'poly_move_after': float,   # poly(t2+30m) - poly(t2)
            'sign_agreement': bool,     # oracle방향 = poly방향(during)
            'gap_1m': float,            # |oracle(t2) - poly(t2+1m)|
        }
    """
    (evt_id, game_key, market_type, move_ts, move_ts_unix, prev_ts_unix,
     metric, threshold, prev_val, new_val, delta_val, direction,
     pregame_status, game_start_ts, o1_name, o2_name, line) = event
    
    # Poly 가격 조회: t1, t2, t2+1m, t2+30m
    poly_t1 = get_poly_price_at(conn, game_key, market_type, prev_ts_unix, o1_name)
    poly_t2 = get_poly_price_at(conn, game_key, market_type, move_ts_unix, o1_name)
    poly_t2_1m = get_poly_price_at(conn, game_key, market_type, move_ts_unix + 60, o1_name)
    poly_t2_30m = get_poly_price_at(conn, game_key, market_type, move_ts_unix + 1800, o1_name)
    
    if poly_t1 is None or poly_t2 is None:
        return None
    
    result = {
        'event_id': evt_id,
        'game_key': game_key,
        'market_type': market_type,
        'metric': metric,
        'pregame_status': pregame_status,
        'oracle_direction': direction,
        'oracle_delta': delta_val,
        'poly_t1': poly_t1,
        'poly_t2': poly_t2,
        'poly_t2_1m': poly_t2_1m,
        'poly_t2_30m': poly_t2_30m,
    }
    
    # poly move during (t1 → t2)
    poly_move_during = poly_t2 - poly_t1
    result['poly_move_during'] = poly_move_during
    
    # poly move after (t2 → t2+30m)
    if poly_t2_30m is not None:
        result['poly_move_after'] = poly_t2_30m - poly_t2
    else:
        result['poly_move_after'] = None
    
    # Sign agreement: oracle 방향과 poly 방향이 같은가?
    # Oracle up(delta > 0) 이면 outcome1 확률 증가
    # Poly도 outcome1 가격이 올랐으면 동일 방향
    if direction == 'up':
        result['sign_agreement'] = poly_move_during > 0
    else:
        result['sign_agreement'] = poly_move_during < 0
    
    # Gap at t2+1m
    if poly_t2_1m is not None and metric == 'implied_prob':
        result['gap_1m'] = abs(new_val - poly_t2_1m)
    else:
        result['gap_1m'] = None
    
    return result

def run_lag_analysis(conn: sqlite3.Connection) -> Dict:
    """
    전체 lag 분석 실행
    """
    # 모든 이벤트 조회
    events = conn.execute("""
        SELECT id, game_key, market_type, move_ts, move_ts_unix, prev_ts_unix,
               metric, threshold, prev_value, new_value, delta_value, direction,
               pregame_status, game_start_ts, outcome1_name, outcome2_name, line
        FROM move_events_v2
        ORDER BY move_ts_unix
    """).fetchall()
    
    results = {
        'total_events': len(events),
        'analyzed': 0,
        'pregame': {'sign_agree': 0, 'sign_disagree': 0, 'gap_1m_values': [], 'events': []},
        'in_play': {'sign_agree': 0, 'sign_disagree': 0, 'gap_1m_values': [], 'events': []},
        'by_market': {},
    }
    
    for event in events:
        analysis = analyze_lag_for_event(conn, event)
        if analysis is None:
            continue
        
        results['analyzed'] += 1
        status = analysis['pregame_status']
        
        if status not in results:
            results[status] = {'sign_agree': 0, 'sign_disagree': 0, 'gap_1m_values': [], 'events': []}
        
        bucket = results[status]
        bucket['events'].append(analysis)
        
        if analysis['sign_agreement']:
            bucket['sign_agree'] += 1
        else:
            bucket['sign_disagree'] += 1
        
        if analysis['gap_1m'] is not None:
            bucket['gap_1m_values'].append(analysis['gap_1m'])
        
        # Market별 집계
        mtype = analysis['market_type']
        if mtype not in results['by_market']:
            results['by_market'][mtype] = {
                'pregame': {'sign_agree': 0, 'sign_disagree': 0, 'events': []},
                'in_play': {'sign_agree': 0, 'sign_disagree': 0, 'events': []},
            }
        
        if status in results['by_market'][mtype]:
            mbucket = results['by_market'][mtype][status]
            mbucket['events'].append(analysis)
            if analysis['sign_agreement']:
                mbucket['sign_agree'] += 1
            else:
                mbucket['sign_disagree'] += 1
    
    return results

def print_lag_report(results: Dict):
    """Lag 분석 리포트 출력"""
    print("=" * 70)
    print("LAG ANALYSIS REPORT v2: Oracle Move → Poly Following")
    print("=" * 70)
    
    print(f"\n총 이벤트: {results['total_events']}")
    print(f"분석 완료: {results['analyzed']} (Poly 데이터 있는 경우)")
    
    print("\n" + "=" * 50)
    print("1. SIGN AGREEMENT (Oracle 방향 = Poly 방향)")
    print("=" * 50)
    
    for status in ['pregame', 'in_play']:
        if status not in results:
            continue
        bucket = results[status]
        total = bucket['sign_agree'] + bucket['sign_disagree']
        if total == 0:
            continue
        
        agree_pct = bucket['sign_agree'] / total * 100
        print(f"\n[{status.upper()}] (N={total})")
        print(f"  Sign Agreement: {bucket['sign_agree']:>4} ({agree_pct:.1f}%)")
        print(f"  Sign Disagree:  {bucket['sign_disagree']:>4} ({100-agree_pct:.1f}%)")
        
        # 가설: 추종하면 agree > 50%
        if agree_pct > 55:
            verdict = "✅ 추종 신호 있음"
        elif agree_pct < 45:
            verdict = "❌ 역추종 또는 무관"
        else:
            verdict = "⚠️ 불확실 (50% 근처)"
        print(f"  판정: {verdict}")
    
    print("\n" + "=" * 50)
    print("2. MARKET별 PREGAME Sign Agreement")
    print("=" * 50)
    
    for mtype, mdata in results['by_market'].items():
        if 'pregame' not in mdata:
            continue
        bucket = mdata['pregame']
        total = bucket['sign_agree'] + bucket['sign_disagree']
        if total == 0:
            continue
        
        agree_pct = bucket['sign_agree'] / total * 100
        print(f"\n  {mtype}: N={total}, Agreement={agree_pct:.1f}%")
    
    print("\n" + "=" * 50)
    print("3. GAP_1M 분포 (implied_prob 이벤트만)")
    print("=" * 50)
    
    for status in ['pregame', 'in_play']:
        if status not in results:
            continue
        gaps = results[status]['gap_1m_values']
        if not gaps:
            print(f"\n[{status.upper()}] gap_1m 데이터 없음")
            continue
        
        print(f"\n[{status.upper()}] (N={len(gaps)})")
        print(f"  Mean:   {statistics.mean(gaps)*100:.1f}%p")
        print(f"  Median: {statistics.median(gaps)*100:.1f}%p")
        
        # >= threshold 비율
        for thresh in [0.03, 0.04, 0.05]:
            cnt = sum(1 for g in gaps if g >= thresh)
            print(f"  gap_1m >= {thresh*100:.0f}%p: {cnt}/{len(gaps)} ({cnt/len(gaps)*100:.1f}%)")
    
    print("\n" + "=" * 50)
    print("4. POLY MOVE ANALYSIS (방향성 상세)")
    print("=" * 50)
    
    for status in ['pregame', 'in_play']:
        if status not in results or 'events' not in results[status]:
            continue
        events = results[status]['events']
        if not events:
            continue
        
        # poly_move_during 분석
        during_toward_oracle = 0
        during_away_oracle = 0
        during_zero = 0
        
        for e in events:
            pm = e['poly_move_during']
            oracle_dir = e['oracle_direction']
            
            if abs(pm) < 0.001:  # 거의 안 움직임
                during_zero += 1
            elif (oracle_dir == 'up' and pm > 0) or (oracle_dir == 'down' and pm < 0):
                during_toward_oracle += 1
            else:
                during_away_oracle += 1
        
        total = len(events)
        print(f"\n[{status.upper()}] Poly Move During [t1→t2] (N={total})")
        print(f"  Oracle 방향으로: {during_toward_oracle} ({during_toward_oracle/total*100:.1f}%)")
        print(f"  Oracle 반대로:   {during_away_oracle} ({during_away_oracle/total*100:.1f}%)")
        print(f"  거의 안 움직임:  {during_zero} ({during_zero/total*100:.1f}%)")
        
        # poly_move_after 분석 (t2 → t2+30m)
        after_events = [e for e in events if e['poly_move_after'] is not None]
        if after_events:
            after_toward = 0
            after_away = 0
            after_zero = 0
            
            for e in after_events:
                pm = e['poly_move_after']
                # Oracle@t2 implied 기준, Poly가 그 방향으로 움직였는지
                oracle_dir = e['oracle_direction']
                
                if abs(pm) < 0.001:
                    after_zero += 1
                elif (oracle_dir == 'up' and pm > 0) or (oracle_dir == 'down' and pm < 0):
                    after_toward += 1
                else:
                    after_away += 1
            
            total_after = len(after_events)
            print(f"\n  Poly Move After [t2→t2+30m] (N={total_after})")
            print(f"    Oracle 방향으로: {after_toward} ({after_toward/total_after*100:.1f}%)")
            print(f"    Oracle 반대로:   {after_away} ({after_away/total_after*100:.1f}%)")
            print(f"    거의 안 움직임:  {after_zero} ({after_zero/total_after*100:.1f}%)")

if __name__ == '__main__':
    conn = sqlite3.connect('data/backtest.db')
    
    print("Lag 분석 실행 중...")
    results = run_lag_analysis(conn)
    print_lag_report(results)
    
    conn.close()
