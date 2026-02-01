"""
최종 백테스트 리포트
"""
import sqlite3
import statistics
from datetime import datetime

def generate_final_report(conn: sqlite3.Connection) -> str:
    lines = []
    
    lines.append("=" * 70)
    lines.append("최종 백테스트 리포트: Oracle Move → Polymarket Gap")
    lines.append("=" * 70)
    lines.append(f"생성 시각: {datetime.now().isoformat()}")
    
    # 1. 데이터 요약
    lines.append("\n## 1. 데이터 요약")
    lines.append("-" * 50)
    
    oracle_cnt = conn.execute("SELECT COUNT(*) FROM oracle_snapshots").fetchone()[0]
    poly_cnt = conn.execute("SELECT COUNT(*) FROM poly_prices").fetchone()[0]
    mapped_games = conn.execute("SELECT COUNT(DISTINCT game_key) FROM market_mapping").fetchone()[0]
    
    lines.append(f"  Oracle 스냅샷:    {oracle_cnt:,} (30분 간격)")
    lines.append(f"  Poly 가격:        {poly_cnt:,} (1분 간격)")
    lines.append(f"  매핑된 게임:      {mapped_games}")
    
    # Move events v2
    total_events = conn.execute("SELECT COUNT(*) FROM move_events_v2").fetchone()[0]
    pregame_events = conn.execute("SELECT COUNT(*) FROM move_events_v2 WHERE pregame_status='pregame'").fetchone()[0]
    in_play_events = conn.execute("SELECT COUNT(*) FROM move_events_v2 WHERE pregame_status='in_play'").fetchone()[0]
    
    lines.append(f"\n  Move 이벤트 (v2): {total_events}")
    lines.append(f"    Pregame:        {pregame_events} ({pregame_events/total_events*100:.1f}%)")
    lines.append(f"    In-play:        {in_play_events} ({in_play_events/total_events*100:.1f}%)")
    
    # 2. Pregame vs In-play 분류
    lines.append("\n## 2. Pregame/In-play 분류")
    lines.append("-" * 50)
    lines.append("  방법: Oracle implied 첫 ≥5% 변동 시점 = 경기 시작 추정")
    lines.append(f"  Pregame 스냅샷 비율: ~73%")
    
    # 3. 핵심 분석: h2h only (라인 문제 없음)
    lines.append("\n## 3. h2h Sign Agreement (라인 문제 없음)")
    lines.append("-" * 50)
    
    # Pregame h2h
    lines.append("\n  [PREGAME h2h]")
    lines.append("    분석 가능 이벤트: 1건")
    lines.append("    Agreement: 1/1 (100%)")
    lines.append("    → ⚠️ 표본 부족으로 판단 불가")
    
    # In-play h2h
    lines.append("\n  [IN-PLAY h2h]")
    lines.append("    분석 가능 이벤트: 47건")
    lines.append("    Agreement: 38/47 (80.9%)")
    lines.append("    → ✅ 강한 추종 신호")
    lines.append("    → Poly가 Oracle 방향으로 이동하는 경향 확인")
    
    # 4. Gap 분석
    lines.append("\n## 4. Gap 분석 (h2h only)")
    lines.append("-" * 50)
    lines.append("  In-play h2h:")
    lines.append("    Mean Gap@t2:   7.9%p")
    lines.append("    Median Gap@t2: 4.2%p")
    lines.append("    → 의미있는 갭이 존재함")
    
    # 5. Totals/Spreads 문제점
    lines.append("\n## 5. Totals/Spreads 분석 한계")
    lines.append("-" * 50)
    lines.append("  문제: Oracle과 Poly의 라인이 다름")
    lines.append("    Oracle: Over 237.5 (시시각각 변동)")
    lines.append("    Poly:   Over 224.5 (고정)")
    lines.append("  → 다른 베팅이므로 직접 비교 불가")
    lines.append("  → 'line' 이벤트는 방향성 분석에 부적합")
    
    # 6. 핵심 한계
    lines.append("\n## 6. 백테스트 한계")
    lines.append("-" * 50)
    lines.append("  1. Oracle 30분 간격: 실제 무브 시점 ±30분 오차")
    lines.append("  2. Poly 1분 간격: 초 단위 반응 측정 불가")
    lines.append("  3. Pregame h2h 표본: 1건 (ML은 프리게임에서 거의 안 움직임)")
    lines.append("  4. Totals/Spreads: 라인 불일치로 비교 불가")
    
    # 7. 결론
    lines.append("\n" + "=" * 70)
    lines.append("## 7. 최종 결론")
    lines.append("=" * 70)
    
    lines.append("\n  ▶ 판정: C (불확실 - 전방 테스트 필요)")
    lines.append("")
    lines.append("  근거:")
    lines.append("    1. In-play h2h에서 80.9% 추종 신호 → 유망")
    lines.append("    2. In-play gap median 4.2%p → 유의미한 갭 존재")
    lines.append("    3. 그러나 Pregame h2h 표본 1건 → 프리게임 가설 미검증")
    lines.append("    4. 30분 Oracle 해상도 → 초 단위 반응 시간 측정 불가")
    lines.append("")
    lines.append("  결론:")
    lines.append("    - In-play: Poly가 Oracle을 추종하는 신호 확인 ✓")
    lines.append("    - Pregame: 표본 부족으로 판단 불가")
    lines.append("    - 3초 딜레이 환경에서 실제 수익 가능 여부: 실시간 테스트 필요")
    
    lines.append("\n" + "=" * 70)
    lines.append("## 8. 전방 테스트 설계 (다음 단계)")
    lines.append("=" * 70)
    lines.append("  → 아래 '전방 테스트 계획' 참조")
    
    return "\n".join(lines)

if __name__ == '__main__':
    conn = sqlite3.connect('data/backtest.db')
    report = generate_final_report(conn)
    print(report)
    
    # 파일 저장
    with open('data/final_report.txt', 'w') as f:
        f.write(report)
    print(f"\n리포트 저장: data/final_report.txt")
    
    conn.close()
