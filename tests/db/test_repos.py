"""Tests for DB repository modules."""
from __future__ import annotations

from src.db.pinnacle_repo import PinnacleRepo
from src.db.poly_repo import PolyRepo
from src.db.triggers_repo import TriggersRepo
from src.db.bot_trades_repo import BotTradesRepo
from src.db.hi_res_repo import HiResRepo
from src.db.game_mapping_repo import GameMappingRepo


class TestPinnacleRepo:
    def test_insert_and_get_previous(self, mem_conn):
        repo = PinnacleRepo(mem_conn)
        repo.insert_snapshot("g1", "2026-01-01T00:00:00Z", 230.5, 1.95, 1.90, 0.513, 0.526)
        repo.insert_snapshot("g1", "2026-01-01T01:00:00Z", 231.0, 1.88, 1.98, 0.532, 0.505)
        repo.commit()

        prev = repo.get_previous("g1")
        assert prev is not None
        assert prev[0] == 230.5  # total_line

    def test_duplicate_ignored(self, mem_conn):
        repo = PinnacleRepo(mem_conn)
        repo.insert_snapshot("g1", "2026-01-01T00:00:00Z", 230.5, 1.95, 1.90, 0.513, 0.526)
        repo.insert_snapshot("g1", "2026-01-01T00:00:00Z", 999.0, 1.0, 1.0, 0.5, 0.5)
        repo.commit()

        row = mem_conn.execute(
            "SELECT total_line FROM pinnacle_snapshots WHERE game_id = 'g1'"
        ).fetchone()
        assert row[0] == 230.5  # original preserved


class TestPolyRepo:
    def test_insert_and_closest(self, mem_conn):
        repo = PolyRepo(mem_conn)
        repo.insert_snapshot("g1", "slug-230pt5", "2026-01-01T00:00:00Z", 230.5, 0.52, 0.48)
        repo.insert_snapshot("g1", "slug-231pt0", "2026-01-01T00:00:00Z", 231.0, 0.55, 0.45)
        repo.commit()

        closest = repo.get_closest_poly_snap("g1", 230.5)
        assert closest is not None
        assert closest[2] == 230.5


class TestTriggersRepo:
    def test_insert_and_close(self, mem_conn):
        repo = TriggersRepo(mem_conn)
        repo.insert_trigger(
            "g1", "2026-01-01T01:00:00Z", "line_move",
            230.5, 0.5, 0.5, 232.0, 0.48, 0.52,
            1.5, 0.02, 0.49, 0.51, 0.01, -0.01,
        )
        repo.commit()

        open_triggers = repo.get_open_triggers()
        assert len(open_triggers) == 1
        tr_id = open_triggers[0][0]

        repo.update_gap_closed(tr_id, "2026-01-01T01:05:00Z", 300)
        repo.commit()

        open_triggers = repo.get_open_triggers()
        assert len(open_triggers) == 0


class TestBotTradesRepo:
    def test_insert(self, mem_conn):
        repo = BotTradesRepo(mem_conn)
        repo.insert_trade(
            "2026-01-01T00:00:00Z", "g1", "slug-1", "cond1",
            "Over", "BUY", 0.55, 100.0, "0xabc",
        )
        repo.commit()

        count = mem_conn.execute("SELECT COUNT(*) FROM bot_trades").fetchone()[0]
        assert count == 1

    def test_duplicate_tx_hash_ignored(self, mem_conn):
        repo = BotTradesRepo(mem_conn)
        repo.insert_trade(
            "2026-01-01T00:00:00Z", "g1", "slug-1", "cond1",
            "Over", "BUY", 0.55, 100.0, "0xabc",
        )
        repo.insert_trade(
            "2026-01-01T00:01:00Z", "g1", "slug-1", "cond1",
            "Under", "SELL", 0.45, 50.0, "0xabc",
        )
        repo.commit()

        count = mem_conn.execute("SELECT COUNT(*) FROM bot_trades").fetchone()[0]
        assert count == 1


class TestHiResRepo:
    def test_insert_and_update(self, mem_conn):
        repo = HiResRepo(mem_conn)
        event_id = repo.insert_move_event(
            "g1", "totals", 1700000000,
            0.50, 0.55, 0.05, 0.48, 0.07,
            trigger_source="oracle_move",
        )
        assert event_id is not None

        repo.update_capture(event_id, 3, 0.50, 0.05)
        repo.insert_gap_series(event_id, 0, 0.48, 0.07)

        events = repo.load_all_events()
        assert len(events) == 1
        assert events[0]["gap_t0"] == 0.07


class TestGameMappingRepo:
    def test_upsert_and_get(self, mem_conn):
        repo = GameMappingRepo(mem_conn)
        repo.upsert("g1", "Washington Wizards", "Portland Trail Blazers", "2026-01-27T00:00:00Z")
        repo.commit()

        slug = repo.get_slug("g1")
        assert slug == "nba-por-was-2026-01-26"  # ET is behind UTC

    def test_get_all_slugs(self, mem_conn):
        repo = GameMappingRepo(mem_conn)
        repo.upsert("g1", "Boston Celtics", "Miami Heat", "2026-01-27T23:00:00Z")
        repo.commit()

        slugs = repo.get_all_slugs()
        assert len(slugs) == 1
        assert slugs[0][0] == "g1"
