"""Tests for the Testament Chain social syndication source."""

from __future__ import annotations

import json
from pathlib import Path

from kerygma_social.testament_source import (
    ChainEvent,
    SyndicationJob,
    TestamentSource,
    format_governance_post,
    format_milestone_post,
)


def _write_chain(path: Path, events: list[dict]) -> None:
    """Write a minimal chain file for testing."""
    with path.open("w") as f:
        for e in events:
            f.write(json.dumps(e, separators=(",", ":")) + "\n")


class TestChainEvent:

    def test_from_dict(self):
        event = ChainEvent.from_dict({
            "event_id": "test-1",
            "sequence": 0,
            "event_type": "governance.promotion",
            "source_organ": "META-ORGANVM",
            "hash": "sha256:abc",
        })
        assert event.event_id == "test-1"
        assert event.tier == "governance"

    def test_tier_classification(self):
        assert ChainEvent.from_dict({"event_type": "governance.promotion"}).tier == "governance"
        assert ChainEvent.from_dict({"event_type": "ci.health"}).tier == "milestone"
        assert ChainEvent.from_dict({"event_type": "registry.update"}).tier == "operational"
        assert ChainEvent.from_dict({"event_type": "git.sync"}).tier == "infrastructure"
        assert ChainEvent.from_dict({"event_type": "unknown.type"}).tier == "operational"


class TestFormatting:

    def test_governance_genesis(self):
        event = ChainEvent(
            sequence=0,
            event_type="testament.genesis",
            payload={"message": "Chain begins."},
        )
        text = format_governance_post(event)
        assert "Testament #0" in text
        assert "Chain begins." in text

    def test_governance_promotion(self):
        event = ChainEvent(
            sequence=5,
            event_type="governance.promotion",
            payload={"repo": "test-repo", "to": "GRADUATED"},
        )
        text = format_governance_post(event)
        assert "test-repo" in text
        assert "GRADUATED" in text

    def test_governance_audit(self):
        event = ChainEvent(
            sequence=10,
            event_type="governance.audit",
            payload={"passed": True, "critical": 0},
        )
        text = format_governance_post(event)
        assert "PASS" in text

    def test_governance_checkpoint(self):
        event = ChainEvent(
            sequence=20,
            event_type="testament.checkpoint",
            payload={"merkle_root": "sha256:abc123def456", "event_count": 19},
        )
        text = format_governance_post(event)
        assert "19 events" in text
        assert "Merkle" in text

    def test_milestone_content_published(self):
        event = ChainEvent(
            sequence=3,
            event_type="content.published",
            payload={"description": "New essay on distributed systems"},
        )
        text = format_milestone_post(event)
        assert "New essay" in text
        assert "#3" in text


class TestTestamentSource:

    def test_poll_empty_chain(self, tmp_path):
        source = TestamentSource(chain_path=tmp_path / "nope.jsonl")
        jobs = source.poll()
        assert jobs == []

    def test_poll_returns_governance_events(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        _write_chain(chain_path, [
            {"event_id": "1", "sequence": 0, "event_type": "testament.genesis",
             "payload": {"message": "Begin."}, "hash": "sha256:a"},
        ])
        source = TestamentSource(
            chain_path=chain_path,
            cursor_path=tmp_path / "cursor.json",
        )
        jobs = source.poll()
        assert len(jobs) == 1
        assert jobs[0].tier == "governance"
        assert "bluesky" in jobs[0].platforms

    def test_poll_skips_infrastructure(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        _write_chain(chain_path, [
            {"event_id": "1", "sequence": 0, "event_type": "git.sync",
             "hash": "sha256:a"},
        ])
        source = TestamentSource(
            chain_path=chain_path,
            cursor_path=tmp_path / "cursor.json",
        )
        jobs = source.poll()
        assert len(jobs) == 0

    def test_poll_skips_operational(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        _write_chain(chain_path, [
            {"event_id": "1", "sequence": 0, "event_type": "registry.update",
             "hash": "sha256:a"},
        ])
        source = TestamentSource(
            chain_path=chain_path,
            cursor_path=tmp_path / "cursor.json",
        )
        jobs = source.poll()
        assert len(jobs) == 0

    def test_cursor_tracks_syndication(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        cursor_path = tmp_path / "cursor.json"
        _write_chain(chain_path, [
            {"event_id": "1", "sequence": 0, "event_type": "testament.genesis",
             "payload": {}, "hash": "sha256:a"},
            {"event_id": "2", "sequence": 1, "event_type": "governance.promotion",
             "payload": {"repo": "x", "to": "G"}, "hash": "sha256:b"},
        ])
        source = TestamentSource(chain_path=chain_path, cursor_path=cursor_path)

        # First poll gets both events
        jobs = source.poll()
        assert len(jobs) == 2

        # Mark syndicated
        source.mark_syndicated(jobs)

        # Second poll gets nothing (cursor advanced)
        jobs2 = source.poll()
        assert len(jobs2) == 0

    def test_cursor_survives_new_instance(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        cursor_path = tmp_path / "cursor.json"
        _write_chain(chain_path, [
            {"event_id": "1", "sequence": 0, "event_type": "testament.genesis",
             "payload": {}, "hash": "sha256:a"},
        ])

        source1 = TestamentSource(chain_path=chain_path, cursor_path=cursor_path)
        jobs = source1.poll()
        source1.mark_syndicated(jobs)

        # New instance, same cursor file
        source2 = TestamentSource(chain_path=chain_path, cursor_path=cursor_path)
        jobs2 = source2.poll()
        assert len(jobs2) == 0

    def test_preview_does_not_advance_cursor(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        cursor_path = tmp_path / "cursor.json"
        _write_chain(chain_path, [
            {"event_id": "1", "sequence": 0, "event_type": "testament.genesis",
             "payload": {}, "hash": "sha256:a"},
        ])
        source = TestamentSource(chain_path=chain_path, cursor_path=cursor_path)

        preview = source.preview()
        assert len(preview) == 1

        # Cursor should not have advanced
        jobs = source.poll()
        assert len(jobs) == 1

    def test_milestone_platforms_are_subset(self, tmp_path):
        chain_path = tmp_path / "chain.jsonl"
        _write_chain(chain_path, [
            {"event_id": "1", "sequence": 0, "event_type": "content.published",
             "payload": {"description": "test"}, "hash": "sha256:a"},
        ])
        source = TestamentSource(
            chain_path=chain_path,
            cursor_path=tmp_path / "cursor.json",
        )
        jobs = source.poll()
        assert len(jobs) == 1
        assert "ghost" not in jobs[0].platforms  # Milestones don't go to Ghost
        assert "bluesky" in jobs[0].platforms
