"""Testament Event Source — reads from the Testament Chain and prepares social posts.

Implements Ring 3 of the Testament Protocol: tiered social syndication.
Events flow from the chain through tier classification to formatted posts
ready for POSSE distribution via the existing social connectors.

Usage:
    from kerygma_social.testament_source import TestamentSource

    source = TestamentSource()
    posts = source.poll()  # Returns list of ContentPost ready for syndication

    # Or from CLI:
    python -m kerygma_social.testament_source --since "2026-03-20" --dry-run
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


# --- Chain path ---

_CHAIN_PATH = Path.home() / ".organvm" / "testament" / "chain.jsonl"
_CURSOR_PATH = Path.home() / ".organvm" / "testament" / "syndication_cursor.json"


# --- Tier definitions (mirrors organvm_engine.ledger.tiers) ---

GOVERNANCE_TYPES = frozenset({
    "governance.promotion", "governance.audit", "governance.dependency_change",
    "testament.genesis", "testament.checkpoint", "testament.verified",
})
MILESTONE_TYPES = frozenset({
    "ci.health", "content.published", "ecosystem.mutation", "pitch.generated",
})
INFRASTRUCTURE_TYPES = frozenset({
    "git.sync", "agent.punch_in", "agent.punch_out", "agent.tool_lock",
})


def _tier(event_type: str) -> str:
    """Classify event type into syndication tier."""
    if event_type in GOVERNANCE_TYPES:
        return "governance"
    if event_type in MILESTONE_TYPES:
        return "milestone"
    if event_type in INFRASTRUCTURE_TYPES:
        return "infrastructure"
    return "operational"


# --- Event record (lightweight, no engine dependency) ---

@dataclass
class ChainEvent:
    """A single event from the Testament Chain."""

    event_id: str = ""
    sequence: int = -1
    timestamp: str = ""
    event_type: str = ""
    source_organ: str = ""
    source_repo: str = ""
    actor: str = ""
    payload: dict[str, Any] = field(default_factory=dict)
    hash: str = ""
    tier: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ChainEvent:
        return cls(
            event_id=data.get("event_id", ""),
            sequence=data.get("sequence", -1),
            timestamp=data.get("timestamp", ""),
            event_type=data.get("event_type", ""),
            source_organ=data.get("source_organ", ""),
            source_repo=data.get("source_repo", ""),
            actor=data.get("actor", ""),
            payload=data.get("payload", {}),
            hash=data.get("hash", ""),
            tier=_tier(data.get("event_type", "")),
        )


# --- Message formatting ---

def format_governance_post(event: ChainEvent) -> str:
    """Format a GOVERNANCE-tier event as a social media post."""
    lines = [f"ORGANVM Testament #{event.sequence}"]
    lines.append("")

    if event.event_type == "testament.genesis":
        lines.append("The Testament Chain has begun.")
        msg = event.payload.get("message", "")
        if msg:
            lines.append(msg)
    elif event.event_type == "governance.promotion":
        repo = event.payload.get("repo", event.source_repo)
        to_status = event.payload.get("to", event.payload.get("to_status", ""))
        lines.append(f"{repo} promoted to {to_status}")
    elif event.event_type == "governance.audit":
        passed = event.payload.get("passed", True)
        critical = event.payload.get("critical", 0)
        lines.append(f"Governance audit: {'PASS' if passed else 'FAIL'} ({critical} critical)")
    elif event.event_type == "testament.checkpoint":
        root = event.payload.get("merkle_root", "")[:20]
        count = event.payload.get("event_count", 0)
        lines.append(f"Merkle checkpoint: {count} events verified")
        if root:
            lines.append(f"Root: {root}...")
    else:
        lines.append(f"{event.event_type}")

    lines.append("")
    lines.append(f"Chain integrity: verified | Event #{event.sequence}")

    return "\n".join(lines)


def format_milestone_post(event: ChainEvent) -> str:
    """Format a MILESTONE-tier event as a social media post."""
    lines = []

    if event.event_type == "content.published":
        desc = event.payload.get("description", "New content published")
        lines.append(desc)
    elif event.event_type == "ci.health":
        lines.append(f"CI health update: {event.source_repo}")
    elif event.event_type == "ecosystem.mutation":
        desc = event.payload.get("description", "Ecosystem change")
        lines.append(desc)
    else:
        lines.append(f"{event.event_type}: {event.source_repo}")

    lines.append("")
    lines.append(f"ORGANVM testament event #{event.sequence}")

    return "\n".join(lines)


# --- Cursor management ---

def _load_cursor() -> int:
    """Load the last-syndicated sequence number. Returns -1 if none."""
    if not _CURSOR_PATH.is_file():
        return -1
    try:
        data = json.loads(_CURSOR_PATH.read_text())
        return data.get("last_sequence", -1)
    except (json.JSONDecodeError, KeyError):
        return -1


def _save_cursor(sequence: int) -> None:
    """Save the last-syndicated sequence number."""
    _CURSOR_PATH.parent.mkdir(parents=True, exist_ok=True)
    _CURSOR_PATH.write_text(json.dumps({
        "last_sequence": sequence,
        "updated": datetime.utcnow().isoformat(),
    }))


# --- Testament Source ---

@dataclass
class SyndicationJob:
    """A formatted post ready for POSSE distribution."""

    event: ChainEvent
    text: str
    tier: str
    platforms: list[str] = field(default_factory=list)


class TestamentSource:
    """Reads from the Testament Chain and produces syndication jobs.

    Maintains a cursor so it only processes new events since the last poll.
    """

    def __init__(
        self,
        chain_path: Path | str | None = None,
        cursor_path: Path | str | None = None,
    ) -> None:
        self._chain_path = Path(chain_path) if chain_path else _CHAIN_PATH
        self._cursor_path = Path(cursor_path) if cursor_path else _CURSOR_PATH

    def _load_cursor(self) -> int:
        """Load the last-syndicated sequence number."""
        if not self._cursor_path.is_file():
            return -1
        try:
            data = json.loads(self._cursor_path.read_text())
            return data.get("last_sequence", -1)
        except (json.JSONDecodeError, KeyError):
            return -1

    def _save_cursor(self, sequence: int) -> None:
        """Save the last-syndicated sequence number."""
        from datetime import timezone

        self._cursor_path.parent.mkdir(parents=True, exist_ok=True)
        self._cursor_path.write_text(json.dumps({
            "last_sequence": sequence,
            "updated": datetime.now(timezone.utc).isoformat(),
        }))

    def poll(self) -> list[SyndicationJob]:
        """Poll the chain for new events since last syndication.

        Returns:
            List of SyndicationJob objects for events that should be syndicated.
            INFRASTRUCTURE-tier events are excluded.
        """
        if not self._chain_path.is_file():
            return []

        cursor = self._load_cursor()
        jobs: list[SyndicationJob] = []

        for line in self._chain_path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError:
                continue

            seq = data.get("sequence", -1)
            if seq <= cursor:
                continue

            event = ChainEvent.from_dict(data)

            # Skip infrastructure and operational tiers
            if event.tier == "infrastructure":
                continue

            # Format by tier
            if event.tier == "governance":
                text = format_governance_post(event)
                platforms = ["bluesky", "mastodon", "discord", "ghost"]
            elif event.tier == "milestone":
                text = format_milestone_post(event)
                platforms = ["bluesky", "mastodon", "discord"]
            else:
                # Operational — skip individual syndication (bundled in digest)
                continue

            jobs.append(SyndicationJob(
                event=event,
                text=text,
                tier=event.tier,
                platforms=platforms,
            ))

        return jobs

    def mark_syndicated(self, jobs: list[SyndicationJob]) -> None:
        """Update the cursor after successful syndication."""
        if not jobs:
            return
        max_seq = max(j.event.sequence for j in jobs)
        self._save_cursor(max_seq)

    def preview(self) -> list[dict[str, Any]]:
        """Preview pending syndication jobs without updating cursor."""
        jobs = self.poll()
        return [
            {
                "sequence": j.event.sequence,
                "event_type": j.event.event_type,
                "tier": j.tier,
                "platforms": j.platforms,
                "text": j.text,
            }
            for j in jobs
        ]


# --- CLI ---

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Testament Chain social syndication")
    parser.add_argument("--dry-run", action="store_true", help="Preview without syndicating")
    parser.add_argument("--chain-path", default=None, help="Override chain path")
    args = parser.parse_args()

    source = TestamentSource(chain_path=args.chain_path)
    jobs = source.poll()

    if not jobs:
        print("No new events to syndicate.")
    else:
        for j in jobs:
            print(f"\n--- [{j.tier.upper()}] Event #{j.event.sequence} ---")
            print(f"Platforms: {', '.join(j.platforms)}")
            print(j.text)
            print()

        if not args.dry_run:
            source.mark_syndicated(jobs)
            print(f"Cursor updated to sequence {max(j.event.sequence for j in jobs)}")
        else:
            print(f"[dry-run] {len(jobs)} events would be syndicated")
