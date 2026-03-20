from dataclasses import dataclass, field
from datetime import datetime, timezone


def _generate_default_run_id() -> str:
    return datetime.now().strftime("%Y%m%d%H%M%S")


def _generate_utc_timestamp() -> str:
    return datetime.now(timezone.utc).replace(tzinfo=None).isoformat()


@dataclass(frozen=True)
class RunState:
    """Encapsulates the runtime context of a job execution."""

    run_id: str = field(default_factory=_generate_default_run_id)
    run_timestamp: str = field(default_factory=_generate_utc_timestamp)
