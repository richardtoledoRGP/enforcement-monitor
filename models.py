from dataclasses import dataclass, field
import hashlib


@dataclass
class EnforcementAction:
    source: str            # e.g., "OCC", "CA DFPI"
    title: str             # Title or description of the action
    url: str               # Link to the full action/document
    date: str = ""         # Date of action (ISO format if available)
    institution: str = ""  # Name of the institution (parsed if possible)
    action_type: str = ""  # e.g., "consent_order", "cease_desist", "penalty"
    penalty_amount: float = 0.0
    raw_text: str = ""     # Full text snippet for keyword matching
    summary: str = ""      # Short summary from listing page columns
    ai_overview: str = ""  # LLM-generated overview (populated in Phase 2)
    fingerprint: str = field(default="", init=False)

    def __post_init__(self):
        self.fingerprint = hashlib.md5(
            f"{self.source}|{self.url}".encode()
        ).hexdigest()


@dataclass
class ScrapeResult:
    source_name: str
    actions: list[EnforcementAction]
    success: bool
    error: str = ""
