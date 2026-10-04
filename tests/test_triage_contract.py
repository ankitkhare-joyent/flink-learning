"""The agent's structured output: the JSON schema Claude must fill is strict enough to store and act on."""
import pytest
from pydantic import ValidationError

from services.agent.triage import TriageResult


def test_schema_requires_every_field():
    schema = TriageResult.model_json_schema()
    assert set(schema["required"]) == {"severity", "summary", "likely_cause", "evidence", "next_steps", "notify",
                                       "data_freshness"}
    assert schema["properties"]["severity"]["enum"] == ["low", "medium", "high", "critical"]


def test_rejects_unknown_severity():
    with pytest.raises(ValidationError):
        TriageResult(severity="sev1", summary="s", likely_cause="c", evidence=[], next_steps=[], notify=None,
                     data_freshness="f")
