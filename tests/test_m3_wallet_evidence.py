from datetime import datetime, timedelta, timezone

from neorome.m3_wallet_evidence import evaluate


AS_OF = "2026-09-26T12:00:00Z"


def observation(at, *, largest="0.20", top10="0.41", outflow="0.00", sold=False):
    return {
        "observed_at": at,
        "source": "helius-fixture",
        "wallet_evidence": {
            "largest_holder_fraction": largest,
            "top_10_holder_fraction": top10,
            "creator_outflow_fraction": outflow,
            "creator_sold": sold,
        },
    }


def benign_observations():
    base = datetime.fromisoformat(AS_OF.replace("Z", "+00:00"))
    return [
        observation((base - timedelta(hours=24)).isoformat().replace("+00:00", "Z")),
        observation((base - timedelta(hours=12)).isoformat().replace("+00:00", "Z")),
        observation(AS_OF),
    ]


def test_insufficient_prior_window_is_unresolved():
    rows = benign_observations()[-2:]
    result = evaluate(rows, as_of=AS_OF)

    assert result["status"] == "unresolved"
    assert result["eligible"] is False
    assert "prior_window_insufficient" in result["reasons"]
    assert result["gates"]["prior_window_sufficient"] is False


def test_concentration_and_creator_flow_red_flags_are_rejected():
    rows = benign_observations()
    rows[1] = observation("2026-09-26T00:00:00Z", largest="0.51", top10="0.72", outflow="0.08", sold=True)
    result = evaluate(rows, as_of=AS_OF)

    assert result["status"] == "rejected"
    assert "wallet_concentration_above_threshold" in result["reasons"]
    assert "creator_flow_above_threshold" in result["reasons"]
    assert "creator_selling_detected" in result["reasons"]


def test_complete_benign_evidence_is_eligible_and_paper_only():
    result = evaluate({"schema_version": "m3-wallet-evidence-v1", "as_of": AS_OF,
                       "observations": benign_observations()})

    assert result["status"] == "eligible"
    assert result["eligible"] is True
    assert result["paper_execution_enabled"] is False
    assert result["reasons"] == []
    assert result["gates"] == {
        "causal_timestamps": True,
        "prior_window_sufficient": True,
        "wallet_concentration_clear": True,
        "creator_flow_clear": True,
    }


def test_future_observation_cannot_qualify():
    rows = benign_observations()
    rows[-1] = observation("2026-09-26T12:00:01Z")
    result = evaluate(rows, as_of=AS_OF)

    assert result["status"] == "rejected"
    assert "observation_after_as_of" in result["reasons"]
    assert result["gates"]["causal_timestamps"] is False


def test_missing_wallet_fields_remain_unresolved():
    rows = benign_observations()
    rows[0]["wallet_evidence"].pop("creator_sold")
    result = evaluate(rows, as_of=AS_OF)

    assert result["status"] == "unresolved"
    assert "creator_sold_missing" in result["reasons"]
