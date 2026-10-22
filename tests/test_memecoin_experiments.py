from neorome.memecoin_experiments import EXPERIMENT_VERSION, assess


def record(**overrides):
    value = {
        "eligible_for_internal_paper": True,
        "pool": {"volume_h1_usd": "5000", "volume_previous_h1_usd": "900", "return_h1_fraction": "0.2"},
        "social": {"response_sha256": "a" * 64},
        "entry_quote": {"quote": {"priceImpact": "0.5"}},
        "launch_event": {"signature": "sig", "slot": 1, "received_at": "2026-09-26T00:00:00Z"},
        "point_in_time_history": {"completed_weeks": 26, "market_cap_usd": "1000000", "return_14d": "0.1", "amihud_illiquidity": "0.01"},
        "wallet_risk": {"circular_volume_flag": False, "coordinated_buy_cluster_flag": False},
    }
    value.update(overrides)
    return value


def by_id(value):
    return {item["cohort_id"]: item for item in value}


def test_complete_point_in_time_evidence_is_assessed_for_all_frozen_cohorts():
    outcomes = by_id(assess(record()))
    assert EXPERIMENT_VERSION == "memecoin-cohorts-v1"
    assert set(outcomes) == {"M0", "M1", "M2", "M3"}
    assert outcomes["M0"]["status"] == "eligible"
    assert outcomes["M1"]["status"] == "candidate_ready"
    assert outcomes["M2"]["status"] == "candidate_ready"
    assert outcomes["M3"]["status"] == "candidate_ready"
    assert all(not value["paper_execution_enabled"] for value in outcomes.values())


def test_missing_evidence_is_retained_as_unresolved_instead_of_assumed_safe():
    outcomes = by_id(assess(record(launch_event=None, point_in_time_history=None,
                                    pool={"volume_h1_usd": "5000"}, wallet_risk=None)))
    assert outcomes["M1"]["reasons"] == ["launch_event_missing"]
    assert outcomes["M2"]["reasons"] == ["point_in_time_history_missing"]
    assert "prior_or_current_h1_volume_missing" in outcomes["M3"]["reasons"]
    assert "wallet_risk_evidence_missing" in outcomes["M3"]["reasons"]
