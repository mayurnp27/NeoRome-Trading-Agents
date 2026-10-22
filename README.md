# NeoRome

### An agent-assisted trading research and paper-execution system

NeoRome brings equities, options, prediction markets and crypto research into
one operational workspace. Agents help investigate strategies, review evidence
and maintain software; deterministic code handles market data, signal checks,
risk controls, paper accounting and reporting.

**Paper-first. Evidence-driven. No promise of profitability.**

This repository is a source snapshot, not a hosted trading service or a
one-command live-trading bot. Private credentials, production databases, trading
records and operational history are deliberately not included.

## What is inside

| Area | Components |
| --- | --- |
| Equities | Intraday research, timestamp/data-quality checks, shadow and paper adapters, walk-forward tools |
| Options | Defined-risk spread modeling, stochastic/SMA experiments, lifecycle review and paper valuation |
| Prediction markets | BTC stochastic/SMA experiments, interval alignment, contract validation, independent paper comparison books |
| Crypto | Token discovery/screening, quote provenance and budgets, paper execution and exit/valuation research |
| Shared infrastructure | SQLite paper ledger, idempotency, funded-cost reservations, reconciliation, control-plane events and reports |
| Operations | Read-only dashboard, bounded maintenance/diagnosis components and repair-memory tooling |

Individual experiments have different histories and readiness levels. The code
being present does not mean a strategy is profitable, externally deployed or
authorized to trade. Separate comparison accounts are experiments—not a pooled
portfolio or independent opportunities when they copy the same signal.

## Architecture

```text
Timestamped observations
        |
Deterministic features and strategy gates
        |
Eligibility, freshness, depth, fees and risk checks
        |
Paper execution -> persisted ledger -> reconciliation
        |                                  |
        +---------- reports and dashboard -+

Agents: research, review, diagnosis and supervised integration
Runtime: ordinary code; no language-model call required per market tick
```

Missing evidence is not a successful check. Rejected signals and no-trade
decisions are valid outcomes. A running service alone is not end-to-end proof.

## Quick start: offline accounting rehearsal

Use Python 3.11 or newer. The rehearsal and core tests below use the standard
library; provider-specific tools have additional dependencies and credentials.

```bash
git clone https://github.com/mayurnp27/NeoRome-Trading-Agents.git
cd NeoRome-Trading-Agents
python3 -m venv .venv
source .venv/bin/activate

python -m neorome.paper_rehearsal --output-dir /tmp/neorome-first-rehearsal
```

Choose a new, nonexistent output directory for each run. The rehearsal uses
explicit synthetic observations and outcomes from [examples/paper](examples/paper).
It tests accounting, rejection and recovery—not market performance. It does not
submit broker orders or need broker credentials.

Run the core offline tests:

```bash
python -m unittest discover -s tests -p 'test_paper_ledger.py'
python -m unittest discover -s tests -p 'test_control_plane_store.py'
python -m unittest discover -s tests -p 'test_btc_bracket_engine.py'
python -m unittest discover -s operations/visual_dashboard -p 'test_server.py'
```

These are focused checks, not a claim that every integration test is portable
without its data/dependencies. Some research tools expect separately supplied
datasets, optional packages or operator-specific deployment paths.

## Dashboard

The dashboard under [operations/visual_dashboard](operations/visual_dashboard)
reads explicitly registered paper books and operational status files. Its
default production paths are configuration assumptions, not bundled databases.
Without those sources it should report missing coverage, not invent activity.

- Exit P&L is fee-net proceeds less allocated basis.
- Settlement rows avoid counting earlier partial exits twice.
- Entries have N/A realized P&L; flat-account marks/timestamps are N/A.
- Genuinely missing valuation inputs remain Unknown; known old marks retain
  their original timestamps.

The viewer is observational and loopback-only by default. Do not expose it or
the control plane publicly without a separately reviewed authenticated gateway.

## Repository map

```text
neorome/                      Shared engines, accounting, adapters and reporting
desks/                        Desk-specific research and strategy components
operations/visual_dashboard/ Read-only operational interface
operations/maintenance_agent/ Maintenance and diagnosis components
tests/                        Core regression and integration tests
examples/paper/               Explicitly synthetic rehearsal inputs
config/                       Example configuration, not credentials
requirements/                 Optional integration dependencies
```

See [Architecture](docs/ARCHITECTURE.md) and
[Security](SECURITY.md) before connecting data or deploying components.

## Safety and research boundaries

Paper fills are simulations, not broker fills. Synthetic fixtures are software
tests, not backtests. Historical or paper results do not establish live execution
quality or future returns. Quotes, contract identity, settlement rules, fees,
depth, sizing and source timestamps must be independently valid at decision time.
Stops and modeled exits do not guarantee executable prices or cap actual losses.

Broker/provider adapters are included for research and integration work. This
repository grants no trading authority, credentials, data entitlements or budget.
Review each entry point before running it; do not start every module or service.
The original independently managed prediction-market workspace and third-party
bot checkouts are not redistributed here.

## Publication and licensing

The initial public release is a reviewed source snapshot without the private
workspace's Git history, raw market-data archives, account ledgers, incident
exports or deployment receipts. No production dashboard screenshot is included.

No open-source license has been selected for the original project code. Public
visibility is not a blanket license grant. Third-party dependencies remain
subject to their own licenses and provider terms.
