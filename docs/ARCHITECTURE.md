# NeoRome architecture

NeoRome separates agent-assisted engineering from deterministic trading runtime.
The public source snapshot is not a copy of private production state.

## Desk ownership

Equities, options, prediction markets and crypto own their strategies and data
adapters. Shared accounting and interfaces support consistent provenance and
reconciliation. Firm-level review coordinates integration and overlapping risk.
An experiment account is not automatically part of a consolidated portfolio.

## Data and decisions

Market observations retain source identity, capture timestamps and hashes.
Strategies consume information available at the decision time and preserve
rejection reasons. Contract bounds, completed candles, quote freshness, depth,
fees and sizing are independent acceptance gates—not optional dashboard labels.

## Accounting and control plane

The paper ledger persists proposals, reservations, positions, fills and closes.
Idempotency and restart/reconciliation checks protect accounting consistency.
Separate control-plane events describe operational activity, reports, incidents
and approved work; a monitoring projection must not replace a financial book.

Different experiments may use distinct paper engines. Do not sum them into a
portfolio return without a separately defined capital/accounting model.

## Operations

The dashboard reads registered paper sources and exposes known/missing/stale
evidence explicitly. Maintenance components observe failures and support bounded
diagnosis/recovery; they do not create general trading authority or permission
to relax strategy checks. Optional repair-memory tooling expects a separately
configured private vault and canonical incident source, neither bundled here.

## Public-release boundary

Public source includes first-party engines, desk code, tests, synthetic examples
and the interface. Private data stores, deployment receipts, historic reports,
host credentials, agent chat records and vendored/external workspaces are omitted.
Some legacy source paths still describe operator-managed deployments. Provider
access, licenses, runtime configuration and full end-to-end deployment require
separate setup; the offline rehearsal is the supported starting point.
