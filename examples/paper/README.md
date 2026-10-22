# Four-desk rehearsal inputs

The fixture runner uses synthetic prices and explicitly constructed outcomes.
Its purpose is testing cash accounting, sizing and recovery, not measuring an
investment strategy. It neither imports a broker client nor loads credentials.

The portfolio starts with $800 split exactly as requested. A loss and rejection
must remain visible. Repeat runs must use a new output directory so that an
old ledger cannot be mistaken for a fresh portfolio.

Historical evidence and observed token discoveries are separate: source
observations must never be relabeled as fills, and fixture eligibility must
never carry into an observed-token execution decision.
