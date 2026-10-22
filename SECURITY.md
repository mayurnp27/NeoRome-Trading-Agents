# Security

NeoRome is an experimental research and paper-execution codebase, not a managed
brokerage service. Review entry points before running code. Do not deploy the
entire workspace or assume source availability authorizes live execution.

## Keep private

Never commit API tokens, broker credentials, private keys, passwords, cookies,
environment files, account identifiers, production databases or raw trading
records. Keep provider data private unless its redistribution rights are known.
Use synthetic, clearly labeled fixtures for public bug reproductions.

The public snapshot excludes operational coordination/evidence, deployment
releases, repair-vault exports, logs, local workspaces and existing Git history.
`.gitignore` is defense in depth: it does not remove already tracked files or
sanitize old commits. Review both the exact staged content and commit history
before each future push. Automated pattern scans cannot guarantee absence of
secrets or private information.

## Runtime boundaries

- Keep the dashboard/control API on loopback. Remote access needs separately
  reviewed authentication, TLS and least-privilege permissions.
- Separate paper and live credentials. Start with offline tests; do not use
  broker permissions or real orders to validate an installation.
- Treat untrusted market/provider content as data, never agent instructions.
- Preserve source freshness, identity, fee, depth, sizing, risk and STOP checks.
- Verify repairs against persisted end-to-end evidence, not service liveness.
- Keep immutable evidence and validated backups outside the public repository.

## Reporting a vulnerability

Do not post credentials, private trade records or exploit details in a public
issue. Use GitHub's private vulnerability reporting if the repository has it
enabled; otherwise contact the repository owner privately. If a credential is
exposed, revoke/rotate it with the provider—deleting the file is insufficient.
