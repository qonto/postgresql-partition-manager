# Security Policy

## Reporting a Vulnerability

We take security vulnerabilities seriously. If you discover a security issue in PostgreSQL Partition Manager, please report it responsibly.

**Do not open a public GitHub issue for security vulnerabilities.**

### How to Report

1. **GitHub Private Security Advisory (preferred):** Use [GitHub's private security advisory feature](https://github.com/qonto/postgresql-partition-manager/security/advisories/new) to report the vulnerability. This ensures the report remains confidential until a fix is available.

2. **Direct contact:** If you cannot use GitHub's security advisory feature, contact the maintainers directly through the channels listed in the repository.

### What to Include

When reporting a vulnerability, please provide:

- A description of the vulnerability and its potential impact
- Steps to reproduce the issue
- Affected versions
- Any suggested fixes or mitigations (if available)

### Response Timeline

- **Acknowledgment:** We will acknowledge receipt of your report within 3 business days.
- **Assessment:** We will provide an initial assessment within 7 business days.
- **Fix:** We aim to release a fix within 30 days of confirming the vulnerability, depending on complexity.

### Disclosure Policy

- We follow coordinated disclosure. Please do not disclose the vulnerability publicly until we have released a fix.
- Once a fix is released, we will publish a security advisory crediting the reporter (unless anonymity is requested).
- Fixed vulnerabilities will be documented in the [CHANGELOG](CHANGELOG.md) under the `Security` section.

## Supported Versions

Security updates are provided for the latest released version only. We recommend always running the most recent version.
