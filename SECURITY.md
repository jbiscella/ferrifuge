# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately, through GitHub's private
vulnerability reporting: the **Report a vulnerability** button on the
repository's [Security tab](https://github.com/jbiscella/ferrifuge/security).
Do not open a public issue for them.

A useful report says which version and features are affected, what an
attacker can do, and how to reproduce it.

## Supported versions

Fixes go to the latest release: while it is a 1.0.0 pre-release, as the next pre-release; from 1.0.0, as a patch release of the latest minor. 0.21.0 gets no fixes.

## Disclosure

A fixed vulnerability is announced as a GitHub security advisory of this
repository, and a RustSec advisory is requested, so that `cargo audit` and
`cargo deny` report the affected versions.
