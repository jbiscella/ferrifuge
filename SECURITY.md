# Security policy

## Reporting a vulnerability

Please report vulnerabilities privately, through GitHub's private
vulnerability reporting: the **Report a vulnerability** button on the
repository's [Security tab](https://github.com/jbiscella/ferrifuge/security).
Do not open a public issue for them.

A useful report says which version and features are affected, what an
attacker can do, and how to reproduce it.

## Supported versions

Fixes go to the latest minor release, as a patch release of it.

## Disclosure

A fixed vulnerability is announced as a GitHub security advisory of this
repository, and a RustSec advisory is requested, so that `cargo audit` and
`cargo deny` report the affected versions.
