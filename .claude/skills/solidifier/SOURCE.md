Vendored copy of the Claude Code variant of the solidifier skill:
https://github.com/FernandoJRR/solidifier, path
`claude-code/plugins/solidifier/skills/solidifier`, at commit `71fd5a9`
(2026-07-14). MIT license, see `LICENSE`.

Edit only as listed under "Local changes". To update, replace the folder with
a newer upstream commit, change the commit above and re-apply local changes.

## Local changes

Wording fixes from the Codex review of ferrifuge PR #10, kept minimal:

- `SKILL.md`, rigor levels: a request that only asks to review, audit,
  assess or explain code runs as `advisory` whatever the configured rigor
  (upstream, the default `conservative` could edit code on a review request).
- `SKILL.md`, workflow step 2: a user-named target and the configured
  `scope` combine as their intersection (upstream's "or" let a broad
  configured scope widen a one-file request).
- `references/configuration.md`, schema: `patterns.deny` defaults to `[]`;
  the listed values are examples (upstream left the default undefined).
- `references/configuration.md`, precedence: `exclude` and `patterns.deny`
  rank above the current message and are overridden only by an explicit
  request naming them, after confirmation (upstream ranked the message
  first while calling them "always respected").

- Second review round: `scope` joins `exclude` and `patterns.deny` as hard
  policy in the precedence list; `exclude` states its default (generated,
  vendored and migration code); `SKILL.md` honours `requireJustification:
  false` and `allowContractChanges: true` from the config.

- Third review round (ferrifuge PR #15): out-of-scope edits are never made
  to make a change compile (stop or pick another refactor instead); the
  config is the nearest `.solidifier.json` from the target up to the
  project root; `scope`/`exclude` globs are relative to the defining file.

Re-apply these when updating from upstream, unless upstream fixed them.
