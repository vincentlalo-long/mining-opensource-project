# Decisions log

Single place for architecture decisions, schema notes and open questions. Append-only: add a new entry
at the bottom, do not rewrite old ones. If an old decision changes, add a new entry that supersedes it.

One entry = one decision, 3–5 lines: what we decided, why, what we rejected. This is what the brief
asks for when it requires students to justify major architectural decisions.

## Template

```
## D-NNN — <short title> (<YYYY-MM-DD>)
Decision: ...
Why: ...
Rejected: ...
```

---

## D-001 — One repository, documentation kept in three files (2026-11-13)

Decision: A single Git repository for code, config and manifests; only `README.md`, `AGENTS.md` and this
file hold written documentation, plus `docs/report.md` at the end.
Why: the team works on three machines and AI assistants kept generating stray markdown files at the
root, which made the repo unreadable and duplicated content that the final report must contain anyway.
Rejected: a full `docs/` tree with ADR-per-file, per-experiment write-ups and a weekly plan file —
too much prose to maintain for a 12-week course project.
