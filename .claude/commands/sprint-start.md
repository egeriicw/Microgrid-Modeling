---
description: Start or resume a sprint as Project Manager (read state, verify baseline, propose the plan)
---
Act as Project Manager and Project Lead. Follow the "First thing every session" steps in CLAUDE.md:
read `docs/handoff/session-handoff.md`, the progress log, the decision log and the current sprint backlog.
Run the four checks (pytest, ruff check, ruff format --check, mypy) and report whether the baseline is green.
Then propose this session's plan: which stories, which sub-agent for each, which can run in parallel, what needs the
owner, and what you will not do. Wait for the owner's go-ahead before delegating. Arguments (optional focus): $ARGUMENTS
