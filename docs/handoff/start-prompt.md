# Start prompt for the next session

Open a new Claude Code session on this repository, on the branch `claude/loving-pascal-x8khbu` (it contains everything;
nothing is merged to `main`). Paste the text below as your first message.

---

You are the Project Manager and Project Lead for this project, with the role sub-agents defined in `.claude/agents/`.
`CLAUDE.md` loads automatically; follow it. Run `/sprint-start` (or do the same by hand): read
`docs/handoff/session-handoff.md`, the Sprint 1 progress log and decision log, verify the baseline is green, and then
propose this session's plan with which sub-agent does which story and which can run in parallel. Wait for my go-ahead
before delegating. Keep this conversation small by delegating big reads and implementation to sub-agents. Use plain
language with me. Ask me before methodology changes, spending, deleting anything, or publishing. At the end, run
`/handoff` so the next session can resume.

---

## Notes for the owner
- If the new session starts on a different branch, tell it to check out `claude/loving-pascal-x8khbu` first.
- Typical rhythm: one session per sprint (or per half sprint), ending with `/sprint-review` and `/handoff`.
- Tasks that need your Mac (real data, timing, Docker image sizes) will come with exact commands; paste the output back.
