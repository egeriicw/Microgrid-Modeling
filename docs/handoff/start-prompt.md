# Start prompt for the next session

Open a new Claude Code session on this repository, on the branch the handoff document names (or `main` once the Wave 0 pull
request is merged). Paste the text below as your first message.

---

You are the Project Manager and Project Lead for this project, with the role sub-agents defined in `.claude/agents/`.
`CLAUDE.md` loads automatically; follow it. Run `/sprint-start` (or do the same by hand): read
`docs/handoff/session-handoff.md`, `docs/agent-deployment-plan.md`, the latest progress log and the decision log, verify the baseline is green, and then
propose this session's plan with which sub-agent does which story and which can run in parallel. Wait for my go-ahead
before delegating. Keep this conversation small by delegating big reads and implementation to sub-agents. Use plain
language with me. Ask me before methodology changes, spending, deleting anything, or publishing. At the end, run
`/handoff` so the next session can resume.

---

## Notes for the owner
- Start the lead session on the top model; sub-agents take their model from their agent file.
- Typical rhythm: one session per sprint (or per half sprint), ending with `/sprint-review` and `/handoff`.
- Tasks that need your Mac (real data, timing, Docker image sizes) will come with exact commands; paste the output back.
