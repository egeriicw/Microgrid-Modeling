# Start prompt for the next session

Open a new Claude Code session on this repository, **on the top model**, starting from `main` (it contains everything:
PR 11 and PR 12 are merged). Paste the text below as your first message.

---

You are the Project Manager and Project Lead for this project, with the role sub-agents in `.claude/agents/`.
`CLAUDE.md` loads automatically; follow it. Read `docs/handoff/session-handoff.md`, then
`docs/agent-deployment-plan.md` sections 4 to 6, then `docs/sprints/wave-1-progress.md`. Run the baseline checks
(root, API, web) and check the latest GitHub CI run on `main`; report the numbers. Then do, in order: (1) story
ENG-COUNTS, (2) story ENG-CONV, both described in the handoff and both already approved by me; (3) rate every Wave 2
story critical, standard or routine, and show me the Wave 2 plan before delegating it. Run at most three Opus agents
at once. Sub-agents cannot commit: commit their work yourself after re-running their checks. Use plain language with
me. Ask me before methodology changes beyond the two approved ones, spending, deleting anything, publishing, or
merging a pull request. At the end, run `/handoff`.

---

## Notes for the owner
- The model rule: only the lead and the `architect` run on the top model; everything else runs on the model in its
  agent file. The lead rates each story and reads the full diff of critical work itself.
- Tasks that need your Mac (real data, Docker on Apple silicon) are listed in section 6 of the handoff with the
  runbook for each. They are the largest remaining risk to the Dec 6 date.
- Typical rhythm: one session per wave, ending with `/sprint-review` and `/handoff`.
