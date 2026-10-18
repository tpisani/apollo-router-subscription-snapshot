# Scenario pages

Interactive, step-by-step diagrams of how the client reaches the current state in
each situation. Open the files directly in the browser (the fonts come from Google
Fonts; without internet, the page uses the system fonts).

- [`retention.html`](retention.html): **current design.** Full state on both
  channels, retention of the last frame per instance and several instances behind a
  balancer. Eight scenarios: first client on an instance, late joiner,
  goal, burst, reconnect on another instance, instance failure, identity and
  cleanup.
- [`status-and-kickoff.html`](status-and-kickoff.html): **match lifecycle.**
  When the screen uses the query only, when it subscribes and when it stops: the
  kickoff time decides when to start, `FULL_TIME` decides when to stop, and the
  `SCHEDULED` cache never goes past the window. A simulator with the same rules as
  the code, a TTL chart and the tested scenarios.
- [`patch-per-window.html`](patch-per-window.html): previous design (commit
  `9a87874`), with `DELTA` on the stats. Kept as a record of the comparison.
