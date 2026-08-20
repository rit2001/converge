# M3 web performance methodology and closure

M3.8A2 is closed as an unaccepted bounded measurement attempt. M3 will not spend further work on this
browser-latency measurement. No frontend latency, frame-rate, 1,000-object performance, concurrency,
production-capacity, or service-level guarantee is claimed.

## Retained deterministic bundle gate

`pnpm check:web-budgets` reads the Next production app-build manifest and production assets. It counts
shared and route-specific files once, calculates raw and gzip sizes, rejects malformed or duplicate
manifest evidence, verifies that the landing bundle does not contain known studio-only code, and
enforces these fixed engineering budgets:

- landing initial JavaScript: at most 184,320 gzip bytes;
- studio initial JavaScript: at most 460,800 gzip bytes;
- initial route CSS: at most 81,920 gzip bytes.

The checker uses strict versioned, privacy-safe evidence and has browser-free tests for manifest
classification, shared-asset deduplication, gzip calculation, exact threshold boundaries, unknown
fields, sensitive evidence, and unsupported capacity language. It requires an existing production
build and creates no result directory.

The first unaccepted diagnostic independently observed 106,857 gzip bytes of landing JavaScript,
163,224 gzip bytes of studio JavaScript, and 8,511 gzip bytes of initial route CSS. Those bundle facts
are deterministic build evidence; they do not rehabilitate the rejected browser-timing evidence.

## Unaccepted browser attempts

The first diagnostic used Playwright action completion as selection latency and compared unnormalized
browser floating-point durations. It is permanently unaccepted. Before rejection, it rendered exact
100, 500, and 1,000-object Layers counts with no page errors, hydration errors, or horizontal overflow.
Those facts prove bounded fixture/route wiring only, not responsiveness at any tier.

The single corrected run attempted captured-pointer-to-next-frame selection timing. Its callback used
the animation-frame timestamp, which can represent a frame boundary before a pointer event captured
later in that frame. The non-negative duration validator rejected the evidence before an accepted tier
artifact could be produced. Per the one-run policy, it was not corrected or rerun.

Only the two timestamped `UNACCEPTED.md` records remain. Raw JSON, environment output, traces,
screenshots, databases, builds, and executable multi-tier measurement paths were removed so they cannot
be mistaken for acceptance evidence.

Production-capacity certification, concurrency testing, and deployment-specific performance evidence
remain future deployment/load work with an independently reviewed harness and reference environment.
