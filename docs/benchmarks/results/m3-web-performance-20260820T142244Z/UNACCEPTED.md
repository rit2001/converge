# UNACCEPTED HARNESS-MEASUREMENT OBSERVATION

This record identifies the first M3.8A2 diagnostic run. It is permanently unaccepted and contributes
to no frontend-latency, object-tier, or production-capacity claim because selection feedback included
Playwright action/transport duration and browser timer noise was not normalized before threshold
evaluation.

The diagnostic did establish only these bounded facts before its generated raw files were removed:

- the production route rendered exactly 100, 500, and 1,000 seeded objects;
- the browser reported no page or hydration errors and no horizontal overflow;
- bundle collection reported 106,857 gzip bytes for landing JavaScript, 163,224 gzip bytes for studio
  JavaScript, and 8,511 gzip bytes for initial route CSS.

The observation must not be compared with or substituted for a corrected accepted artifact.
