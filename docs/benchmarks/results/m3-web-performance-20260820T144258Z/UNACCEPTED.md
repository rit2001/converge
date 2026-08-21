# UNACCEPTED HARNESS-MEASUREMENT OBSERVATION

The single corrected official M3.8A2 run did not produce accepted performance evidence.

- Classification: selection-feedback harness measurement failure.
- Bounded failure: `PERFORMANCE_DURATION_INPUT` rejected a non-negative-duration invariant.
- Cause: the animation-frame callback timestamp can represent a frame boundary before a pointer
  event captured later within that frame.
- Impact: no selection percentile or tier result from this run is accepted, and no performance
  budget or production-capacity conclusion may be drawn from it.
- Rerun policy: no second corrected official run was executed.
