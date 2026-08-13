import assert from "node:assert/strict";
import test from "node:test";
import {
  SCALE_COMMAND_INTERVAL_MS,
  SCALE_RAMP_DOWN_DURATION_MS,
  SCALE_RAMP_DOWN_START_MS,
  SCALE_TOTAL_DURATION_MS,
  createScaleSessionOwnership,
  scaleSessionCloseAtMs,
  scaleParkDurationSeconds,
  shouldClassifySocketFailure,
  shouldCloseScaleSession,
} from "./scale-session.js";

test("owns one persistent connection and one snapshot per scale VU", () => {
  const ownership = createScaleSessionOwnership();
  assert.equal(ownership.start(), true);
  ownership.snapshotRequested();
  assert.equal(ownership.start(), false);
  assert.throws(() => ownership.snapshotRequested(), /snapshot ownership/);
  assert.deepEqual(ownership.snapshot(), { started: true, snapshots: 1, secondInvocations: 1 });
});

test("proves the prior 200/100 signature is one second invocation per VU", () => {
  const owners = Array.from({ length: 100 }, () => createScaleSessionOwnership());
  assert.equal(owners.filter((owner) => owner.start()).length, 100);
  assert.equal(owners.filter((owner) => !owner.start()).length, 100);
  assert.equal(
    owners.reduce((total, owner) => total + owner.snapshot().secondInvocations, 0),
    100,
  );
});

test("parks a completed VU beyond scenario end instead of returning", () => {
  assert.equal(scaleParkDurationSeconds(209_000, SCALE_TOTAL_DURATION_MS), 76);
  assert.equal(scaleParkDurationSeconds(14_500, 20_000), 65.5);
});

test("expected post-completion socket closure is not a protocol failure", () => {
  assert.equal(shouldClassifySocketFailure(true), false);
  assert.equal(shouldClassifySocketFailure(false), true);
});

test("closes each VU before its deterministic ramp-down removal without reconnecting", () => {
  const first = scaleSessionCloseAtMs({
    vu: 1,
    maximumVus: 100,
    rampDownStartMs: SCALE_RAMP_DOWN_START_MS,
    rampDownDurationMs: SCALE_RAMP_DOWN_DURATION_MS,
  });
  const last = scaleSessionCloseAtMs({
    vu: 100,
    maximumVus: 100,
    rampDownStartMs: SCALE_RAMP_DOWN_START_MS,
    rampDownDurationMs: SCALE_RAMP_DOWN_DURATION_MS,
  });
  assert.equal(last, 209_000);
  assert.equal(first, 209_000);
  assert.equal(shouldCloseScaleSession(last - 1, last), false);
  assert.equal(shouldCloseScaleSession(last, last), true);
  assert.ok(first < SCALE_TOTAL_DURATION_MS);
  assert.equal(SCALE_COMMAND_INTERVAL_MS, 250);
  assert.equal(
    scaleSessionCloseAtMs({
      vu: 100,
      maximumVus: 100,
      rampDownStartMs: SCALE_RAMP_DOWN_START_MS,
      rampDownDurationMs: SCALE_RAMP_DOWN_DURATION_MS,
      closeMarginMs: 5_000,
    }),
    205_000,
  );
  assert.equal(
    scaleSessionCloseAtMs({
      vu: 10,
      maximumVus: 10,
      rampDownStartMs: 15_000,
      rampDownDurationMs: 5_000,
      closeMarginMs: 4_000,
    }),
    11_000,
  );
});

test("rejects unsafe persistent-session boundaries", () => {
  assert.throws(() =>
    scaleSessionCloseAtMs({
      vu: 0,
      maximumVus: 100,
      rampDownStartMs: 210_000,
      rampDownDurationMs: 15_000,
    }),
  );
});
