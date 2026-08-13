import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { describe, expect, it } from "vitest";
import { DEFAULT_DURATION_BUCKETS_SECONDS, METRIC_CATALOG } from "./telemetry.js";

const yaml = createRequire(import.meta.url)("js-yaml") as {
  load(source: string): unknown;
};

const REQUIRED_ALERTS = [
  "ConvergeDeliveryQuarantineDetected",
  "ConvergeDeliveryFailuresElevated",
  "ConvergeOutboxBlockedDetected",
  "ConvergeOutboxRetriesElevated",
  "ConvergeRecoveryBlockedDetected",
  "ConvergeRecoveryFailuresElevated",
  "ConvergeSnapshotFailuresElevated",
  "ConvergeCompactionBlockedDetected",
  "ConvergeOutboxPublicationLatencyHigh",
  "ConvergeSnapshotLatencyHigh",
  "ConvergeCompactionLatencyHigh",
  "ConvergeRecoveryLatencyHigh",
] as const;

type AlertName = (typeof REQUIRED_ALERTS)[number];
type Subsystem = "delivery" | "outbox" | "recovery" | "snapshot" | "compaction";

interface AlertRule {
  alert: string;
  expr: string;
  for: string;
  labels: Record<string, string>;
  annotations: Record<string, string>;
  record?: unknown;
}

interface RuleFile {
  groups: Array<{ name: string; rules: AlertRule[] }>;
}

interface PromtoolFixture {
  rule_files: string[];
  tests: Array<{
    name: string;
    input_series: Array<{ series: string; values: string }>;
    alert_rule_test: Array<{
      eval_time: string;
      alertname: string;
      exp_alerts: unknown[];
    }>;
  }>;
}

interface ExpectedMetadata {
  duration: "0m" | "15m";
  severity: "warning" | "critical";
  subsystem: Subsystem;
  runbook: string;
}

const EXPECTED_METADATA: Record<AlertName, ExpectedMetadata> = {
  ConvergeDeliveryQuarantineDetected: {
    duration: "0m",
    severity: "critical",
    subsystem: "delivery",
    runbook: "docs/observability/runbooks/m2-8-incidents.md#quarantined-delivery-or-cursor-loss",
  },
  ConvergeDeliveryFailuresElevated: {
    duration: "0m",
    severity: "warning",
    subsystem: "delivery",
    runbook: "docs/observability/runbooks/m2-8-incidents.md#socket-consumerwatchdog-outage",
  },
  ConvergeOutboxBlockedDetected: {
    duration: "0m",
    severity: "critical",
    subsystem: "outbox",
    runbook: "docs/observability/runbooks/m2-8-incidents.md#blocked-outbox-event",
  },
  ConvergeOutboxRetriesElevated: {
    duration: "0m",
    severity: "warning",
    subsystem: "outbox",
    runbook: "docs/observability/runbooks/m2-8-incidents.md#redisoutbox-publication-outage",
  },
  ConvergeRecoveryBlockedDetected: {
    duration: "0m",
    severity: "critical",
    subsystem: "recovery",
    runbook:
      "docs/observability/runbooks/m2-8-incidents.md#recovery-blocked-or-corrupt-snapshot-chain",
  },
  ConvergeRecoveryFailuresElevated: {
    duration: "0m",
    severity: "warning",
    subsystem: "recovery",
    runbook: "docs/observability/runbooks/m2-8-incidents.md#high-recovery-or-publication-latency",
  },
  ConvergeSnapshotFailuresElevated: {
    duration: "0m",
    severity: "warning",
    subsystem: "snapshot",
    runbook: "docs/observability/runbooks/m2-8-incidents.md#snapshot-failure",
  },
  ConvergeCompactionBlockedDetected: {
    duration: "0m",
    severity: "warning",
    subsystem: "compaction",
    runbook: "docs/observability/runbooks/m2-8-incidents.md#compaction-blocked-or-failed",
  },
  ConvergeOutboxPublicationLatencyHigh: {
    duration: "15m",
    severity: "warning",
    subsystem: "outbox",
    runbook: "docs/observability/runbooks/m2-8-incidents.md#high-recovery-or-publication-latency",
  },
  ConvergeSnapshotLatencyHigh: {
    duration: "15m",
    severity: "warning",
    subsystem: "snapshot",
    runbook: "docs/observability/runbooks/m2-8-incidents.md#snapshot-failure",
  },
  ConvergeCompactionLatencyHigh: {
    duration: "15m",
    severity: "warning",
    subsystem: "compaction",
    runbook: "docs/observability/runbooks/m2-8-incidents.md#compaction-blocked-or-failed",
  },
  ConvergeRecoveryLatencyHigh: {
    duration: "15m",
    severity: "warning",
    subsystem: "recovery",
    runbook: "docs/observability/runbooks/m2-8-incidents.md#high-recovery-or-publication-latency",
  },
};

const COUNTER_ALERTS = REQUIRED_ALERTS.slice(0, 8);
const LATENCY_GUARDS: Partial<Record<AlertName, number>> = {
  ConvergeOutboxPublicationLatencyHigh: 20,
  ConvergeSnapshotLatencyHigh: 5,
  ConvergeCompactionLatencyHigh: 5,
  ConvergeRecoveryLatencyHigh: 20,
};
const RATIO_ALERTS = [
  "ConvergeDeliveryFailuresElevated",
  "ConvergeOutboxRetriesElevated",
  "ConvergeRecoveryFailuresElevated",
] as const;
const DEFERRED_ALERTS = [
  "ConvergeApiHttpUnavailable",
  "ConvergeSocketDeliveryUnavailable",
  "ConvergeWorkerCoreUnavailable",
  "ConvergeOutboxDeliveryUnavailable",
  "ConvergeBackgroundWorkStuck",
] as const;

const ruleSource = readFileSync(
  new URL("../../../ops/prometheus/converge-alerts.yml", import.meta.url),
  "utf8",
);
const policySource = readFileSync(
  new URL("../../../docs/observability/m2-8-slos-and-alerts.md", import.meta.url),
  "utf8",
);
const parsed = yaml.load(ruleSource) as RuleFile;
const fixtureSource = readFileSync(
  new URL("../../../ops/prometheus/converge-alert-tests.yml", import.meta.url),
  "utf8",
);
const fixtures = yaml.load(fixtureSource) as PromtoolFixture;
const rootPackage = JSON.parse(
  readFileSync(new URL("../../../package.json", import.meta.url), "utf8"),
) as { scripts: Record<string, string> };
const ciSource = readFileSync(
  new URL("../../../.github/workflows/ci.yml", import.meta.url),
  "utf8",
);
const runnerSource = readFileSync(
  new URL("../../../scripts/check-prometheus.mjs", import.meta.url),
  "utf8",
);
const promtoolMetadata = JSON.parse(
  readFileSync(new URL("../../../ops/prometheus/promtool.json", import.meta.url), "utf8"),
) as { version: string; image: string; digest: string };
const documentationSource = policySource;

function rules(): AlertRule[] {
  const group = parsed.groups[0];
  if (!group) throw new Error("Alert rule group is missing");
  return group.rules;
}

function policyExpression(alert: AlertName): string {
  const escaped = alert.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  const match = policySource.match(
    new RegExp(`\`${escaped}\`:\\n\\n\`\`\`promql\\n([\\s\\S]*?)\\n\`\`\``),
  );
  if (!match?.[1]) throw new Error(`Policy expression is missing for ${alert}`);
  return match[1];
}

function headingAnchor(heading: string): string {
  return heading
    .toLowerCase()
    .replace(/[^a-z0-9 -]/g, "")
    .trim()
    .replace(/ +/g, "-");
}

describe("provider-neutral Prometheus alert rules", () => {
  it("parses as one deterministic group with exactly the required ordered alerts", () => {
    expect(parsed.groups).toHaveLength(1);
    expect(parsed.groups[0]?.name).toBe("converge-m2-operational-alerts");
    expect(rules()).toHaveLength(12);
    expect(rules().map((rule) => rule.alert)).toEqual(REQUIRED_ALERTS);
    expect(new Set(rules().map((rule) => rule.alert)).size).toBe(REQUIRED_ALERTS.length);
    expect(rules().every((rule) => rule.record === undefined)).toBe(true);
  });

  it("copies every authoritative policy expression and fixed metadata exactly", () => {
    for (const rule of rules()) {
      const alert = rule.alert as AlertName;
      const expected = EXPECTED_METADATA[alert];
      expect(rule.expr, alert).toBe(policyExpression(alert));
      expect(rule.for, alert).toBe(expected.duration);
      expect(rule.labels, alert).toEqual({
        service: "converge",
        severity: expected.severity,
        subsystem: expected.subsystem,
      });
      expect(Object.keys(rule.annotations).sort(), alert).toEqual([
        "description",
        "runbook",
        "summary",
      ]);
      expect(rule.annotations.runbook, alert).toBe(expected.runbook);
    }
  });

  it("uses only catalog metrics and valid histogram exposition suffixes", () => {
    const catalog = METRIC_CATALOG as Record<string, { type: string }>;
    for (const rule of rules()) {
      const metricNames = rule.expr.match(/\bconverge_[a-z0-9_]+\b/g) ?? [];
      expect(metricNames.length, rule.alert).toBeGreaterThan(0);
      for (const metricName of metricNames) {
        if (metricName in catalog) continue;
        const derived = metricName.match(/^(.*)_(bucket|count|sum)$/);
        expect(derived, metricName).not.toBeNull();
        expect(catalog[derived?.[1] ?? ""]?.type, metricName).toBe("histogram");
      }
    }
  });

  it("preserves counter, ratio, quantile, and minimum-volume policy guards", () => {
    for (const alert of COUNTER_ALERTS) {
      const rule = rules().find((candidate) => candidate.alert === alert);
      expect(rule?.expr, alert).toContain("increase(");
    }
    for (const alert of RATIO_ALERTS) {
      const expression = rules().find((rule) => rule.alert === alert)?.expr ?? "";
      expect(expression, alert).toContain("/");
      expect(expression, alert).toMatch(/>= 20$/);
    }
    for (const [alert, minimum] of Object.entries(LATENCY_GUARDS)) {
      const expression = rules().find((rule) => rule.alert === alert)?.expr ?? "";
      expect(expression, alert).toContain("histogram_quantile(\n  0.99,");
      expect(expression, alert).toMatch(new RegExp(`_count\\[15m\\]\\)\\) >= ${minimum}$`));
    }
  });

  it("uses bounded fixed annotations whose runbook files and sections resolve", () => {
    for (const rule of rules()) {
      const annotationText = JSON.stringify(rule.annotations);
      expect(annotationText, rule.alert).not.toMatch(/\$(?:labels|value)\b|\{\{|\}\}/i);
      expect(annotationText, rule.alert).not.toMatch(/https?:\/\//i);
      expect(annotationText, rule.alert).not.toMatch(
        /(?:board|user|principal|operation|event|socket|snapshot|redis(?:Entry)?|instance)[_-]?id\b|payload|credential|password|bearer|authorization|database_url|redis_url|\bsql\b|stack(?:trace)?|error[_-]?message/i,
      );

      const reference = rule.annotations.runbook ?? "";
      const [relativePath, anchor] = reference.split("#");
      expect(relativePath, rule.alert).toMatch(/^docs\/observability\/runbooks\/[a-z0-9-]+\.md$/);
      expect(anchor, rule.alert).toBeTruthy();
      const runbookSource = readFileSync(
        new URL(`../../../${relativePath}`, import.meta.url),
        "utf8",
      );
      const anchors = [...runbookSource.matchAll(/^## (.+)$/gm)].map((match) =>
        headingAnchor(match[1] ?? ""),
      );
      expect(anchors, rule.alert).toContain(anchor);
    }
  });

  it("keeps deferred probe and attempt-age alerts absent", () => {
    const names = new Set(rules().map((rule) => rule.alert));
    for (const deferred of DEFERRED_ALERTS) expect(names.has(deferred), deferred).toBe(false);
  });

  it("binds promtool fixtures to the production rules with positive and negative coverage", () => {
    expect(fixtures.rule_files).toEqual(["converge-alerts.yml"]);
    const assertions = fixtures.tests.flatMap((fixture) => fixture.alert_rule_test);
    for (const alert of REQUIRED_ALERTS) {
      const coverage = assertions.filter((assertion) => assertion.alertname === alert);
      expect(
        coverage.some((assertion) => assertion.exp_alerts.length > 0),
        alert,
      ).toBe(true);
      expect(
        coverage.some((assertion) => assertion.exp_alerts.length === 0),
        alert,
      ).toBe(true);
    }
    for (const deferred of DEFERRED_ALERTS)
      expect(
        assertions.some((assertion) => assertion.alertname === deferred),
        deferred,
      ).toBe(false);
  });

  it("represents every ratio volume boundary and latency for-duration boundary", () => {
    for (const subject of ["delivery failure", "outbox retry", "recovery failure"]) {
      expect(
        fixtures.tests.some(
          (fixture) => fixture.name === `${subject} ratio stays below its minimum volume`,
        ),
      ).toBe(true);
      expect(
        fixtures.tests.some(
          (fixture) =>
            fixture.name === `${subject} ratio stays below threshold at sufficient volume`,
        ),
      ).toBe(true);
      expect(
        fixtures.tests.some(
          (fixture) =>
            fixture.name === `${subject} ratio fires above threshold at sufficient volume`,
        ),
      ).toBe(true);
    }
    for (const [subject, alert] of [
      ["outbox", "ConvergeOutboxPublicationLatencyHigh"],
      ["snapshot", "ConvergeSnapshotLatencyHigh"],
      ["compaction", "ConvergeCompactionLatencyHigh"],
      ["recovery", "ConvergeRecoveryLatencyHigh"],
    ] as const) {
      const fixture = fixtures.tests.find(
        (candidate) =>
          candidate.name ===
          `${subject} latency respects volume threshold distribution and for duration`,
      );
      const assertions =
        fixture?.alert_rule_test.filter((entry) => entry.alertname === alert) ?? [];
      expect(
        assertions.map((entry) => [entry.eval_time, entry.exp_alerts.length]),
        alert,
      ).toEqual([
        ["15m", 0],
        ["30m", 0],
        ["45m", 0],
        ["46m", 1],
      ]);
    }
  });

  it("keeps fixture metrics labels and histogram boundaries within the fixed catalog", () => {
    const catalog = METRIC_CATALOG as Record<
      string,
      { type: string; labels: Record<string, readonly string[]> }
    >;
    const expectedBoundaries = [...DEFAULT_DURATION_BUCKETS_SECONDS.map(String), "+Inf"];
    for (const fixture of fixtures.tests) {
      for (const input of fixture.input_series) {
        const selector = input.series.match(/^([a-z0-9_]+)(?:\{([^}]*)\})?$/);
        expect(selector, input.series).not.toBeNull();
        const metricName = selector?.[1] ?? "";
        const derived = metricName.match(/^(.*)_(bucket|count|sum)$/);
        const baseName = derived?.[1] ?? metricName;
        const definition = catalog[baseName];
        expect(definition, metricName).toBeDefined();
        if (derived) expect(definition?.type, metricName).toBe("histogram");

        const labelPairs = [...(selector?.[2] ?? "").matchAll(/([a-z_]+)="([^"]*)"/g)];
        const actualKeys = labelPairs.map((match) => match[1]);
        const expectedKeys =
          derived?.[2] === "bucket" ? ["le"] : Object.keys(definition?.labels ?? {});
        expect(actualKeys, input.series).toEqual(expectedKeys);
        for (const [, key, value] of labelPairs) {
          if (key === "le") continue;
          expect(definition?.labels[key ?? ""], input.series).toContain(value);
        }
      }

      if (fixture.name.includes(" latency ")) {
        const boundaries = fixture.input_series
          .filter((input) => input.series.includes("_bucket{"))
          .map((input) => input.series.match(/le="([^"]+)"/)?.[1]);
        expect(boundaries, fixture.name).toEqual(expectedBoundaries);
      }
    }
  });

  it("pins the runner metadata and enforces the root and CI commands", () => {
    expect(promtoolMetadata).toEqual({
      version: "3.5.0",
      image: "prom/prometheus",
      digest: "sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996",
    });
    expect(runnerSource).toContain("`${metadata.image}:v${metadata.version}@${metadata.digest}`");
    expect(runnerSource).not.toMatch(/latest|IMAGE_OVERRIDE/);
    expect(runnerSource).toContain('"--rm"');
    expect(runnerSource).toContain('"--network"');
    expect(runnerSource).toContain('"--read-only"');
    expect(runnerSource).toContain("readonly");
    expect(runnerSource).not.toContain('join(repositoryRoot, ".env")');
    expect(runnerSource).not.toContain("src=${repositoryRoot},dst=");
    expect(rootPackage.scripts["check:prometheus"]).toBe("node scripts/check-prometheus.mjs");
    expect(ciSource).toContain("- run: pnpm check:prometheus");
    expect(documentationSource).toContain(`Prometheus ${promtoolMetadata.version}`);
    expect(documentationSource).toContain(
      `\`${promtoolMetadata.image}@${promtoolMetadata.digest}\``,
    );
  });
});
