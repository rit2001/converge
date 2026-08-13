const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export const K6_METRIC_NAMES = Object.freeze([
  "converge_socket_connect_duration",
  "converge_board_join_ack_duration",
  "converge_command_ack_duration",
  "converge_live_delivery_duration",
  "converge_iteration_failures",
  "converge_protocol_failures",
  "converge_duplicate_events",
  "converge_sequence_gaps",
  "converge_commands_acknowledged",
  "converge_live_events_received",
]);
export const K6_ALLOWED_TAGS = Object.freeze(["profile", "operation_type", "outcome"]);

const PROFILES = Object.freeze({
  smoke: Object.freeze({ vus: 2, duration: "30s", commandsPerClient: 2, intervalMs: 1_000 }),
  baseline: Object.freeze({ vus: 10, duration: "2m", commandsPerClient: 10, intervalMs: 1_000 }),
  "scale-step": Object.freeze({
    stages: Object.freeze([
      Object.freeze({ duration: "1m", target: 10 }),
      Object.freeze({ duration: "2m", target: 50 }),
      Object.freeze({ duration: "2m", target: 100 }),
      Object.freeze({ duration: "30s", target: 0 }),
    ]),
    commandsPerClient: 10,
    intervalMs: 1_000,
  }),
});

function fail(code) {
  throw new Error(`Invalid k6 workload configuration: ${code}`);
}

function hasControlCharacters(value) {
  return [...value].some((character) => {
    const code = character.charCodeAt(0);
    return code < 32 || code === 127;
  });
}

function integer(env, name, fallback, minimum, maximum) {
  const raw = env[name];
  if (raw === undefined || raw === "") return fallback;
  if (!/^[0-9]+$/.test(raw)) fail(`${name}_INVALID`);
  const value = Number(raw);
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum)
    fail(`${name}_OUT_OF_RANGE`);
  return value;
}

function duration(env, name, fallback) {
  const raw = env[name] ?? fallback;
  const match = /^([1-9][0-9]*)(s|m)$/.exec(raw);
  if (!match) fail(`${name}_INVALID`);
  const seconds = Number(match[1]) * (match[2] === "m" ? 60 : 1);
  if (seconds > 600) fail(`${name}_OUT_OF_RANGE`);
  return raw;
}

function target(env, name, protocols) {
  const raw = env[name];
  if (!raw) fail(`${name}_REQUIRED`);
  const match = /^(https?|wss?):\/\/(\[[0-9a-fA-F:]+\]|[A-Za-z0-9.-]+)(?::([0-9]{1,5}))?\/?$/.exec(
    raw,
  );
  if (!match) fail(`${name}_INVALID`);
  const protocol = `${match[1]}:`;
  if (!protocols.includes(protocol)) fail(`${name}_PROTOCOL`);
  const port = match[3] === undefined ? undefined : Number(match[3]);
  if (port !== undefined && (port < 1 || port > 65_535)) fail(`${name}_INVALID`);
  const hostname = match[2].replace(/^\[|\]$/g, "").toLowerCase();
  const loopback = hostname === "localhost" || hostname === "127.0.0.1" || hostname === "::1";
  return {
    value: `${match[1]}://${match[2]}${port === undefined ? "" : `:${port}`}`,
    loopback,
  };
}

export function parseWorkloadConfig(env, { requireStateful = true } = {}) {
  const profileName = env.CONVERGE_PROFILE ?? "smoke";
  if (!(profileName in PROFILES)) fail("CONVERGE_PROFILE_INVALID");
  const profile = PROFILES[profileName];
  if (!requireStateful) return Object.freeze({ profile: profileName });

  const base = target(env, "CONVERGE_BASE_URL", ["http:", "https:"]);
  const socket = target(env, "CONVERGE_SOCKET_URL", ["ws:", "wss:", "http:", "https:"]);
  if ((!base.loopback || !socket.loopback) && env.CONVERGE_ALLOW_REMOTE_TARGET !== "true")
    fail("CONVERGE_ALLOW_REMOTE_TARGET_REQUIRED");

  const boardId = env.CONVERGE_BOARD_ID;
  if (!boardId || !UUID_PATTERN.test(boardId)) fail("CONVERGE_BOARD_ID_INVALID");
  const authToken = env.CONVERGE_AUTH_TOKEN;
  if (!authToken || authToken.length > 4_096 || hasControlCharacters(authToken))
    fail("CONVERGE_AUTH_TOKEN_INVALID");

  const configuredVus = integer(env, "CONVERGE_VUS", profile.vus ?? 10, 1, 100);
  const configuredDuration = duration(env, "CONVERGE_DURATION", profile.duration ?? "2m");
  return Object.freeze({
    profile: profileName,
    baseUrl: base.value,
    socketUrl: socket.value,
    boardId,
    authToken,
    vus: configuredVus,
    duration: configuredDuration,
    stages: profileName === "scale-step" ? profile.stages : undefined,
    commandsPerClient: integer(
      env,
      "CONVERGE_COMMANDS_PER_CLIENT",
      profile.commandsPerClient,
      1,
      100,
    ),
    commandIntervalMs: integer(
      env,
      "CONVERGE_COMMAND_INTERVAL_MS",
      profile.intervalMs,
      100,
      60_000,
    ),
    maxPacketBytes: integer(env, "CONVERGE_MAX_PACKET_BYTES", 131_072, 1_024, 1_048_576),
    connectTimeoutMs: integer(env, "CONVERGE_CONNECT_TIMEOUT_MS", 10_000, 100, 60_000),
    acknowledgementTimeoutMs: integer(env, "CONVERGE_ACK_TIMEOUT_MS", 10_000, 100, 60_000),
    debugPhases: env.CONVERGE_DEBUG_PHASES === "true",
    debugRepeat: env.CONVERGE_DEBUG_REPEAT === "true",
    debugConcurrent: env.CONVERGE_DEBUG_CONCURRENT === "true",
    debugSequential: env.CONVERGE_DEBUG_SEQUENTIAL === "true",
  });
}

export function workloadOptions(config) {
  const execution = config.debugSequential
    ? {
        scenarios: {
          debug_sequential: {
            executor: "per-vu-iterations",
            vus: 1,
            iterations: 2,
            maxDuration: "15s",
          },
        },
      }
    : config.debugConcurrent
      ? {
          scenarios: {
            debug_concurrent: {
              executor: "per-vu-iterations",
              vus: 2,
              iterations: 1,
              maxDuration: "15s",
            },
          },
        }
      : config.debugRepeat
        ? {
            scenarios: {
              debug_repeat: {
                executor: "per-vu-iterations",
                vus: 2,
                iterations: 4,
                maxDuration: "30s",
              },
            },
          }
        : config.debugPhases
          ? { vus: 1, iterations: 1 }
          : config.profile === "scale-step"
            ? { stages: config.stages }
            : { vus: config.vus, duration: config.duration };
  const thresholds = {
    converge_iteration_failures: ["rate<0.01"],
    converge_protocol_failures: ["rate==0"],
    converge_sequence_gaps: ["count==0"],
    converge_socket_connect_duration: ["p(99)<5000"],
    converge_board_join_ack_duration: ["p(99)<5000"],
  };
  if (config.profile !== "scale-step") thresholds.converge_command_ack_duration = ["p(99)<2000"];
  return {
    ...execution,
    systemTags: [],
    thresholds,
  };
}
