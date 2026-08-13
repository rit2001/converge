import type pg from "pg";
import {
  deliveryEnvelopeSchema,
  idSchema,
  type DeliveryEnvelope,
  type DeliveryEventType,
} from "@converge/protocol";

export const DEFAULT_OUTBOX_CLAIM_BATCH_SIZE = 32;
export const MAX_OUTBOX_CLAIM_BATCH_SIZE = 32;
export const DEFAULT_OUTBOX_LEASE_MS = 60_000;
export const MAX_OUTBOX_LEASE_MS = 300_000;
export const OUTBOX_MAX_ATTEMPTS = 20;
export const OUTBOX_RETRY_BASE_MS = 250;
export const OUTBOX_RETRY_CAP_MS = 30_000;

const INVALID_DELIVERY_ENVELOPE_CODE = "INVALID_DELIVERY_ENVELOPE";
const INVALID_DELIVERY_ENVELOPE_MESSAGE = "Delivery envelope failed strict validation.";

export type OutboxStatus = "pending" | "leased" | "retry_wait" | "published" | "blocked";

export interface ClaimedOutboxEvent {
  eventId: string;
  boardId: string;
  deliverySeq: number;
  canvasSeq: number | null;
  eventType: DeliveryEventType;
  schemaVersion: 1;
  envelope: DeliveryEnvelope;
  attemptCount: number;
  leaseOwner: string;
  leaseToken: string;
  leasedUntil: Date;
}

export interface ClaimOutboxOptions {
  owner: string;
  batchSize?: number;
  leaseDurationMs?: number;
}

export interface LeaseIdentity {
  eventId: string;
  leaseToken: string;
}

export interface MarkOutboxPublishedInput extends LeaseIdentity {
  publicationId: string;
}

export interface RecordOutboxFailureInput extends LeaseIdentity {
  retryable: boolean;
  errorCode: string;
  errorMessage: string;
  retryJitter?: number;
}

export interface RenewOutboxLeaseInput extends LeaseIdentity {
  leaseDurationMs?: number;
}

export type LeaseMutationResult =
  | {
      outcome: "applied";
      eventId: string;
      status: OutboxStatus;
      attemptCount: number;
      leasedUntil: Date | null;
      nextAttemptAt: Date | null;
      publishedAt: Date | null;
    }
  | { outcome: "stale"; eventId: string };

export type OperatorRetryResult =
  | { outcome: "applied"; eventId: string; status: "pending"; attemptCount: 0 }
  | { outcome: "not_blocked"; eventId: string };

export interface OutboxRepositoryHooks {
  afterClaimUpdate?: (claims: readonly ClaimedOutboxEvent[]) => Promise<void>;
  afterLeaseMutationUpdate?: (context: {
    operation: "publish" | "fail" | "renew";
    eventId: string;
  }) => Promise<void>;
  afterOperatorRetryUpdate?: (eventId: string) => Promise<void>;
}

interface ClaimedRow {
  id: string;
  board_id: string;
  delivery_seq: string;
  canvas_seq: string | null;
  event_type: string;
  schema_version: number;
  payload: unknown;
  attempt_count: number;
  lease_owner: string;
  lease_token: string;
  leased_until: Date;
}

interface MutationRow {
  id: string;
  status: OutboxStatus;
  attempt_count: number;
  leased_until: Date | null;
  next_attempt_at: Date | number | null;
  published_at: Date | null;
}

function requireIntegerInRange(
  value: number,
  minimum: number,
  maximum: number,
  name: string,
): void {
  if (!Number.isSafeInteger(value) || value < minimum || value > maximum)
    throw new RangeError(`${name} must be an integer between ${minimum} and ${maximum}`);
}

function validateOwner(owner: string): string {
  if (!/^[A-Za-z0-9._:-]{1,128}$/.test(owner))
    throw new TypeError("Outbox lease owner must be a bounded opaque identifier");
  return owner;
}

function validatePublicationId(publicationId: string): string {
  if (
    publicationId.length < 1 ||
    publicationId.length > 128 ||
    [...publicationId].some((character) => {
      const code = character.charCodeAt(0);
      return code <= 31 || code === 127;
    })
  )
    throw new TypeError("Publication identifier must be printable and at most 128 characters");
  return publicationId;
}

function sanitizeErrorCode(value: string): string {
  const sanitized = value
    .replace(/[^A-Za-z0-9._:-]/g, "_")
    .slice(0, 64)
    .replace(/^_+|_+$/g, "");
  return sanitized || "UNKNOWN";
}

function sanitizeErrorMessage(value: string): string {
  const withoutControls = [...value]
    .map((character) => {
      const code = character.charCodeAt(0);
      return code <= 31 || code === 127 ? " " : character;
    })
    .join("");
  const sanitized = withoutControls.replace(/\s+/g, " ").trim().slice(0, 500);
  return sanitized || "Publication failed";
}

function validateLeaseIdentity(identity: LeaseIdentity): LeaseIdentity {
  return {
    eventId: idSchema.parse(identity.eventId),
    leaseToken: idSchema.parse(identity.leaseToken),
  };
}

function validateLeaseDuration(value: number | undefined): number {
  const duration = value ?? DEFAULT_OUTBOX_LEASE_MS;
  requireIntegerInRange(duration, 1, MAX_OUTBOX_LEASE_MS, "leaseDurationMs");
  return duration;
}

export function calculateOutboxRetryDelayMs(attemptCount: number, jitterUnit: number): number {
  requireIntegerInRange(attemptCount, 1, OUTBOX_MAX_ATTEMPTS, "attemptCount");
  if (!Number.isFinite(jitterUnit) || jitterUnit < 0 || jitterUnit >= 1)
    throw new RangeError("retryJitter must be in the range [0, 1)");
  const exponentialLimit = Math.min(
    OUTBOX_RETRY_CAP_MS,
    OUTBOX_RETRY_BASE_MS * 2 ** (attemptCount - 1),
  );
  return Math.floor(jitterUnit * (exponentialLimit + 1));
}

function claimedEvent(row: ClaimedRow): ClaimedOutboxEvent {
  const envelope = deliveryEnvelopeSchema.parse(row.payload);
  const deliverySeq = Number(row.delivery_seq);
  const canvasSeq = row.canvas_seq === null ? null : Number(row.canvas_seq);
  if (
    envelope.eventId !== row.id ||
    envelope.boardId !== row.board_id ||
    envelope.deliverySeq !== deliverySeq ||
    envelope.eventType !== row.event_type ||
    envelope.schemaVersion !== row.schema_version
  )
    throw new Error("Outbox envelope identity does not match its relational columns");
  return {
    eventId: row.id,
    boardId: row.board_id,
    deliverySeq,
    canvasSeq,
    eventType: envelope.eventType,
    schemaVersion: envelope.schemaVersion,
    envelope,
    attemptCount: row.attempt_count,
    leaseOwner: row.lease_owner,
    leaseToken: row.lease_token,
    leasedUntil: row.leased_until,
  };
}

function appliedMutation(row: MutationRow): LeaseMutationResult {
  return {
    outcome: "applied",
    eventId: row.id,
    status: row.status,
    attemptCount: row.attempt_count,
    leasedUntil: row.leased_until,
    nextAttemptAt: row.next_attempt_at instanceof Date ? row.next_attempt_at : null,
    publishedAt: row.published_at,
  };
}

export class OutboxRepository {
  constructor(
    private readonly pool: pg.Pool,
    private readonly hooks: OutboxRepositoryHooks = {},
  ) {}

  async claimAvailable(options: ClaimOutboxOptions): Promise<ClaimedOutboxEvent[]> {
    const owner = validateOwner(options.owner);
    const batchSize = options.batchSize ?? DEFAULT_OUTBOX_CLAIM_BATCH_SIZE;
    requireIntegerInRange(batchSize, 1, MAX_OUTBOX_CLAIM_BATCH_SIZE, "batchSize");
    const leaseDurationMs = validateLeaseDuration(options.leaseDurationMs);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      await client.query(
        `WITH exhausted AS (
           SELECT id
           FROM outbox_events
           WHERE status = 'leased'
             AND leased_until <= statement_timestamp()
             AND attempt_count >= $1
           ORDER BY leased_until, created_at, id
           FOR UPDATE SKIP LOCKED
           LIMIT $2
         )
         UPDATE outbox_events event
         SET status = 'blocked',
             lease_owner = NULL,
             lease_token = NULL,
             leased_until = NULL,
             next_attempt_at = 'infinity'::timestamptz,
             last_error_code = 'lease_expired',
             last_error_message = 'Lease expired at the automatic claim limit.',
             last_error_at = statement_timestamp(),
             updated_at = statement_timestamp()
         FROM exhausted
         WHERE event.id = exhausted.id`,
        [OUTBOX_MAX_ATTEMPTS, batchSize],
      );
      const result = await client.query<ClaimedRow>(
        `WITH candidates AS (
           SELECT candidate.id
           FROM outbox_events candidate
           WHERE (
             (
               candidate.status IN ('pending', 'retry_wait')
               AND candidate.next_attempt_at <= statement_timestamp()
             )
             OR (
               candidate.status = 'leased'
               AND candidate.leased_until <= statement_timestamp()
               AND candidate.attempt_count < $1
             )
           )
           AND NOT EXISTS (
             SELECT 1
             FROM outbox_events earlier
             WHERE earlier.board_id = candidate.board_id
               AND earlier.delivery_seq < candidate.delivery_seq
               AND earlier.status <> 'published'
           )
           ORDER BY
             CASE
               WHEN candidate.status = 'leased' THEN candidate.leased_until
               ELSE candidate.next_attempt_at
             END,
             candidate.created_at,
             candidate.id
           FOR UPDATE OF candidate SKIP LOCKED
           LIMIT $2
         )
         UPDATE outbox_events event
         SET status = 'leased',
             attempt_count = event.attempt_count + 1,
             lease_owner = $3,
             lease_token = gen_random_uuid(),
             leased_until = statement_timestamp() + ($4 * interval '1 millisecond'),
             last_error_code = CASE
               WHEN event.status = 'leased' THEN 'lease_expired'
               ELSE event.last_error_code
             END,
             last_error_message = CASE
               WHEN event.status = 'leased' THEN 'Previous lease expired before completion.'
               ELSE event.last_error_message
             END,
             last_error_at = CASE
               WHEN event.status = 'leased' THEN statement_timestamp()
               ELSE event.last_error_at
             END,
             updated_at = statement_timestamp()
         FROM candidates
         WHERE event.id = candidates.id
         RETURNING event.id, event.board_id, event.delivery_seq, event.canvas_seq,
                   event.event_type, event.schema_version, event.payload, event.attempt_count,
                   event.lease_owner, event.lease_token, event.leased_until`,
        [OUTBOX_MAX_ATTEMPTS, batchSize, owner, leaseDurationMs],
      );
      const claims: ClaimedOutboxEvent[] = [];
      for (const row of result.rows) {
        try {
          claims.push(claimedEvent(row));
        } catch {
          const quarantined = await client.query(
            `UPDATE outbox_events
             SET status = 'blocked',
                 lease_owner = NULL,
                 lease_token = NULL,
                 leased_until = NULL,
                 next_attempt_at = 'infinity'::timestamptz,
                 last_error_code = $3,
                 last_error_message = $4,
                 last_error_at = statement_timestamp(),
                 updated_at = statement_timestamp()
             WHERE id = $1
               AND status = 'leased'
               AND lease_token = $2
             RETURNING id`,
            [
              row.id,
              row.lease_token,
              INVALID_DELIVERY_ENVELOPE_CODE,
              INVALID_DELIVERY_ENVELOPE_MESSAGE,
            ],
          );
          if (quarantined.rowCount !== 1)
            throw new Error("Failed to quarantine invalid delivery envelope");
        }
      }
      await this.hooks.afterClaimUpdate?.(claims);
      await client.query("COMMIT");
      return claims;
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async markPublished(input: MarkOutboxPublishedInput): Promise<LeaseMutationResult> {
    const identity = validateLeaseIdentity(input);
    const publicationId = validatePublicationId(input.publicationId);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<MutationRow>(
        `UPDATE outbox_events
         SET status = 'published',
             lease_owner = NULL,
             lease_token = NULL,
             leased_until = NULL,
             next_attempt_at = 'infinity'::timestamptz,
             redis_entry_id = $3,
             published_at = statement_timestamp(),
             last_error_code = NULL,
             last_error_message = NULL,
             last_error_at = NULL,
             updated_at = statement_timestamp()
         WHERE id = $1
           AND status = 'leased'
           AND lease_token = $2
           AND leased_until > statement_timestamp()
         RETURNING id, status, attempt_count, leased_until, next_attempt_at, published_at`,
        [identity.eventId, identity.leaseToken, publicationId],
      );
      const row = result.rows[0];
      if (!row) {
        await client.query("COMMIT");
        return { outcome: "stale", eventId: identity.eventId };
      }
      await this.hooks.afterLeaseMutationUpdate?.({
        operation: "publish",
        eventId: identity.eventId,
      });
      await client.query("COMMIT");
      return appliedMutation(row);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async recordFailure(input: RecordOutboxFailureInput): Promise<LeaseMutationResult> {
    const identity = validateLeaseIdentity(input);
    const errorCode = sanitizeErrorCode(input.errorCode);
    const errorMessage = sanitizeErrorMessage(input.errorMessage);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const current = await client.query<{ attempt_count: number }>(
        `SELECT attempt_count
         FROM outbox_events
         WHERE id = $1
           AND status = 'leased'
           AND lease_token = $2
           AND leased_until > statement_timestamp()
         FOR UPDATE`,
        [identity.eventId, identity.leaseToken],
      );
      const owned = current.rows[0];
      if (!owned) {
        await client.query("COMMIT");
        return { outcome: "stale", eventId: identity.eventId };
      }
      const blocked = !input.retryable || owned.attempt_count >= OUTBOX_MAX_ATTEMPTS;
      let retryDelayMs = 0;
      if (!blocked) {
        if (input.retryJitter === undefined)
          throw new TypeError("retryJitter is required for a retryable failure");
        retryDelayMs = calculateOutboxRetryDelayMs(owned.attempt_count, input.retryJitter);
      }
      const result = await client.query<MutationRow>(
        `UPDATE outbox_events
         SET status = CASE WHEN $3 THEN 'blocked' ELSE 'retry_wait' END,
             lease_owner = NULL,
             lease_token = NULL,
             leased_until = NULL,
             next_attempt_at = CASE
               WHEN $3 THEN 'infinity'::timestamptz
               ELSE statement_timestamp() + ($4 * interval '1 millisecond')
             END,
             last_error_code = $5,
             last_error_message = $6,
             last_error_at = statement_timestamp(),
             updated_at = statement_timestamp()
         WHERE id = $1
           AND status = 'leased'
           AND lease_token = $2
           AND leased_until > statement_timestamp()
         RETURNING id, status, attempt_count, leased_until, next_attempt_at, published_at`,
        [identity.eventId, identity.leaseToken, blocked, retryDelayMs, errorCode, errorMessage],
      );
      const row = result.rows[0];
      if (!row) {
        await client.query("COMMIT");
        return { outcome: "stale", eventId: identity.eventId };
      }
      await this.hooks.afterLeaseMutationUpdate?.({
        operation: "fail",
        eventId: identity.eventId,
      });
      await client.query("COMMIT");
      return appliedMutation(row);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async renewLease(input: RenewOutboxLeaseInput): Promise<LeaseMutationResult> {
    const identity = validateLeaseIdentity(input);
    const leaseDurationMs = validateLeaseDuration(input.leaseDurationMs);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<MutationRow>(
        `UPDATE outbox_events
         SET leased_until = GREATEST(
               leased_until,
               statement_timestamp() + ($3 * interval '1 millisecond')
             ),
             updated_at = statement_timestamp()
         WHERE id = $1
           AND status = 'leased'
           AND lease_token = $2
           AND leased_until > statement_timestamp()
         RETURNING id, status, attempt_count, leased_until, next_attempt_at, published_at`,
        [identity.eventId, identity.leaseToken, leaseDurationMs],
      );
      const row = result.rows[0];
      if (!row) {
        await client.query("COMMIT");
        return { outcome: "stale", eventId: identity.eventId };
      }
      await this.hooks.afterLeaseMutationUpdate?.({
        operation: "renew",
        eventId: identity.eventId,
      });
      await client.query("COMMIT");
      return appliedMutation(row);
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }

  async retryBlocked(eventIdInput: string): Promise<OperatorRetryResult> {
    const eventId = idSchema.parse(eventIdInput);
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await client.query<{ id: string }>(
        `UPDATE outbox_events
         SET status = 'pending',
             attempt_count = 0,
             next_attempt_at = statement_timestamp(),
             last_error_code = NULL,
             last_error_message = NULL,
             last_error_at = NULL,
             updated_at = statement_timestamp()
         WHERE id = $1 AND status = 'blocked'
         RETURNING id`,
        [eventId],
      );
      if (!result.rows[0]) {
        await client.query("COMMIT");
        return { outcome: "not_blocked", eventId };
      }
      await this.hooks.afterOperatorRetryUpdate?.(eventId);
      await client.query("COMMIT");
      return { outcome: "applied", eventId, status: "pending", attemptCount: 0 };
    } catch (error) {
      await client.query("ROLLBACK");
      throw error;
    } finally {
      client.release();
    }
  }
}
