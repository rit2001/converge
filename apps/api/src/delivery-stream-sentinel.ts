export const DELIVERY_STREAM_INITIALIZATION_TYPE = "converge.stream.initialized.v1" as const;
export const DELIVERY_STREAM_INITIALIZATION_TYPE_FIELD = "controlType" as const;
export const DELIVERY_STREAM_INITIALIZATION_TOKEN_FIELD = "generation" as const;

const CANONICAL_GENERATION_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

export function isCanonicalDeliveryStreamGeneration(value: unknown): value is string {
  return (
    typeof value === "string" && value.length === 36 && CANONICAL_GENERATION_PATTERN.test(value)
  );
}
