const KIND_PREFIXES = Object.freeze({
  client: "10000000",
  object: "20000000",
  operation: "30000000",
});

export function deterministicUuid(kind, vuOrdinal, iterationOrdinal, commandOrdinal) {
  if (!Object.hasOwn(KIND_PREFIXES, kind)) throw new Error("Invalid collaboration identity kind");
  for (const value of [vuOrdinal, iterationOrdinal, commandOrdinal])
    if (!Number.isSafeInteger(value) || value < 0)
      throw new Error("Invalid collaboration identity ordinal");
  const identityOrdinal =
    BigInt(vuOrdinal) * 1_000_000_000n + BigInt(iterationOrdinal) * 1_000n + BigInt(commandOrdinal);
  if (identityOrdinal > 0xffffffffffffn)
    throw new Error("Collaboration identity ordinal exceeds UUID capacity");
  const tail = identityOrdinal.toString(16).padStart(12, "0");
  return `${KIND_PREFIXES[kind]}-0000-4000-8000-${tail}`;
}
