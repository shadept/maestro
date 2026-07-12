// @maestro/domain — entities, state machines, errors, Schemas.
// Pure package: no IO, no dependencies beyond `effect`.
// Placeholder entry point: real domain model lands in M1.2.
import { Effect } from "effect";

export const placeholder = Effect.succeed("@maestro/domain" as const);
