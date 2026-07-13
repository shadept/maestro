import { Schema } from "effect";
import { AgentEffort } from "./AgentSettings.ts";

// The normalized inbound event shape. Every ingest source (Linear webhook,
// generic REST API) maps its platform payload into a TaskContext; everything
// downstream of ingestion only ever sees this.

export const TaskSource = Schema.Literals(["linear", "generic"]);
export type TaskSource = typeof TaskSource.Type;

export const TicketReference = Schema.Struct({
  source: TaskSource,
  /** Platform-native ticket key, e.g. a Linear issue identifier ("FUR-42"). */
  externalId: Schema.NonEmptyString,
});
export type TicketReference = typeof TicketReference.Type;

export const TaskContext = Schema.Struct({
  source: TaskSource,
  ticket: TicketReference,
  /** Who triggered the turn (platform-native user handle or name). */
  actor: Schema.String,
  /** Ticket title; part of the first-turn prompt. Null on comment-triggered turns. */
  title: Schema.NullOr(Schema.String),
  /** The text that drives the turn: triggering comment, or issue body on first turn. */
  body: Schema.String,
  /**
   * Task-level agent model override (FUR-41), the most specific of the three
   * levels. Linear: parsed at ingest from a `maestro:model=<id>` issue label.
   * Null on comment-triggered turns — Linear comment webhooks carry no labels,
   * so task-level overrides can only arrive on label (issue) events; the
   * session's pinned first-turn model covers the turns in between.
   */
  agentModel: Schema.NullOr(Schema.NonEmptyString),
  /** Task-level effort override (`maestro:effort=<level>` label). Same reach as agentModel. */
  agentEffort: Schema.NullOr(AgentEffort),
  /** Platform delivery id — the idempotency key for webhook dedup. */
  deliveryId: Schema.NonEmptyString,
  /** The original platform payload, preserved opaquely for outbound responders. */
  payload: Schema.Unknown,
});
export type TaskContext = typeof TaskContext.Type;
