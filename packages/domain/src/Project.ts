import { Schema } from "effect";
import { ProjectId } from "./ids.ts";

// Per-repo git-convention overrides. Absent key = orchestrator default
// (branch "maestro/<ticket-key>", repo default base branch, draft PR on first
// push, distinct Maestro author identity).
export const GitConventionOverrides = Schema.Struct({
  /** Branch name template; `{ticketKey}` is replaced with the ticket's external id. */
  branchPattern: Schema.optionalKey(Schema.NonEmptyString),
  baseBranch: Schema.optionalKey(Schema.NonEmptyString),
  draftPr: Schema.optionalKey(Schema.Boolean),
  commitAuthorName: Schema.optionalKey(Schema.NonEmptyString),
  commitAuthorEmail: Schema.optionalKey(Schema.NonEmptyString),
});
export type GitConventionOverrides = typeof GitConventionOverrides.Type;

// Project tier of the two-tier resource model (agent tier is orchestrator
// config). Absent key = orchestrator default.
export const ResourceTiers = Schema.Struct({
  memoryBaselineMib: Schema.optionalKey(
    Schema.Number.check(Schema.isInt(), Schema.isGreaterThan(0)),
  ),
  burstMultiplier: Schema.optionalKey(Schema.Number.check(Schema.isGreaterThan(1))),
});
export type ResourceTiers = typeof ResourceTiers.Type;

export const Project = Schema.Struct({
  id: ProjectId,
  /** Credential-free git URL; credentials are injected per-invocation, never stored. */
  repoGitUrl: Schema.NonEmptyString,
  /**
   * Linear team key ("FUR" in "FUR-42") that routes this team's webhooks to
   * this project (FUR-18). Null = project not reachable from Linear ingest.
   */
  linearTeamKey: Schema.NullOr(Schema.NonEmptyString),
  /** Master clone location under the orchestrator storage root; null until first clone. */
  localCachePath: Schema.NullOr(Schema.String),
  gitConventions: GitConventionOverrides,
  resources: ResourceTiers,
  createdAt: Schema.Date,
});
export type Project = typeof Project.Type;
