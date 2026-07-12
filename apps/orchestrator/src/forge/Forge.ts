import type { ForgeError } from "@maestro/domain";
import type { Effect } from "effect";

// The Forge port: everything OutboundGit needs from a code forge. GitHub is
// the only M1 implementation (GitHubForge); a GitLab forge would implement
// this same interface behind its own service key, selected per config at the
// composition root.

export interface EnsurePullRequest {
  /** The project's credential-free remote URL; the forge derives its own coordinates from it. */
  readonly repoGitUrl: string;
  readonly headBranch: string;
  readonly baseBranch: string;
  /** Used when the call opens the PR; an existing PR's title is never rewritten. */
  readonly title: string;
  readonly body: string;
  /** Open as draft (the non-intrusive default per Tech Requirements §10). */
  readonly draft: boolean;
  /** PR number already recorded on the session, if any — updates instead of creating. */
  readonly existingNumber: number | null;
}

export interface PullRequestRef {
  readonly number: number;
  readonly url: string;
  /** True when this call opened the PR (first publish). */
  readonly created: boolean;
}

export interface Forge {
  /**
   * Idempotently ensures a PR exists for the branch: creates a draft on first
   * publish, updates the existing PR afterwards (the pushed branch already
   * carries the new commits; the API call refreshes the Maestro-owned body).
   */
  readonly ensurePullRequest: (
    args: EnsurePullRequest,
  ) => Effect.Effect<PullRequestRef, ForgeError>;
}
