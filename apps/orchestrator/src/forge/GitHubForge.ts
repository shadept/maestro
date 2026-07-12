import { ForgeApiError, type ForgeError, RepoUrlParseError } from "@maestro/domain";
import { Context, Effect, Layer, Option, Redacted } from "effect";
import { Octokit } from "octokit";
import { AppConfig } from "../config/AppConfig.ts";
import type { EnsurePullRequest, Forge, PullRequestRef } from "./Forge.ts";

export interface RepoSlug {
  readonly owner: string;
  readonly repo: string;
}

// The remote URL forms GitHub actually serves. The project stores one
// credential-free URL (used verbatim for clone/fetch/push); forge coordinates
// are derived from it here rather than stored separately.
const urlForms = [
  /^https?:\/\/[^/]+\/([^/]+)\/([^/]+?)(?:\.git)?\/?$/, // https://github.com/owner/repo(.git)
  /^ssh:\/\/(?:[^@/]+@)?[^/]+(?::\d+)?\/([^/]+)\/([^/]+?)(?:\.git)?\/?$/, // ssh://git@github.com/owner/repo(.git)
  /^[^@/\s]+@[^:/\s]+:([^/]+)\/([^/]+?)(?:\.git)?\/?$/, // git@github.com:owner/repo(.git)
];

export const parseGitHubRepoUrl = (url: string): RepoSlug | null => {
  for (const form of urlForms) {
    const match = url.match(form);
    if (match?.[1] !== undefined && match[2] !== undefined) {
      return { owner: match[1], repo: match[2] };
    }
  }
  return null;
};

const toApiError = (operation: string) => (error: unknown) =>
  new ForgeApiError({
    operation,
    message: error instanceof Error ? error.message : String(error),
    status:
      typeof error === "object" &&
      error !== null &&
      "status" in error &&
      typeof error.status === "number"
        ? error.status
        : null,
  });

/** A call the fake forge observed, recorded for test assertions. */
export interface ForgeCall {
  readonly op: "create" | "update";
  readonly args: EnsurePullRequest;
}

export class GitHubForge extends Context.Service<GitHubForge, Forge>()(
  "maestro/forge/GitHubForge",
) {
  static readonly layer = Layer.effect(
    GitHubForge,
    Effect.gen(function* () {
      const config = yield* AppConfig;
      // Built once; absent token must not fail boot (publishing fails per-call).
      const octokit = Option.map(
        config.githubToken,
        (token) => new Octokit({ auth: Redacted.value(token) }),
      );

      const request = <A>(operation: string, run: () => Promise<A>) =>
        Effect.tryPromise({ try: run, catch: toApiError(operation) });

      return {
        ensurePullRequest: Effect.fn("GitHubForge.ensurePullRequest")(function* (
          args: EnsurePullRequest,
        ) {
          if (Option.isNone(octokit)) {
            return yield* new ForgeApiError({
              operation: "GitHubForge.ensurePullRequest",
              message: "MAESTRO_GITHUB_TOKEN is not configured",
              status: null,
            });
          }
          const gh = octokit.value;
          const slug = parseGitHubRepoUrl(args.repoGitUrl);
          if (slug === null) {
            return yield* new RepoUrlParseError({ url: args.repoGitUrl, forge: "github" });
          }
          const { owner, repo } = slug;

          const update = (pullNumber: number): Effect.Effect<PullRequestRef, ForgeError> =>
            request("GitHubForge.updatePullRequest", () =>
              // the push already updated the PR's commits; refresh only the
              // Maestro-owned body (never the title — users may edit it)
              gh.rest.pulls.update({ owner, repo, pull_number: pullNumber, body: args.body }),
            ).pipe(
              Effect.map((res) => ({
                number: res.data.number,
                url: res.data.html_url,
                created: false,
              })),
            );

          if (args.existingNumber !== null) {
            return yield* update(args.existingNumber);
          }

          return yield* request("GitHubForge.createPullRequest", () =>
            gh.rest.pulls.create({
              owner,
              repo,
              title: args.title,
              body: args.body,
              head: args.headBranch,
              base: args.baseBranch,
              draft: args.draft,
            }),
          ).pipe(
            Effect.map((res) => ({
              number: res.data.number,
              url: res.data.html_url,
              created: true,
            })),
            Effect.catch((error) =>
              // 422 "A pull request already exists" — the session lost its PR
              // number (crash between create and persist); recover it by head.
              error._tag === "ForgeApiError" && error.status === 422
                ? request("GitHubForge.findPullRequest", () =>
                    gh.rest.pulls.list({
                      owner,
                      repo,
                      head: `${owner}:${args.headBranch}`,
                      state: "open",
                    }),
                  ).pipe(
                    Effect.flatMap((res) => {
                      const found = res.data[0];
                      return found === undefined ? Effect.fail(error) : update(found.number);
                    }),
                  )
                : Effect.fail(error),
            ),
          );
        }),
      };
    }),
  );

  /**
   * In-memory fake per the .layerTest convention — never talks to GitHub.
   * Pass a `calls` array to observe the forge traffic; `failWith` makes every
   * call fail (publish-failure paths).
   */
  static readonly layerTest = (
    options: { readonly calls?: Array<ForgeCall>; readonly failWith?: ForgeError } = {},
  ) =>
    Layer.sync(GitHubForge)(() => {
      const prs = new Map<string, { number: number; url: string }>();
      let nextNumber = 1;
      const keyOf = (args: EnsurePullRequest) => `${args.repoGitUrl}#${args.headBranch}`;
      const urlFor = (args: EnsurePullRequest, number: number) =>
        `https://github.test/${args.headBranch}/pull/${number}`;
      return {
        ensurePullRequest: Effect.fn("GitHubForge.ensurePullRequest")(function* (
          args: EnsurePullRequest,
        ) {
          if (options.failWith !== undefined) {
            return yield* Effect.fail(options.failWith);
          }
          // Like GitHub: a caller-known PR number or an open PR for the head
          // branch means update, otherwise create. existingNumber is checked
          // first so the fake behaves even when the layer (and this map) is
          // rebuilt between effects.
          const known = prs.get(keyOf(args));
          const existing =
            known ??
            (args.existingNumber !== null
              ? { number: args.existingNumber, url: urlFor(args, args.existingNumber) }
              : undefined);
          if (existing !== undefined) {
            options.calls?.push({ op: "update", args });
            return { ...existing, created: false };
          }
          const pr = { number: nextNumber++, url: urlFor(args, nextNumber - 1) };
          prs.set(keyOf(args), pr);
          options.calls?.push({ op: "create", args });
          return { ...pr, created: true };
        }),
      };
    });
}
