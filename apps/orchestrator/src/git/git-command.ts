import { execFile } from "node:child_process";
import { GitCommandError } from "@maestro/domain";
import { Effect, Option, Redacted } from "effect";

export interface GitCredentials {
  /** Username paired with the token; defaults to "x-access-token" (GitHub PAT convention). */
  readonly username?: string;
  readonly token: Redacted.Redacted;
}

/**
 * Per-invocation credential injection via GIT_CONFIG_* environment variables
 * (git ≥ 2.31). Credentials never appear in argv (visible in process lists),
 * in error messages, or in any stored git config.
 */
const credentialEnv = (credentials: Option.Option<GitCredentials>): Record<string, string> =>
  Option.match(credentials, {
    onNone: () => ({}),
    onSome: (c) => {
      const basic = Buffer.from(
        `${c.username ?? "x-access-token"}:${Redacted.value(c.token)}`,
      ).toString("base64");
      return {
        GIT_CONFIG_COUNT: "1",
        GIT_CONFIG_KEY_0: "http.extraheader",
        GIT_CONFIG_VALUE_0: `Authorization: Basic ${basic}`,
      };
    },
  });

export interface GitCommandOptions {
  readonly cwd?: string;
  readonly credentials?: GitCredentials;
}

/** Effect-wrapped git executor. All failures carry the exit code and stderr. */
export const runGit = (
  args: ReadonlyArray<string>,
  options: GitCommandOptions = {},
): Effect.Effect<string, GitCommandError> =>
  Effect.callback<string, GitCommandError>((resume) => {
    const child = execFile(
      "git",
      args,
      {
        ...(options.cwd !== undefined && { cwd: options.cwd }),
        env: {
          ...process.env,
          // never prompt for credentials in a headless orchestrator
          GIT_TERMINAL_PROMPT: "0",
          ...credentialEnv(Option.fromNullishOr(options.credentials)),
        },
        maxBuffer: 16 * 1024 * 1024,
      },
      (error, stdout, stderr) => {
        if (error) {
          resume(
            Effect.fail(
              new GitCommandError({
                command: `git ${args.join(" ")}`,
                exitCode: typeof error.code === "number" ? error.code : null,
                stderr: String(stderr),
              }),
            ),
          );
        } else {
          resume(Effect.succeed(stdout.trimEnd()));
        }
      },
    );
    return Effect.sync(() => child.kill());
  });
