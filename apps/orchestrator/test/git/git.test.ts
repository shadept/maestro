import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { Project, Session } from "@maestro/domain";
import { Effect, Layer, Redacted, Schema } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { AppConfig } from "../../src/config/AppConfig.ts";
import { GitCache } from "../../src/git/GitCache.ts";
import { RepoLocks } from "../../src/git/RepoLocks.ts";
import { branchNameFor, WorktreeManager } from "../../src/git/WorktreeManager.ts";

const git = (cwd: string, ...args: string[]): string =>
  execFileSync("git", args, { cwd, encoding: "utf8" }).trimEnd();

let root: string;
let originDir: string;
let layer: Layer.Layer<GitCache | WorktreeManager>;

const decodeProject = Schema.decodeUnknownSync(Project);
const decodeSession = Schema.decodeUnknownSync(Session);

const uuid = (n: number) => `00000000-0000-4000-8000-${n.toString().padStart(12, "0")}`;

let project: Project;

const makeSession = (n: number, branch: string): Session =>
  decodeSession({
    id: uuid(100 + n),
    projectId: project.id,
    ticketReference: { source: "linear", externalId: `FUR-${n}` },
    gitBranch: branch,
    claudeSessionUuid: null,
    prNumber: null,
    prUrl: null,
    terminationRequestedAt: null,
    state: "WARM_IDLE",
    createdAt: new Date(),
    lastActivityAt: new Date(),
  });

beforeAll(async () => {
  root = await mkdtemp(path.join(tmpdir(), "maestro-git-"));
  // fixture origin repo with one commit on main
  originDir = path.join(root, "origin");
  execFileSync("git", ["init", "-b", "main", originDir]);
  git(originDir, "config", "user.email", "fixture@test");
  git(originDir, "config", "user.name", "Fixture");
  await writeFile(path.join(originDir, "README.md"), "hello\n");
  git(originDir, "add", ".");
  git(originDir, "commit", "-m", "initial");

  project = decodeProject({
    id: uuid(1),
    repoGitUrl: `file://${originDir}`,
    linearTeamKey: null,
    localCachePath: null,
    gitConventions: {},
    resources: {},
    createdAt: new Date(),
  });

  layer = Layer.mergeAll(GitCache.layer, WorktreeManager.layer).pipe(
    Layer.provideMerge(GitCache.layer),
    // single RepoLocks reference — memoized, so both services share one lock map
    Layer.provide(RepoLocks.layer),
    Layer.provide(
      AppConfig.layerTest({
        databaseUrl: "postgresql://unused:5432/x",
        storageRoot: path.join(root, "storage"),
      }),
    ),
    Layer.orDie,
  );
});

afterAll(async () => {
  await rm(root, { recursive: true, force: true });
});

const run = <A, E>(effect: Effect.Effect<A, E, GitCache | WorktreeManager>): Promise<A> =>
  Effect.runPromise(Effect.provide(effect, layer));

describe("GitCache + WorktreeManager", () => {
  it("full lifecycle: clone → provision two sessions → commit → remove", async () => {
    const credentials = { token: Redacted.make("SUPER-SECRET-TOKEN") };

    // clone (with credentials offered — file:// ignores them, but they must not persist)
    const cachePath = await run(
      Effect.gen(function* () {
        const cache = yield* GitCache;
        return yield* cache.ensureClone(project, credentials);
      }),
    );
    expect(git(cachePath, "rev-parse", "--is-bare-repository")).toBe("true");

    // idempotent
    const again = await run(
      Effect.gen(function* () {
        const cache = yield* GitCache;
        return yield* cache.ensureClone(project);
      }),
    );
    expect(again).toBe(cachePath);

    const sessionA = makeSession(
      1,
      branchNameFor({ source: "linear", externalId: "FUR-1" }, project),
    );
    const sessionB = makeSession(
      2,
      branchNameFor({ source: "linear", externalId: "FUR-2" }, project),
    );
    expect(sessionA.gitBranch).toBe("maestro/FUR-1");

    // concurrent provisioning of two sessions on the same repo
    const [pathsA, pathsB] = await run(
      Effect.gen(function* () {
        const manager = yield* WorktreeManager;
        return yield* Effect.all(
          [
            manager.provision({ session: sessionA, project }),
            manager.provision({ session: sessionB, project }),
          ],
          { concurrency: 2 },
        );
      }),
    );

    expect(await readFile(path.join(pathsA.worktreePath, "README.md"), "utf8")).toBe("hello\n");
    expect(pathsA.gitDir).toBe(cachePath);
    const worktrees = git(cachePath, "worktree", "list", "--porcelain");
    expect(worktrees).toContain(pathsA.worktreePath);
    expect(worktrees).toContain(pathsB.worktreePath);
    expect(git(pathsA.worktreePath, "rev-parse", "--abbrev-ref", "HEAD")).toBe("maestro/FUR-1");

    // a worker-style local commit works inside the worktree
    await writeFile(path.join(pathsA.worktreePath, "work.txt"), "done\n");
    git(pathsA.worktreePath, "add", ".");
    git(
      pathsA.worktreePath,
      "-c",
      "user.email=maestro@test",
      "-c",
      "user.name=Maestro",
      "commit",
      "-m",
      "work",
    );
    expect(git(pathsA.worktreePath, "log", "--oneline")).toContain("work");

    // provision is idempotent for an existing worktree (dormant rehydration)
    const rehydrated = await run(
      Effect.gen(function* () {
        const manager = yield* WorktreeManager;
        return yield* manager.provision({ session: sessionA, project });
      }),
    );
    expect(rehydrated.worktreePath).toBe(pathsA.worktreePath);

    // remove session A: worktree gone, branch gone, cache clean, B untouched
    await run(
      Effect.gen(function* () {
        const manager = yield* WorktreeManager;
        yield* manager.remove({ session: sessionA, project });
      }),
    );
    const afterRemove = git(cachePath, "worktree", "list", "--porcelain");
    expect(afterRemove).not.toContain(pathsA.worktreePath);
    expect(afterRemove).toContain(pathsB.worktreePath);
    expect(git(cachePath, "branch", "--list", "maestro/FUR-1")).toBe("");
    expect(git(cachePath, "branch", "--list", "maestro/FUR-2")).not.toBe("");

    // remove is idempotent
    await run(
      Effect.gen(function* () {
        const manager = yield* WorktreeManager;
        yield* manager.remove({ session: sessionA, project });
      }),
    );

    // no credentials at rest anywhere in the stored config
    const config = await readFile(path.join(cachePath, "config"), "utf8");
    expect(config).not.toContain("SUPER-SECRET-TOKEN");
    expect(config.toLowerCase()).not.toContain("authorization");
    expect(config).toContain(`file://${originDir}`);
  });

  it("fetch picks up new commits from origin", async () => {
    await writeFile(path.join(originDir, "update.txt"), "more\n");
    git(originDir, "add", ".");
    git(originDir, "commit", "-m", "update");
    const originHead = git(originDir, "rev-parse", "main");

    await run(
      Effect.gen(function* () {
        const cache = yield* GitCache;
        yield* cache.fetch(project, { token: Redacted.make("SUPER-SECRET-TOKEN") });
      }),
    );

    const cachePath = await run(
      Effect.gen(function* () {
        const cache = yield* GitCache;
        return yield* cache.ensureClone(project);
      }),
    );
    expect(git(cachePath, "rev-parse", "main")).toBe(originHead);
  });

  it("reports default branch and applies branch pattern overrides", async () => {
    const branch = await run(
      Effect.gen(function* () {
        const cache = yield* GitCache;
        return yield* cache.defaultBranch(project);
      }),
    );
    expect(branch).toBe("main");

    const custom = decodeProject({
      ...project,
      gitConventions: { branchPattern: "bot/{ticketKey}/work" },
    });
    expect(branchNameFor({ source: "linear", externalId: "FUR-9" }, custom)).toBe("bot/FUR-9/work");
  });

  it("failures carry stderr as GitCommandError", async () => {
    const bad = decodeProject({
      ...project,
      id: uuid(2),
      repoGitUrl: `file://${root}/does-not-exist`,
    });
    const error = await run(
      Effect.gen(function* () {
        const cache = yield* GitCache;
        return yield* cache.ensureClone(bad).pipe(Effect.flip);
      }),
    );
    expect(error._tag).toBe("GitCommandError");
    if (error._tag === "GitCommandError") {
      expect(error.stderr.length).toBeGreaterThan(0);
      expect(error.exitCode).not.toBe(0);
    }
  });
});
