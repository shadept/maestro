import { execFileSync } from "node:child_process";
import { appendFileSync } from "node:fs";
import { mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import type { Project, Session, TaskContext } from "@maestro/domain";
import { Effect, Layer, Option, Redacted } from "effect";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { AppConfig } from "../../src/config/AppConfig.ts";
import { Db } from "../../src/db/Db.ts";
import { ProjectRepo } from "../../src/db/ProjectRepo.ts";
import { SessionRepo } from "../../src/db/SessionRepo.ts";
import { EventBus } from "../../src/events/EventBus.ts";
import { type ForgeCall, GitHubForge } from "../../src/forge/GitHubForge.ts";
import { GitCache } from "../../src/git/GitCache.ts";
import { OutboundGit } from "../../src/git/OutboundGit.ts";
import { RepoLocks } from "../../src/git/RepoLocks.ts";
import { branchNameFor, WorktreeManager } from "../../src/git/WorktreeManager.ts";
import { startTestDb, type TestDb } from "../support/pg.ts";

// OutboundGit integration: real git against a LOCAL origin (never a network
// remote) + real Postgres for the session PR record + the fake forge layer.

type Services = ProjectRepo | SessionRepo | GitCache | WorktreeManager | OutboundGit;

const git = (cwd: string, ...args: string[]): string =>
  execFileSync("git", args, { cwd, encoding: "utf8" }).trimEnd();

let testDb: TestDb;
let root: string;
let originDir: string;
let layer: Layer.Layer<Services>;
const forgeCalls: ForgeCall[] = [];

beforeAll(async () => {
  testDb = await startTestDb();
  root = await realpath(await mkdtemp(path.join(tmpdir(), "maestro-outbound-")));

  originDir = path.join(root, "origin");
  execFileSync("git", ["init", "-b", "main", originDir]);
  git(originDir, "config", "user.email", "fixture@test");
  git(originDir, "config", "user.name", "Fixture");
  await writeFile(path.join(originDir, "README.md"), "hello\n");
  git(originDir, "add", ".");
  git(originDir, "commit", "-m", "initial");

  layer = Layer.mergeAll(
    GitCache.layer,
    WorktreeManager.layer,
    OutboundGit.layer,
    ProjectRepo.layer,
    SessionRepo.layer,
  ).pipe(
    Layer.provideMerge(Layer.mergeAll(GitCache.layer, SessionRepo.layer)),
    Layer.provide(Layer.mergeAll(RepoLocks.layer, GitHubForge.layerTest({ calls: forgeCalls }))),
    Layer.provide(Db.layerTest(testDb.connectionString)),
    Layer.provide(EventBus.layer),
    Layer.provide(
      AppConfig.layerTest({
        databaseUrl: testDb.connectionString,
        storageRoot: path.join(root, "storage"),
        // a file:// origin ignores credentials, but they must never persist
        githubToken: Option.some(Redacted.make("SUPER-SECRET-TOKEN")),
      }),
    ),
    Layer.orDie,
  );
});

afterAll(async () => {
  await rm(root, { recursive: true, force: true });
  await testDb.stop();
});

const run = <A, E>(effect: Effect.Effect<A, E, Services>): Promise<A> =>
  Effect.runPromise(Effect.provide(effect, layer));

const taskContext = (ticketKey: string, title: string | null): TaskContext => ({
  source: "linear",
  ticket: { source: "linear", externalId: ticketKey },
  actor: "shade",
  title,
  body: "please",
  deliveryId: `d-${ticketKey}-${Math.random()}`,
  payload: {},
});

/** Project + session + provisioned worktree, ready for commits. */
const setupSession = (ticketKey: string) =>
  run(
    Effect.gen(function* () {
      const projectRepo = yield* ProjectRepo;
      const sessionRepo = yield* SessionRepo;
      const cache = yield* GitCache;
      const manager = yield* WorktreeManager;
      const project = yield* projectRepo.create({ repoGitUrl: `file://${originDir}` });
      const session = yield* sessionRepo.create({
        projectId: project.id,
        ticketReference: { source: "linear", externalId: ticketKey },
        gitBranch: branchNameFor({ source: "linear", externalId: ticketKey }, project),
      });
      yield* cache.ensureClone(project);
      const paths = yield* manager.provision({ session, project });
      return { project, session, paths };
    }),
  );

const publish = (session: Session, project: Project, context: TaskContext) =>
  run(
    Effect.gen(function* () {
      const outbound = yield* OutboundGit;
      return yield* outbound.publish({ session, project, context });
    }),
  );

const freshSession = (session: Session) =>
  run(
    Effect.gen(function* () {
      const repo = yield* SessionRepo;
      return yield* repo.get(session.id);
    }),
  );

const commit = (worktree: string, file: string, message: string) => {
  appendFileSync(path.join(worktree, file), "work\n");
  git(worktree, "add", ".");
  git(worktree, "-c", "user.email=agent@test", "-c", "user.name=Agent", "commit", "-m", message);
};

describe("OutboundGit.publish", () => {
  it("first publish pushes and opens a draft PR; second updates; then no-ops", async () => {
    const { project, session, paths } = await setupSession("FUR-201");
    const callsBefore = forgeCalls.length;

    // ── first publish: new commits → push creates the branch + draft PR ────
    commit(paths.worktreePath, "work.txt", "turn one");
    const first = await publish(session, project, taskContext("FUR-201", "Do the work"));
    expect(first._tag).toBe("Published");
    if (first._tag !== "Published") return;
    expect(first.prCreated).toBe(true);

    // branch exists on origin at the worktree's head
    expect(git(originDir, "rev-parse", `refs/heads/${session.gitBranch}`)).toBe(
      git(paths.worktreePath, "rev-parse", "HEAD"),
    );

    // draft PR opened with ticket-derived title against the base branch
    expect(forgeCalls.length).toBe(callsBefore + 1);
    const createCall = forgeCalls[callsBefore];
    expect(createCall?.op).toBe("create");
    expect(createCall?.args.draft).toBe(true);
    expect(createCall?.args.headBranch).toBe(session.gitBranch);
    expect(createCall?.args.baseBranch).toBe("main");
    expect(createCall?.args.title).toBe("FUR-201: Do the work");
    expect(createCall?.args.body).toContain("FUR-201");
    expect(createCall?.args.existingNumber).toBeNull();

    // PR persisted on the session
    const afterFirst = await freshSession(session);
    expect(afterFirst.prNumber).toBe(first.prNumber);
    expect(afterFirst.prUrl).toBe(first.prUrl);

    // ── second publish: more commits → push updates branch + existing PR ───
    commit(paths.worktreePath, "work.txt", "turn two");
    const second = await publish(afterFirst, project, taskContext("FUR-201", null));
    expect(second._tag).toBe("Published");
    if (second._tag !== "Published") return;
    expect(second.prCreated).toBe(false);
    expect(second.prNumber).toBe(first.prNumber);
    expect(second.prUrl).toBe(first.prUrl);

    expect(git(originDir, "rev-parse", `refs/heads/${session.gitBranch}`)).toBe(
      git(paths.worktreePath, "rev-parse", "HEAD"),
    );
    expect(forgeCalls.length).toBe(callsBefore + 2);
    expect(forgeCalls[callsBefore + 1]?.op).toBe("update");
    expect(forgeCalls[callsBefore + 1]?.args.existingNumber).toBe(first.prNumber);

    // ── third publish: nothing new → no push, no forge traffic ─────────────
    const afterSecond = await freshSession(session);
    const third = await publish(afterSecond, project, taskContext("FUR-201", null));
    expect(third._tag).toBe("NothingToPublish");
    expect(forgeCalls.length).toBe(callsBefore + 2);

    // credentials were offered on every remote operation but never stored
    const config = await readFile(path.join(paths.gitDir, "config"), "utf8");
    expect(config).not.toContain("SUPER-SECRET-TOKEN");
    expect(config.toLowerCase()).not.toContain("authorization");
  });

  it("no-commit turn publishes nothing: no branch on origin, no PR", async () => {
    const { project, session } = await setupSession("FUR-202");
    const callsBefore = forgeCalls.length;

    const outcome = await publish(session, project, taskContext("FUR-202", "Question only"));
    expect(outcome).toEqual({ _tag: "NothingToPublish" });

    expect(git(originDir, "branch", "--list", session.gitBranch)).toBe("");
    expect(forgeCalls.length).toBe(callsBefore);
    const after = await freshSession(session);
    expect(after.prNumber).toBeNull();
    expect(after.prUrl).toBeNull();
  });

  it("publish replays converge: pushed branch without a PR record heals on retry", async () => {
    // Simulates the crash window between push and PR persistence: the branch
    // is already on origin, the session has no PR — publish must still ensure
    // the PR instead of skipping (remote == local but prNumber is null).
    const { project, session, paths } = await setupSession("FUR-203");
    commit(paths.worktreePath, "work.txt", "turn one");
    git(
      paths.gitDir,
      "push",
      "origin",
      `refs/heads/${session.gitBranch}:refs/heads/${session.gitBranch}`,
    );

    const callsBefore = forgeCalls.length;
    const outcome = await publish(session, project, taskContext("FUR-203", "Heal me"));
    expect(outcome._tag).toBe("Published");
    if (outcome._tag !== "Published") return;
    expect(outcome.prCreated).toBe(true);
    expect(forgeCalls.length).toBe(callsBefore + 1);

    const after = await freshSession(session);
    expect(after.prNumber).toBe(outcome.prNumber);
  });

  it("respects project git conventions: base branch and non-draft PRs", async () => {
    const branch = "develop";
    git(originDir, "branch", branch);
    const { project, session, paths } = await run(
      Effect.gen(function* () {
        const projectRepo = yield* ProjectRepo;
        const sessionRepo = yield* SessionRepo;
        const cache = yield* GitCache;
        const manager = yield* WorktreeManager;
        const project = yield* projectRepo.create({
          repoGitUrl: `file://${originDir}`,
          gitConventions: { baseBranch: branch, draftPr: false },
        });
        const session = yield* sessionRepo.create({
          projectId: project.id,
          ticketReference: { source: "linear", externalId: "FUR-204" },
          gitBranch: branchNameFor({ source: "linear", externalId: "FUR-204" }, project),
        });
        yield* cache.ensureClone(project);
        const paths = yield* manager.provision({ session, project });
        return { project, session, paths };
      }),
    );
    commit(paths.worktreePath, "work.txt", "turn one");

    const callsBefore = forgeCalls.length;
    const outcome = await publish(session, project, taskContext("FUR-204", "Custom conventions"));
    expect(outcome._tag).toBe("Published");
    const call = forgeCalls[callsBefore];
    expect(call?.args.baseBranch).toBe(branch);
    expect(call?.args.draft).toBe(false);
  });
});
