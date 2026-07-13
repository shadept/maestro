import { execFileSync } from "node:child_process";
import { appendFileSync, writeFileSync } from "node:fs";
import { mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { BranchDivergedError, type Project, type Session, type TaskContext } from "@maestro/domain";
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

const publishWithProposal = (
  session: Session,
  project: Project,
  context: TaskContext,
  proposal: { title: string; body: string },
) =>
  run(
    Effect.gen(function* () {
      const outbound = yield* OutboundGit;
      return yield* outbound.publish({ session, project, context, proposal });
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

/**
 * Simulates a human touching the SESSION branch on the forge (e.g. GitHub's
 * "Update branch" button): mutates the branch in a temporary origin worktree
 * so origin's tip moves without the orchestrator's involvement.
 */
const humanUpdatesOriginBranch = (branch: string, mutate: (dir: string) => void) => {
  const tmp = path.join(root, `human-${branch.replaceAll("/", "-")}-${Date.now()}`);
  git(originDir, "worktree", "add", tmp, branch);
  mutate(tmp);
  git(originDir, "worktree", "remove", tmp);
};

describe("OutboundGit.publish", () => {
  it("first publish pushes and opens a ready (non-draft) PR; second updates; then no-ops", async () => {
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
    expect(createCall?.args.draft).toBe(false);
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

  it("respects project git conventions: base branch and draft-PR override", async () => {
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
          gitConventions: { baseBranch: branch, draftPr: true },
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
    expect(call?.args.draft).toBe(true);
  });

  it("agent proposal drives PR title and body; footer keeps the ticket reference", async () => {
    const { project, session, paths } = await setupSession("FUR-205");
    commit(paths.worktreePath, "work.txt", "turn one");
    const callsBefore = forgeCalls.length;
    const outcome = await publishWithProposal(session, project, taskContext("FUR-205", "Ignored"), {
      title: "Teach the sweeper to sweep",
      body: "## What\nSweeping.\n\n## Why\nDust.",
    });
    expect(outcome._tag).toBe("Published");
    const call = forgeCalls[callsBefore];
    expect(call?.args.title).toBe("FUR-205: Teach the sweeper to sweep");
    expect(call?.args.body).toContain("## What\nSweeping.");
    expect(call?.args.body).toContain("Ticket: FUR-205");
    expect(call?.args.body).toContain(`Maestro session: ${session.id}`);
  });

  it("proposal title already carrying the ticket id is not double-prefixed", async () => {
    const { project, session, paths } = await setupSession("FUR-206");
    commit(paths.worktreePath, "work.txt", "turn one");
    const callsBefore = forgeCalls.length;
    await publishWithProposal(session, project, taskContext("FUR-206", "Ignored"), {
      title: "FUR-206: self-titled",
      body: "",
    });
    expect(forgeCalls[callsBefore]?.args.title).toBe("FUR-206: self-titled");
  });

  it("diverged remote (update-branch merge commit) reconciles: merge, push, PR update", async () => {
    const { project, session, paths } = await setupSession("FUR-207");
    commit(paths.worktreePath, "work.txt", "turn one");
    const first = await publish(session, project, taskContext("FUR-207", "Diverge me"));
    expect(first._tag).toBe("Published");

    // Human presses GitHub's "Update branch": main gains a commit, and the
    // session branch gains a merge commit the orchestrator has never seen.
    await writeFile(path.join(originDir, "upstream.txt"), "upstream change\n");
    git(originDir, "add", ".");
    git(originDir, "commit", "-m", "upstream work on main");
    humanUpdatesOriginBranch(session.gitBranch, (dir) => {
      git(dir, "merge", "main", "-m", `Merge branch 'main' into ${session.gitBranch}`);
    });
    const humanTip = git(originDir, "rev-parse", `refs/heads/${session.gitBranch}`);

    // Meanwhile the agent committed again locally — histories have diverged.
    commit(paths.worktreePath, "work.txt", "turn two");

    const callsBefore = forgeCalls.length;
    const afterFirst = await freshSession(session);
    const outcome = await publish(afterFirst, project, taskContext("FUR-207", null));
    expect(outcome._tag).toBe("Published");
    if (outcome._tag !== "Published") return;
    expect(outcome.prCreated).toBe(false);

    // origin now carries the local tip, which is a Maestro-authored merge
    // commit joining the agent's turn-two commit and the human's merge commit
    const localTip = git(paths.worktreePath, "rev-parse", "HEAD");
    expect(git(originDir, "rev-parse", `refs/heads/${session.gitBranch}`)).toBe(localTip);
    expect(git(paths.worktreePath, "log", "-1", "--format=%an <%ae>")).toBe(
      "Maestro <maestro@localhost>",
    );
    const parents = git(paths.worktreePath, "log", "-1", "--format=%P").split(" ");
    expect(parents).toHaveLength(2);
    expect(parents).toContain(humanTip);
    // both sides' content survived the merge
    expect(git(paths.worktreePath, "status", "--porcelain")).toBe("");
    await expect(
      readFile(path.join(paths.worktreePath, "upstream.txt"), "utf8"),
    ).resolves.toContain("upstream change");

    // PR updated as on any other push
    expect(forgeCalls.length).toBe(callsBefore + 1);
    expect(forgeCalls[callsBefore]?.op).toBe("update");
  });

  it("diverged remote with conflicts: merge aborted, worktree pristine, actionable error", async () => {
    const { project, session, paths } = await setupSession("FUR-208");
    commit(paths.worktreePath, "work.txt", "turn one");
    const first = await publish(session, project, taskContext("FUR-208", "Conflict me"));
    expect(first._tag).toBe("Published");

    // Human edits the same line on the remote session branch...
    humanUpdatesOriginBranch(session.gitBranch, (dir) => {
      writeFileSync(path.join(dir, "work.txt"), "human version\n");
      git(dir, "add", ".");
      git(dir, "commit", "-m", "human hotfix on the PR branch");
    });
    const remoteTip = git(originDir, "rev-parse", `refs/heads/${session.gitBranch}`);

    // ...while the agent rewrites it locally — the merge must conflict.
    writeFileSync(path.join(paths.worktreePath, "work.txt"), "agent version\n");
    git(paths.worktreePath, "add", ".");
    git(
      paths.worktreePath,
      "-c",
      "user.email=agent@test",
      "-c",
      "user.name=Agent",
      "commit",
      "-m",
      "turn two",
    );
    const localTip = git(paths.worktreePath, "rev-parse", "HEAD");

    const callsBefore = forgeCalls.length;
    const afterFirst = await freshSession(session);
    const error = await run(
      Effect.gen(function* () {
        const outbound = yield* OutboundGit;
        return yield* outbound
          .publish({ session: afterFirst, project, context: taskContext("FUR-208", null) })
          .pipe(Effect.flip);
      }),
    );

    // purposeful error: the String() rendering IS the turn-failed summary tail
    expect(error).toBeInstanceOf(BranchDivergedError);
    expect(String(error)).toContain("@maestro");
    expect(String(error)).toContain(`origin/${session.gitBranch}`);
    expect(String(error)).toContain("resolve the conflicts");

    // merge aborted: worktree pristine on the local tip, no merge in progress
    expect(git(paths.worktreePath, "rev-parse", "HEAD")).toBe(localTip);
    expect(git(paths.worktreePath, "status", "--porcelain")).toBe("");
    expect(() => git(paths.worktreePath, "rev-parse", "--verify", "MERGE_HEAD")).toThrow();

    // remote untouched (never force-pushed), no forge traffic, PR record kept
    expect(git(originDir, "rev-parse", `refs/heads/${session.gitBranch}`)).toBe(remoteTip);
    expect(forgeCalls.length).toBe(callsBefore);
    const after = await freshSession(session);
    expect(after.prNumber).toBe(afterFirst.prNumber);
  });
});
