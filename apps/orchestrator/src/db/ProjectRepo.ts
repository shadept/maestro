import {
  type DbError,
  EntityNotFoundError,
  type GitConventionOverrides,
  Project,
  type ProjectId,
  type ResourceTiers,
} from "@maestro/domain";
import { eq } from "drizzle-orm";
import { Context, Effect, Layer, Schema } from "effect";
import { Db } from "./Db.ts";
import { projects } from "./schema/index.ts";
import { dbTry } from "./support.ts";

const decode = Schema.decodeUnknownSync(Project);
const toProject = (row: typeof projects.$inferSelect): Project => decode(row);

export interface ProjectCreate {
  readonly repoGitUrl: string;
  readonly gitConventions?: GitConventionOverrides;
  readonly resources?: ResourceTiers;
}

export class ProjectRepo extends Context.Service<
  ProjectRepo,
  {
    readonly create: (input: ProjectCreate) => Effect.Effect<Project, DbError>;
    readonly get: (id: ProjectId) => Effect.Effect<Project, DbError>;
    readonly list: Effect.Effect<ReadonlyArray<Project>, DbError>;
    readonly setLocalCachePath: (id: ProjectId, path: string) => Effect.Effect<Project, DbError>;
  }
>()("maestro/db/ProjectRepo") {
  static readonly layer = Layer.effect(
    ProjectRepo,
    Effect.gen(function* () {
      const { client } = yield* Db;
      return {
        create: Effect.fn("ProjectRepo.create")(function* (input: ProjectCreate) {
          const rows = yield* dbTry("ProjectRepo.create")(() =>
            client
              .insert(projects)
              .values({
                repoGitUrl: input.repoGitUrl,
                gitConventions: input.gitConventions ?? {},
                resources: input.resources ?? {},
              })
              .returning(),
          );
          // biome-ignore lint/style/noNonNullAssertion: insert returning always yields one row
          return toProject(rows[0]!);
        }),
        get: Effect.fn("ProjectRepo.get")(function* (id: ProjectId) {
          const rows = yield* dbTry("ProjectRepo.get")(() =>
            client.select().from(projects).where(eq(projects.id, id)),
          );
          const row = rows[0];
          if (!row) {
            return yield* new EntityNotFoundError({ entity: "Project", entityId: id });
          }
          return toProject(row);
        }),
        list: Effect.fn("ProjectRepo.list")(function* () {
          const rows = yield* dbTry("ProjectRepo.list")(() => client.select().from(projects));
          return rows.map(toProject);
        })(),
        setLocalCachePath: Effect.fn("ProjectRepo.setLocalCachePath")(function* (
          id: ProjectId,
          path: string,
        ) {
          const rows = yield* dbTry("ProjectRepo.setLocalCachePath")(() =>
            client
              .update(projects)
              .set({ localCachePath: path })
              .where(eq(projects.id, id))
              .returning(),
          );
          const row = rows[0];
          if (!row) {
            return yield* new EntityNotFoundError({ entity: "Project", entityId: id });
          }
          return toProject(row);
        }),
      };
    }),
  );
}
