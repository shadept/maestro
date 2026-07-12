import { AuditLog, type DbError } from "@maestro/domain";
import { desc } from "drizzle-orm";
import { Context, Effect, Layer, Schema } from "effect";
import { Db } from "./Db.ts";
import { auditLogs } from "./schema/index.ts";
import { dbTry } from "./support.ts";

const decode = Schema.decodeUnknownSync(AuditLog);
const toAuditLog = (row: typeof auditLogs.$inferSelect): AuditLog => decode(row);

export interface AuditRecord {
  readonly actor: string;
  readonly action: string;
  readonly targetEntity: string;
  readonly priorState?: string;
}

export class AuditRepo extends Context.Service<
  AuditRepo,
  {
    readonly record: (input: AuditRecord) => Effect.Effect<AuditLog, DbError>;
    readonly list: Effect.Effect<ReadonlyArray<AuditLog>, DbError>;
  }
>()("maestro/db/AuditRepo") {
  static readonly layer = Layer.effect(
    AuditRepo,
    Effect.gen(function* () {
      const { client } = yield* Db;
      return {
        record: Effect.fn("AuditRepo.record")(function* (input: AuditRecord) {
          const rows = yield* dbTry("AuditRepo.record")(() =>
            client
              .insert(auditLogs)
              .values({
                actor: input.actor,
                action: input.action,
                targetEntity: input.targetEntity,
                priorState: input.priorState ?? null,
              })
              .returning(),
          );
          // biome-ignore lint/style/noNonNullAssertion: insert returning always yields one row
          return toAuditLog(rows[0]!);
        }),
        list: Effect.fn("AuditRepo.list")(function* () {
          const rows = yield* dbTry("AuditRepo.list")(() =>
            client.select().from(auditLogs).orderBy(desc(auditLogs.createdAt)),
          );
          return rows.map(toAuditLog);
        })(),
      };
    }),
  );
}
