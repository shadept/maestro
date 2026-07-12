import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { NodeHttpServer } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { HttpClient, HttpRouter } from "effect/unstable/http";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { AppConfig } from "../../src/config/AppConfig.ts";
import { StaticRoutes } from "../../src/http/static.ts";

// FUR-17 acceptance: the orchestrator process itself serves the admin UI
// bundle — and stays bootable (plain 404s) when the bundle was never built.

const INDEX_HTML = "<!doctype html><title>Maestro</title><div id=root></div>";
const APP_JS = 'console.log("maestro admin ui");';

let distDir: string;

beforeAll(async () => {
  distDir = await mkdtemp(path.join(tmpdir(), "maestro-admin-ui-dist-"));
  await writeFile(path.join(distDir, "index.html"), INDEX_HTML);
  await mkdir(path.join(distDir, "assets"), { recursive: true });
  await writeFile(path.join(distDir, "assets", "app.js"), APP_JS);
});

afterAll(async () => {
  await rm(distDir, { recursive: true, force: true });
});

// Boots the real HTTP server with the static routes pointed at `root`.
const withServer = <A>(
  root: string,
  f: (client: HttpClient.HttpClient) => Effect.Effect<A, unknown, HttpClient.HttpClient>,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const client = yield* HttpClient.HttpClient;
      return yield* f(client);
    }).pipe(
      Effect.provide(
        HttpRouter.serve(StaticRoutes, { disableLogger: true, disableListenLog: true }).pipe(
          Layer.provideMerge(NodeHttpServer.layerTest),
          Layer.provide(AppConfig.layerTest({ adminUiDist: root })),
          Layer.orDie,
        ),
      ),
    ),
  );

describe("admin UI static serving", () => {
  it("serves index.html at /", async () => {
    const { status, contentType, body } = await withServer(distDir, (client) =>
      client.get("/").pipe(
        Effect.flatMap((response) =>
          Effect.map(response.text, (body) => ({
            status: response.status,
            contentType: response.headers["content-type"],
            body,
          })),
        ),
      ),
    );
    expect(status).toBe(200);
    expect(contentType).toContain("text/html");
    expect(body).toBe(INDEX_HTML);
  });

  it("serves bundle assets with their MIME type", async () => {
    const { status, contentType, body } = await withServer(distDir, (client) =>
      client.get("/assets/app.js").pipe(
        Effect.flatMap((response) =>
          Effect.map(response.text, (body) => ({
            status: response.status,
            contentType: response.headers["content-type"],
            body,
          })),
        ),
      ),
    );
    expect(status).toBe(200);
    expect(contentType).toContain("javascript");
    expect(body).toBe(APP_JS);
  });

  it("404s unknown paths (hash routing needs no SPA fallback)", async () => {
    const status = await withServer(distDir, (client) =>
      client.get("/definitely/not/a/file").pipe(Effect.map((response) => response.status)),
    );
    expect(status).toBe(404);
  });

  it("stays bootable when the bundle dir does not exist", async () => {
    const status = await withServer(path.join(distDir, "never-built"), (client) =>
      client.get("/").pipe(Effect.map((response) => response.status)),
    );
    expect(status).toBe(404);
  });
});
