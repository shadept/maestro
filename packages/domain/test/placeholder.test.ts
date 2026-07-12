import { Effect } from "effect";
import { expect, it } from "vitest";

import { placeholder } from "../src/index.ts";

it("workspace harness runs", async () => {
  expect(await Effect.runPromise(placeholder)).toBe("@maestro/domain");
});
