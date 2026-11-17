// @vitest-environment node
// QA review of W1-WEB: the staleness guard for the generated API types really fails when the contract changes.
// Runs the real scripts/gen-api.mjs in a scratch copy of the repo layout (web/scripts, web/src/api/generated,
// api/openapi.json) so the committed files are never touched.
import { afterAll, describe, expect, it } from "vitest";

// The web tsconfig has no Node types (@types/node is not a dependency), so the Node built-ins are loaded with
// minimal local typings instead of static imports.
type Json = { components: { schemas: { RunOut: { properties: Record<string, unknown> } } } };
interface NodeFs {
  cpSync(a: string, b: string): void;
  mkdirSync(p: string, o: { recursive: boolean }): void;
  mkdtempSync(prefix: string): string;
  readFileSync(p: string, enc: "utf8"): string;
  rmSync(p: string, o: { recursive: boolean; force: boolean }): void;
  symlinkSync(target: string, path: string, type: "dir"): void;
  writeFileSync(p: string, data: string): void;
}
interface NodePath {
  join(...p: string[]): string;
  resolve(...p: string[]): string;
}
const nodeImport = (name: string) => import(/* @vite-ignore */ `node:${name}`);
const { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } = (await nodeImport("fs")) as NodeFs;
const { tmpdir } = (await nodeImport("os")) as { tmpdir(): string };
const { join, resolve } = (await nodeImport("path")) as NodePath;
const { fileURLToPath } = (await nodeImport("url")) as { fileURLToPath(u: string): string };
const { spawnSync } = (await nodeImport("child_process")) as {
  spawnSync(cmd: string, args: string[], o: { encoding: "utf8" }): { status: number | null; stderr: string };
};
const proc = (globalThis as unknown as { process: { execPath: string } }).process;

const WEB = resolve(fileURLToPath(import.meta.url), "../../..");
const SPEC = resolve(WEB, "../api/openapi.json");
const roots: string[] = [];

function layout(mutateSpec?: (spec: Json) => void): string {
  const root = mkdtempSync(join(tmpdir(), "qa-genapi-"));
  roots.push(root);
  mkdirSync(join(root, "web/scripts"), { recursive: true });
  mkdirSync(join(root, "web/src/api/generated"), { recursive: true });
  mkdirSync(join(root, "api"), { recursive: true });
  cpSync(join(WEB, "scripts/gen-api.mjs"), join(root, "web/scripts/gen-api.mjs"));
  cpSync(join(WEB, "src/api/generated/openapi.ts"), join(root, "web/src/api/generated/openapi.ts"));
  symlinkSync(join(WEB, "node_modules"), join(root, "web/node_modules"), "dir");
  const spec = JSON.parse(readFileSync(SPEC, "utf8")) as Json;
  mutateSpec?.(spec);
  writeFileSync(join(root, "api/openapi.json"), JSON.stringify(spec, null, 2));
  return root;
}

const check = (root: string) => spawnSync(proc.execPath, [join(root, "web/scripts/gen-api.mjs"), "--check"], { encoding: "utf8" });

afterAll(() => {
  for (const r of roots) rmSync(r, { recursive: true, force: true });
});

describe("QA W1-WEB: `npm run check:api` staleness guard", () => {
  it("passes when the generated file matches api/openapi.json", () => {
    expect(check(layout()).status).toBe(0);
  }, 30_000);

  it("fails when the API adds a field", () => {
    const r = check(
      layout((s) => {
        s.components.schemas.RunOut.properties.qa_new_field = { type: "string", title: "Qa New Field" };
      }),
    );
    expect(r.status).toBe(1);
    expect(r.stderr).toMatch(/out of date/);
  }, 30_000);

  it("fails when a field becomes nullable (a semantic change with the same field names)", () => {
    const r = check(
      layout((s) => {
        s.components.schemas.RunOut.properties.engine_version = { anyOf: [{ type: "string" }, { type: "null" }], title: "Engine Version" };
      }),
    );
    expect(r.status).toBe(1);
  }, 30_000);
});
