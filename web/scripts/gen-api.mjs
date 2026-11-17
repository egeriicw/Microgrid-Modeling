// Generates src/api/generated/openapi.ts from the API contract (../api/openapi.json).
//   node scripts/gen-api.mjs          write the file
//   node scripts/gen-api.mjs --check  exit 1 if the committed file is out of date (used by a unit test)
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import openapiTS, { astToString } from "openapi-typescript";

const here = dirname(fileURLToPath(import.meta.url));
const SPEC = resolve(here, "../../api/openapi.json");
const OUT = resolve(here, "../src/api/generated/openapi.ts");
const HEADER =
  "// GENERATED from api/openapi.json by `npm run gen:api` (openapi-typescript). Do not edit by hand.\n" +
  "// `npm run check:api` (part of `npm test`) fails when this file is out of date with the contract.\n";

const spec = JSON.parse(readFileSync(SPEC, "utf8"));
const ast = await openapiTS(spec, { alphabetize: true });
const text = HEADER + astToString(ast);

if (process.argv.includes("--check")) {
  let current = "";
  try {
    current = readFileSync(OUT, "utf8");
  } catch {
    /* a missing file counts as out of date */
  }
  if (current !== text) {
    console.error("src/api/generated/openapi.ts is out of date with api/openapi.json. Run `npm run gen:api`.");
    process.exit(1);
  }
  console.log("generated API types are up to date");
} else {
  mkdirSync(dirname(OUT), { recursive: true });
  writeFileSync(OUT, text);
  console.log(`wrote ${OUT}`);
}
