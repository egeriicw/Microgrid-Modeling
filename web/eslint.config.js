import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";

export default tseslint.config(
  // src/api/generated/ is written by `npm run gen:api` from api/openapi.json and never edited by hand.
  { ignores: ["dist", "node_modules", "spikes", "playwright-report", "test-results", "src/api/generated"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["**/*.{ts,tsx}"],
    languageOptions: { globals: { ...globals.browser } },
    plugins: { "react-hooks": reactHooks },
    rules: {
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "warn",
      // All HTTP lives in src/api/ (httpClient.ts).
      "no-restricted-globals": ["error", { name: "fetch", message: "Use the ApiClient (src/api/); HTTP stays in one module." }],
      "no-restricted-properties": [
        "error",
        { object: "window", property: "fetch", message: "Use the ApiClient (src/api/); HTTP stays in one module." },
        { object: "globalThis", property: "fetch", message: "Use the ApiClient (src/api/); HTTP stays in one module." },
        { object: "self", property: "fetch", message: "Use the ApiClient (src/api/); HTTP stays in one module." },
      ],
    },
  },
  { files: ["src/api/**", "src/**/*.test.{ts,tsx}"], rules: { "no-restricted-globals": "off", "no-restricted-properties": "off" } },
  { files: ["scripts/**/*.mjs"], languageOptions: { globals: { ...globals.node } } },
);
