import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";

export default tseslint.config(
  { ignores: ["dist", "node_modules", "spikes", "playwright-report", "test-results"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  {
    files: ["**/*.{ts,tsx}"],
    languageOptions: { globals: { ...globals.browser } },
    plugins: { "react-hooks": reactHooks },
    rules: {
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "warn",
      // All HTTP lives in src/api/. The real generated client replaces src/api/mockClient.ts later.
      "no-restricted-globals": ["error", { name: "fetch", message: "Use the ApiClient (src/api/); HTTP stays in one module." }],
    },
  },
  { files: ["src/api/**", "src/**/*.test.{ts,tsx}"], rules: { "no-restricted-globals": "off" } },
);
