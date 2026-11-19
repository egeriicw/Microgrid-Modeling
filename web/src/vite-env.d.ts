/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** "mock" selects the fixture client; anything else (or unset) the real API. */
  readonly VITE_API_MODE?: string;
  /** Base URL of the real API; default "/api/v1" (same origin). */
  readonly VITE_API_BASE_URL?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
