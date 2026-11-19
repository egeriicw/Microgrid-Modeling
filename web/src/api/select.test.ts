import { describe, expect, it } from "vitest";
import { HttpApiClient } from "./httpClient";
import { createClientFromEnv, resolveApiConfig } from "./select";

describe("client selection (contract with the deploy story)", () => {
  it("uses the real client at /api/v1 when nothing is set", () => {
    expect(resolveApiConfig({})).toEqual({ mode: "http", baseUrl: "/api/v1" });
    expect(createClientFromEnv({})).toBeInstanceOf(HttpApiClient);
  });

  it("uses the mock only for VITE_API_MODE=mock", () => {
    expect(resolveApiConfig({ VITE_API_MODE: "mock" }).mode).toBe("mock");
    expect(createClientFromEnv({ VITE_API_MODE: "mock" }).kind).toBe("mock");
    for (const v of ["", "http", "MOCK ", "real"]) expect(resolveApiConfig({ VITE_API_MODE: v }).mode).toBe("http");
  });

  it("honours VITE_API_BASE_URL and treats an empty value as unset", () => {
    expect(resolveApiConfig({ VITE_API_BASE_URL: "https://api.example.test/api/v1" }).baseUrl).toBe("https://api.example.test/api/v1");
    expect(resolveApiConfig({ VITE_API_BASE_URL: "" }).baseUrl).toBe("/api/v1");
  });
});
