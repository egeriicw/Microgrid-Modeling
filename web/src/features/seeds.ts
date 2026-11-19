import type { SeedKey } from "../api/types";

/** Plain names for the three engine seeds (the technical name stays visible as secondary text). */
export const SEED_INFO: Record<SeedKey, { label: string; explanation: string }> = {
  composition_seed: { label: "Building mix seed", explanation: "Which mix of building types is drawn." },
  archetype_seed: { label: "Building pick seed", explanation: "Which individual buildings are picked." },
  perturbation_seed: { label: "Variation seed", explanation: "Small random variations applied to each building." },
};
