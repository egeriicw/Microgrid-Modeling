// How the API (Python) sees a composition name, so the form and the mock judge it the same way the server does
// (api/src/microgrid_api/composition_schemas.py): length in code points, `str.strip()` whitespace, and the characters
// it refuses. JavaScript differs on all three (`.length` counts UTF-16 units; `.trim()` also strips U+FEFF).

/** The API's limit on a composition name (composition_schemas.MAX_NAME), counted in code points. */
export const MAX_NAME = 120;

/** Python's str.isspace() code points (Unicode White_Space plus the C0 separators 0x1C-0x1F). U+FEFF is NOT one. */
const PY_SPACE = new Set([
  0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x85, 0xa0, 0x1680,
  0x2000, 0x2001, 0x2002, 0x2003, 0x2004, 0x2005, 0x2006, 0x2007, 0x2008, 0x2009, 0x200a,
  0x2028, 0x2029, 0x202f, 0x205f, 0x3000,
]);

/** Characters the API refuses in a name: C0/C1 controls, DEL, line/paragraph separators, bidi embeddings/isolates. */
const FORBIDDEN: [number, number][] = [
  [0x00, 0x1f],
  [0x7f, 0x9f],
  [0x2028, 0x2029],
  [0x202a, 0x202e],
  [0x2066, 0x2069],
];

const codePoints = (s: string): number[] => Array.from(s, (ch) => ch.codePointAt(0)!);

/** Length as Python's len(): code points, so an emoji counts once. */
export const pyLength = (s: string): number => codePoints(s).length;

/** True when Python's `s.strip()` would be empty. */
export const pyBlank = (s: string): boolean => codePoints(s).every((c) => PY_SPACE.has(c));

export const hasForbiddenNameChar = (s: string): boolean => codePoints(s).some((c) => FORBIDDEN.some(([lo, hi]) => c >= lo && c <= hi));
