# Microgrid Web (Next.js)

Next.js + TypeScript frontend (Pages Router) for Microgrid-Modeling.

## Dev

```bash
cd "Community Load Profiles/web"

npm install
npm run dev
```

Open <http://localhost:3000>.

## Tests

```bash
npm test
```

## Notes

- The Jobs widget polls `/api/runs?active=true` but the backend endpoint will be added in later PRs.
- Charts and config editor UI will come in subsequent PRs.
