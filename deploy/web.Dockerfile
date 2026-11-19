# Web app image: build the React app with Node 22, serve it with unprivileged nginx. Build context is the repo root
# (.dockerignore keeps node_modules, web/dist, data/ and secrets out). Multi-arch: both base images publish arm64
# [verify with `docker buildx imagetools inspect node:22-bookworm-slim`].
#
# Contract with the web app: this build sets NO VITE_API_MODE and NO VITE_API_BASE_URL, so the app uses its own
# production default. With the client switch from the web story (W1-WEB) that default is the real API client on
# same-origin /api/v1, which nginx proxies to the api service; an app build from before that story uses its mock data.

FROM node:22-bookworm-slim AS build
WORKDIR /web
ENV CI=true NPM_CONFIG_UPDATE_NOTIFIER=false NPM_CONFIG_FUND=false NPM_CONFIG_AUDIT=false
# Dependencies first (cached until the lockfile changes). glibc base: the lockfile carries the linux-*-gnu bindings.
# .npmrc carries legacy-peer-deps, which the lockfile was resolved with; without it `npm ci` rejects the lockfile.
COPY web/package.json web/package-lock.json web/.npmrc ./
RUN npm ci
COPY web/ ./
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.27-alpine AS web
COPY deploy/web/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /web/dist /usr/share/nginx/html
# The base image already runs as the unprivileged `nginx` user (uid 101); stated here so it cannot silently change.
USER 101
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --retries=10 CMD wget -q -O /dev/null http://127.0.0.1:8080/ || exit 1
