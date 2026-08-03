import { createReadStream, existsSync, statSync } from "node:fs";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import type { Plugin } from "vite";
import react from "@vitejs/plugin-react";
import { viteSingleFile } from "vite-plugin-singlefile";

const REPO_DATA = fileURLToPath(new URL("../data/", import.meta.url));

/**
 * Serve the repo's data/ directory at /data during `npm run dev`.
 *
 * Vite's dev root is dashboard/, so data/golf.db sits outside it and the
 * Results Browser and DB Query tabs would be dead in dev — the two tabs most
 * likely to need iterating on. The built app does not need this: it is served
 * from the repo root by serve_dashboard(), where /data resolves naturally.
 */
function serveRepoData(): Plugin {
  return {
    name: "serve-repo-data",
    apply: "serve",
    configureServer(server) {
      server.middlewares.use((req, res, next) => {
        const url = req.url?.split("?")[0];
        if (!url?.startsWith("/data/")) return next();

        const file = resolve(REPO_DATA, url.slice("/data/".length));
        // Contain the path: a request for /data/../../etc/passwd must not
        // escape, even on a dev server bound to localhost.
        if (!file.startsWith(REPO_DATA) || !existsSync(file) || !statSync(file).isFile()) {
          return next();
        }

        res.setHeader("Content-Type", "application/octet-stream");
        res.setHeader("Content-Length", statSync(file).size);
        createReadStream(file).pipe(res);
      });
    },
  };
}

export default defineConfig({
  // singlefile inlines the JS and CSS into dist/index.html. This is not a size
  // optimisation — it is what makes the built app open by double-click.
  // Vite's normal output entry is <script type="module" src="...">, and
  // browsers block module scripts over file:// (opaque origin, CORS). An
  // INLINE module script has nothing to fetch, so it runs fine.
  //
  // data/slate.js stays external on purpose: it is regenerated weekly by the
  // notebook and must not be frozen into the build. It is a classic (non-module)
  // script, which file:// permits.
  // Pattern is root-level only: a single * does not cross "/", so it inlines
  // the bundle chunks but can never match data/slate.js.
  plugins: [react(), serveRepoData(), viteSingleFile({ inlinePattern: ["*.js", "*.css"] })],
  // Relative asset paths, so dist/index.html also opens by double-click
  // (file://). Absolute "/assets/..." would 404 off a server.
  base: "./",
  build: {
    outDir: "dist",
    emptyOutDir: true,
    // Inline every asset (the IBM Plex woff2 files) as a data URI so the CSS
    // is self-contained before singlefile inlines the CSS itself. Without this
    // the fonts stay as separate files that file:// cannot fetch.
    assetsInlineLimit: 10_000_000,
  },
  server: {
    open: true,
  },
});
