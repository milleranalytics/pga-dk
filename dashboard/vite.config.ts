import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { viteSingleFile } from "vite-plugin-singlefile";

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
  plugins: [react(), viteSingleFile({ inlinePattern: ["*.js", "*.css"] })],
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
