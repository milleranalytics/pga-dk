import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";
import { installTokens } from "./tokens";

// ARCHIVO VARIABLE, declared by hand in fonts.css against the package's own
// woff2 files — see that file for why the package stylesheet is not imported.
// IBM Plex Mono stays for the ONE thing a real monospace is still right for:
// SQL on the Query tab (`font.code`). IBM Plex Sans is gone.
//
// Google Fonts would break the offline / file:// story the architecture rests
// on; assetsInlineLimit in vite.config.ts inlines these as data URIs so the
// built page is genuinely one file plus slate.js.
import "./fonts.css";
import "@fontsource/ibm-plex-mono/latin-400.css";
import "@fontsource/ibm-plex-mono/latin-600.css";

import "./index.css";

// BEFORE THE FIRST PAINT, not from an effect: index.css reads every colour as
// `var(--c-…)` and a custom property with nothing behind it is transparent.
installTokens();

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
