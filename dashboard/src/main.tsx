import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import App from "./App";

// Self-hosted IBM Plex, latin subset, only the weights the design uses.
// Google Fonts would break the offline / file:// story the architecture rests
// on; assetsInlineLimit in vite.config.ts inlines these as data URIs so the
// built page is genuinely one file plus slate.js.
import "@fontsource/ibm-plex-sans/latin-400.css";
import "@fontsource/ibm-plex-sans/latin-500.css";
import "@fontsource/ibm-plex-sans/latin-600.css";
import "@fontsource/ibm-plex-mono/latin-400.css";
import "@fontsource/ibm-plex-mono/latin-500.css";
import "@fontsource/ibm-plex-mono/latin-600.css";

import "./index.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
