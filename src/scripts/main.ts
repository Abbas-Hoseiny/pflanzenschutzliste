import { bootstrap } from "./core/bootstrap";

const bootstrapFlag = "__pflanzenschutz_bootstrapped__";
const win = window as typeof window & Record<string, unknown>;

function startApp(): void {
  bootstrap().catch((error) => {
    console.error("bootstrap failed", error);
  });
}

if (!win[bootstrapFlag]) {
  win[bootstrapFlag] = true;

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", startApp, { once: true });
  } else {
    startApp();
  }
}
