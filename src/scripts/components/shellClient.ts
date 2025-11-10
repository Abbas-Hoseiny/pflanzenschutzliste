import {
  type AppState,
  subscribeState,
  updateSlice,
} from "@scripts/core/state";
import { emit } from "@scripts/core/eventBus";

function initShell(): void {
  const buttons = document.querySelectorAll<HTMLButtonElement>(".nav-btn");

  buttons.forEach((button) => {
    button.addEventListener("click", (event) => {
      const target = event.currentTarget;
      if (!(target instanceof HTMLElement)) {
        return;
      }
      const section = target.dataset.section as
        | AppState["app"]["activeSection"]
        | undefined;
      if (!section) {
        return;
      }

      updateSlice("app", (app) => ({ ...app, activeSection: section }));

      buttons.forEach((btn) => btn.classList.remove("active"));
      button.classList.add("active");

      emit("app:sectionChanged", section);
    });
  });

  subscribeState((state) => {
    const brandTitle = document.getElementById("brand-title");
    const brandTagline = document.getElementById("brand-tagline");
    const appVersion = document.getElementById("app-version");

    if (brandTitle && state.company.name) {
      brandTitle.textContent = state.company.name;
    }
    if (brandTagline && state.company.headline) {
      brandTagline.textContent = state.company.headline;
    }
    if (appVersion && state.app.version) {
      appVersion.textContent = `v${state.app.version}`;
    }

    buttons.forEach((btn) => {
      const section = btn.getAttribute("data-section");
      btn.disabled = !state.app.hasDatabase;
      if (section === state.app.activeSection) {
        btn.classList.add("active");
      } else {
        btn.classList.remove("active");
      }
      if (!state.app.hasDatabase) {
        btn.classList.remove("active");
      }
    });
  });

  const calcBtn = document.querySelector('[data-section="calc"]');
  if (calcBtn instanceof HTMLElement) {
    calcBtn.classList.add("active");
  }

  let isPrinting = false;
  const activeDocumentTitle = document.title || "Pflanzenschutz";

  window.addEventListener("beforeprint", () => {
    if (isPrinting) return;
    isPrinting = true;
    document.title = " ";
  });

  window.addEventListener("afterprint", () => {
    if (!isPrinting) return;
    isPrinting = false;
    document.title = activeDocumentTitle;
  });
}

function bootstrapShell(): void {
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", initShell, { once: true });
  } else {
    initShell();
  }
}

bootstrapShell();
