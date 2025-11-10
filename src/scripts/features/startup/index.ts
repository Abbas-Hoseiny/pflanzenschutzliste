import {
  createDatabase,
  detectPreferredDriver,
  getActiveDriverKey,
  openDatabase,
  setActiveDriver,
} from "@scripts/core/storage";
import { applyDatabase, createInitialDatabase } from "@scripts/core/database";
import { getState, subscribeState } from "@scripts/core/state";
import { escapeHtml } from "@scripts/core/utils";
import type { emit as emitEvent } from "@scripts/core/eventBus";

interface Services {
  state: {
    getState: typeof getState;
    subscribe: typeof subscribeState;
  };
  events: {
    emit: typeof emitEvent;
  };
}

type StartupView = "landing" | "wizard";
type CompanyState = ReturnType<typeof getState>["company"];

const DOWNLOAD_FILENAME_FALLBACK = "pflanzenschutz-datenbank.json";

let initialized = false;

function sanitizeFilename(name: string): string {
  if (!name) {
    return DOWNLOAD_FILENAME_FALLBACK;
  }
  const slug = name
    .toString()
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "");
  return `${slug || "pflanzenschutz-datenbank"}.json`;
}

async function withButtonBusy(
  button: HTMLButtonElement | null,
  task: () => Promise<void>
): Promise<void> {
  if (!button) {
    await task();
    return;
  }
  const originalText = button.textContent ?? "";
  button.disabled = true;
  button.dataset.busy = "true";
  button.textContent = "Bitte warten...";
  try {
    await task();
  } finally {
    button.disabled = false;
    button.dataset.busy = "false";
    button.textContent = originalText;
  }
}

function escapeAttr(value: unknown): string {
  return escapeHtml(value);
}

type WizardElements = {
  section: HTMLElement;
  form: HTMLFormElement;
  resultCard: HTMLElement;
  preview: HTMLElement;
  filenameLabel: HTMLElement;
  saveHint: HTMLElement | null;
  saveButton: HTMLButtonElement | null;
  reset: () => void;
};

function createWizard(baseCompany: CompanyState): WizardElements {
  const section = document.createElement("section");
  section.className = "section-container d-none";
  section.innerHTML = `
    <div class="section-inner">
      <div class="card card-dark">
        <div class="card-body">
          <div class="d-flex justify-content-between align-items-start flex-wrap gap-3 mb-3">
            <div>
              <h2 class="mb-1">Neue Datenbank konfigurieren</h2>
              <p class="text-muted mb-0">Erfasse deine Eckdaten. Daraus entsteht eine JSON-Datei für den späteren Import.</p>
            </div>
            <button type="button" class="btn btn-outline-light" data-action="wizard-back">Zurück</button>
          </div>
          <form id="database-wizard-form" class="row g-3 text-start">
            <div class="col-md-6">
              <label class="form-label" for="wizard-company-name">Firmenname*</label>
              <input class="form-control" id="wizard-company-name" name="wizard-company-name" required value="${escapeAttr(baseCompany.name)}" />
            </div>
            <div class="col-md-6">
              <label class="form-label" for="wizard-company-headline">Überschrift / Claim</label>
              <input class="form-control" id="wizard-company-headline" name="wizard-company-headline" value="${escapeAttr(baseCompany.headline)}" />
            </div>
            <div class="col-md-6">
              <label class="form-label" for="wizard-company-logo">Logo-URL</label>
              <input class="form-control" id="wizard-company-logo" name="wizard-company-logo" placeholder="https://example.com/logo.png" value="${escapeAttr(baseCompany.logoUrl)}" />
            </div>
            <div class="col-md-6">
              <label class="form-label" for="wizard-company-email">Kontakt-E-Mail</label>
              <input class="form-control" id="wizard-company-email" name="wizard-company-email" value="${escapeAttr(baseCompany.contactEmail)}" />
            </div>
            <div class="col-12">
              <label class="form-label" for="wizard-company-address">Adresse</label>
              <textarea class="form-control" id="wizard-company-address" name="wizard-company-address" rows="2">${escapeHtml(baseCompany.address)}</textarea>
            </div>
            <div class="col-12 d-flex flex-column flex-md-row gap-3">
              <button type="submit" class="btn btn-success px-4">Datenbank erzeugen</button>
              <button type="button" class="btn btn-outline-light px-4" data-action="wizard-back">Abbrechen</button>
            </div>
          </form>
        </div>
      </div>
      <div class="card card-dark mt-4 d-none" data-role="wizard-result">
        <div class="card-body">
          <h3 class="h5 mb-3">Datenbank erstellt</h3>
          <p class="mb-2">Vorschlag für den Dateinamen: <code data-role="wizard-filename"></code></p>
          <div class="d-flex flex-column flex-lg-row gap-2 mb-3">
            <button type="button" class="btn btn-success" data-action="wizard-save">Datei speichern</button>
          </div>
          <p class="text-muted small mb-2" data-role="wizard-save-hint"></p>
          <p class="text-muted small mb-2">Vorschau der kompletten JSON-Struktur (scrollbar):</p>
          <pre class="bg-dark rounded p-3 small overflow-auto" style="max-height: 14rem;" data-role="wizard-preview"></pre>
        </div>
      </div>
    </div>
  `;

  const form = section.querySelector<HTMLFormElement>("#database-wizard-form");
  if (!form) {
    throw new Error("Wizard-Formular konnte nicht erzeugt werden");
  }
  const resultCard = section.querySelector<HTMLElement>(
    '[data-role="wizard-result"]'
  );
  if (!resultCard) {
    throw new Error("Wizard-Resultat-Container fehlt");
  }

  return {
    section,
    form,
    resultCard,
    preview: section.querySelector<HTMLElement>(
      '[data-role="wizard-preview"]'
    )!,
    filenameLabel: section.querySelector<HTMLElement>(
      '[data-role="wizard-filename"]'
    )!,
    saveHint: section.querySelector<HTMLElement>(
      '[data-role="wizard-save-hint"]'
    ),
    saveButton: section.querySelector<HTMLButtonElement>(
      '[data-action="wizard-save"]'
    ),
    reset() {
      form.reset();
      resultCard.classList.add("d-none");
      const preview = section.querySelector<HTMLElement>(
        '[data-role="wizard-preview"]'
      );
      if (preview) {
        preview.textContent = "";
      }
      const filenameLabel = section.querySelector<HTMLElement>(
        '[data-role="wizard-filename"]'
      );
      if (filenameLabel) {
        filenameLabel.textContent = "";
      }
    },
  };
}

export function initStartup(
  container: Element | null,
  services: Services
): void {
  if (!container || initialized) {
    return;
  }

  const host = container as HTMLElement;
  let generatedDatabase: any = null;
  let generatedFilename = DOWNLOAD_FILENAME_FALLBACK;
  let activeView: StartupView = "landing";

  const stateSnapshot = services.state.getState();
  const baseCompany = stateSnapshot.company;

  const landingSection = document.createElement("section");
  landingSection.className = "section-container";

  const activeDriver = getActiveDriverKey();
  const usingSQLite = activeDriver === "sqlite";
  const storageInfo = usingSQLite
    ? ""
    : "Die erzeugte JSON-Datei kann später erneut geladen oder weitergegeben werden.";

  landingSection.innerHTML = `
    <div class="section-inner">
      <div class="card card-dark">
        <div class="card-body text-center">
          <h2 class="mb-3">Datenbank starten</h2>
          <p class="mb-4">
            Verwalte und berechne Pflanzenschutzmittel zentral: Lege eine neue SQLite-Datenbank an oder verbinde eine vorhandene Datei, die du lokal speichern und jederzeit erneut verwenden kannst.
          </p>
          <div class="d-flex flex-column flex-md-row gap-3 justify-content-center">
            <button class="btn btn-success px-4" data-action="start-wizard">Neue Datenbank erstellen</button>
            <button class="btn btn-outline-light px-4" data-action="open">Bestehende Datei verbinden</button>
            <button class="btn btn-secondary px-4" data-action="useDefaults">Defaults testen</button>
          </div>
          ${storageInfo ? `<p class="mt-3 text-muted mb-0 small">${storageInfo}</p>` : ""}
        </div>
      </div>
    </div>
  `;

  const wizard = createWizard(baseCompany);

  host.innerHTML = "";
  host.appendChild(landingSection);
  host.appendChild(wizard.section);

  const fileSystemSupported =
    typeof window !== "undefined" &&
    typeof (
      window as typeof window & {
        showSaveFilePicker?: (...args: unknown[]) => Promise<unknown>;
      }
    ).showSaveFilePicker === "function";
  if (wizard.saveButton) {
    if (!fileSystemSupported) {
      wizard.saveButton.disabled = true;
      wizard.saveButton.textContent = "Datei speichern (nicht verfügbar)";
      if (wizard.saveHint) {
        wizard.saveHint.textContent =
          "Dieser Browser unterstützt keinen direkten Dateidialog. Bitte nutze einen Chromium-basierten Browser (z. B. Chrome, Edge) über HTTPS oder http://localhost.";
      }
    } else if (wizard.saveHint) {
      wizard.saveHint.textContent =
        'Der Browser fragt nach einem Speicherort. Die erzeugte Datei kannst du später über "Bestehende Datei verbinden" erneut laden.';
    }
  }

  function updateRegionVisibility(state = services.state.getState()): void {
    const hasDatabase = Boolean(state.app?.hasDatabase);
    host.classList.toggle("d-none", hasDatabase);
    if (hasDatabase) {
      landingSection.classList.add("d-none");
      wizard.section.classList.add("d-none");
      return;
    }
    if (activeView === "wizard") {
      landingSection.classList.add("d-none");
      wizard.section.classList.remove("d-none");
    } else {
      landingSection.classList.remove("d-none");
      wizard.section.classList.add("d-none");
    }
  }

  async function handleOpen(button: HTMLButtonElement | null): Promise<void> {
    await withButtonBusy(button, async () => {
      try {
        const preferred = getActiveDriverKey();
        if (preferred === "sqlite" || preferred === "filesystem") {
          setActiveDriver(preferred);
        } else {
          setActiveDriver("filesystem");
        }
      } catch (err) {
        window.alert(
          "Dateisystemzugriff wird nicht unterstützt in diesem Browser."
        );
        throw err instanceof Error
          ? err
          : new Error("Dateisystem nicht verfügbar");
      }

      try {
        const result = await openDatabase();
        applyDatabase(result.data);
        services.events.emit("database:connected", {
          driver: getActiveDriverKey(),
        });
      } catch (err) {
        console.error("Fehler beim Öffnen der Datenbank", err);
        window.alert(
          err instanceof Error
            ? err.message
            : "Öffnen der Datenbank fehlgeschlagen"
        );
      }
    });
  }

  function handleUseDefaults(button: HTMLButtonElement | null): void {
    void withButtonBusy(button, async () => {
      try {
        try {
          setActiveDriver("localstorage");
        } catch (err) {
          console.warn(
            "LocalStorage-Treiber nicht verfügbar, nutze Memory",
            err
          );
          setActiveDriver("memory");
        }
        const initialData = createInitialDatabase();
        applyDatabase(initialData);
        services.events.emit("database:connected", {
          driver: getActiveDriverKey() || "memory",
        });
      } catch (err) {
        console.error("Fehler beim Laden der Defaults", err);
        window.alert(
          err instanceof Error
            ? err.message
            : "Defaults konnten nicht geladen werden"
        );
      }
    });
  }

  async function handleWizardSave(
    button: HTMLButtonElement | null
  ): Promise<void> {
    if (!generatedDatabase) {
      window.alert("Bitte erst die Datenbank erzeugen.");
      return;
    }
    await withButtonBusy(button, async () => {
      try {
        const preferred = getActiveDriverKey();
        if (preferred === "sqlite" || preferred === "filesystem") {
          setActiveDriver(preferred);
        } else {
          setActiveDriver("filesystem");
        }
      } catch (err) {
        window.alert(
          "Dateisystemzugriff wird nicht unterstützt in diesem Browser."
        );
        throw err instanceof Error
          ? err
          : new Error("Dateisystem nicht verfügbar");
      }

      try {
        const result = await createDatabase(generatedDatabase);
        applyDatabase(result.data);
        services.events.emit("database:connected", {
          driver: getActiveDriverKey(),
        });
      } catch (err) {
        console.error("Fehler beim Speichern der Datenbank", err);
        window.alert(
          err instanceof Error
            ? err.message
            : "Die Datei konnte nicht gespeichert werden"
        );
      }
    });
  }

  function handleWizardSubmit(event: SubmitEvent): void {
    event.preventDefault();
    const formData = new FormData(wizard.form);
    const name = (formData.get("wizard-company-name") || "").toString().trim();
    if (!name) {
      window.alert("Bitte einen Firmennamen angeben.");
      return;
    }

    const headline = (formData.get("wizard-company-headline") || "")
      .toString()
      .trim();
    const logoUrl = (formData.get("wizard-company-logo") || "")
      .toString()
      .trim();
    const contactEmail = (formData.get("wizard-company-email") || "")
      .toString()
      .trim();
    const address = (formData.get("wizard-company-address") || "")
      .toString()
      .trim();

    const overrides = {
      meta: {
        company: {
          name,
          headline,
          logoUrl,
          contactEmail,
          address,
        },
      },
    };

    generatedDatabase = createInitialDatabase(overrides);
    generatedFilename = sanitizeFilename(name);

    wizard.preview.textContent = JSON.stringify(generatedDatabase, null, 2);
    wizard.filenameLabel.textContent = generatedFilename;
    wizard.resultCard.classList.remove("d-none");
    wizard.resultCard.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  function showLanding(): void {
    activeView = "landing";
    generatedDatabase = null;
    generatedFilename = DOWNLOAD_FILENAME_FALLBACK;
    wizard.reset();
    updateRegionVisibility();
  }

  function showWizard(): void {
    activeView = "wizard";
    updateRegionVisibility();
  }

  landingSection.addEventListener("click", (event) => {
    const button = (event.target as HTMLElement | null)?.closest(
      "button[data-action]"
    ) as HTMLButtonElement | null;
    if (!button) {
      return;
    }
    const action = button.dataset.action;
    if (action === "start-wizard") {
      showWizard();
      return;
    }
    if (action === "open") {
      void handleOpen(button);
    } else if (action === "useDefaults") {
      handleUseDefaults(button);
    }
  });

  wizard.form.addEventListener("submit", handleWizardSubmit);

  wizard.section.addEventListener("click", (event) => {
    const button = (event.target as HTMLElement | null)?.closest(
      "[data-action]"
    ) as HTMLButtonElement | null;
    if (!button) {
      return;
    }
    const action = button.dataset.action;
    if (action === "wizard-back") {
      showLanding();
      return;
    }
    if (action === "wizard-save") {
      void handleWizardSave(button);
    }
  });

  services.state.subscribe((nextState) => updateRegionVisibility(nextState));
  updateRegionVisibility(services.state.getState());

  if (!services.state.getState().app.hasDatabase) {
    const preferred = detectPreferredDriver();
    if (preferred && preferred !== getActiveDriverKey()) {
      try {
        setActiveDriver(preferred);
      } catch (err) {
        console.warn("Bevorzugter Speicher konnte nicht gesetzt werden", err);
      }
    }
  }

  initialized = true;
}
