import {
  getState,
  subscribeState,
  updateSlice,
  type AppState,
} from "@scripts/core/state";
import { syncBvlData } from "@scripts/core/bvlSync";
import { checkForUpdates } from "@scripts/core/bvlDataset";
import * as storage from "@scripts/core/storage/sqlite";
import { escapeHtml } from "@scripts/core/utils";

declare const bootstrap:
  | {
      Collapse: new (
        element: Element,
        options?: unknown
      ) => {
        toggle?: () => void;
        show?: () => void;
        hide?: () => void;
      };
    }
  | undefined;

interface Services {
  state: {
    getState: typeof getState;
    subscribe: typeof subscribeState;
    updateSlice: typeof updateSlice;
  };
  events: {
    emit: (eventName: string, payload?: unknown) => void;
    subscribe?: <T = unknown>(
      eventName: string,
      handler: (payload: T) => void
    ) => () => void;
  };
}

type ZulassungState = AppState["zulassung"];

type ReportingResult = {
  status_json?: string;
  extras?: Record<string, any> | null;
  wirkstoffe?: Array<Record<string, any>> | null;
  vertrieb?: Array<Record<string, any>> | null;
  gefahrhinweise?: Array<Record<string, any>> | null;
  kulturen?: Array<Record<string, any>> | null;
  schadorganismen?: Array<Record<string, any>> | null;
  aufwaende?: Array<Record<string, any>> | null;
  wartezeiten?: Array<Record<string, any>> | null;
  [key: string]: any;
};

type ProgressInfo = ZulassungState["progress"];

type SyncLogEntry = {
  synced_at: string;
  ok: number;
  message: string;
};

const numberFormatter = new Intl.NumberFormat("de-DE", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 3,
});

let initialized = false;
let container: HTMLElement | null = null;
let services: Services | null = null;
let isSectionVisible = false;

function safeParseJson<T = any>(value: unknown): T | null {
  if (!value) {
    return null;
  }
  try {
    return JSON.parse(String(value)) as T;
  } catch (error) {
    console.warn("Failed to parse JSON payload in Zulassung view", error);
    return null;
  }
}

function firstNonEmpty<T>(...values: Array<T | null | undefined>): T | null {
  for (const value of values) {
    if (value === undefined || value === null) {
      continue;
    }
    if (typeof value === "string" && value.trim() === "") {
      continue;
    }
    return value;
  }
  return null;
}

function coerceNumber(value: unknown): number | null {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  if (typeof value === "string") {
    const trimmed = value.trim();
    if (trimmed === "") {
      return null;
    }

    let normalized = trimmed.replace(/\s+/g, "");
    if (normalized.includes(",")) {
      normalized = normalized.replace(/\./g, "").replace(/,/g, ".");
    }

    const parsed = Number(normalized);
    return Number.isNaN(parsed) ? null : parsed;
  }

  return null;
}

function formatAmount(value: unknown, unit: unknown): string | null {
  const trimmedUnit = typeof unit === "string" ? unit.trim() : "";
  const numeric = coerceNumber(value);

  if (numeric !== null) {
    const formatted = numberFormatter.format(numeric);
    return trimmedUnit ? `${formatted} ${trimmedUnit}` : formatted;
  }

  if (value === null || value === undefined) {
    return null;
  }

  const text = String(value).trim();
  if (text === "") {
    return null;
  }

  return trimmedUnit ? `${text} ${trimmedUnit}` : text;
}

function renderIfVisible(): void {
  if (isSectionVisible) {
    render();
  }
}

function renderAufwandRow(aufwand: Record<string, any>): string {
  const payload =
    safeParseJson<Record<string, any>>(aufwand.payload_json) || {};

  const mittelUnit = firstNonEmpty(
    aufwand.mittel_einheit,
    payload.aufwandmenge_einheit,
    payload.m_aufwand_einheit,
    payload.max_aufwandmenge_einheit
  );

  const mittelValue = firstNonEmpty(
    aufwand.mittel_menge,
    payload.m_aufwand,
    payload.max_aufwandmenge,
    payload.m_aufwand_bis,
    payload.m_aufwand_von,
    payload.aufwandmenge
  );

  const mittelFrom = firstNonEmpty(
    payload.m_aufwand_von,
    payload.max_aufwandmenge_von
  );
  const mittelTo = firstNonEmpty(
    payload.m_aufwand_bis,
    payload.max_aufwandmenge_bis
  );

  let mittelDisplay = formatAmount(mittelValue, mittelUnit);
  if (!mittelDisplay && (mittelFrom || mittelTo)) {
    const fromDisplay = formatAmount(mittelFrom, mittelUnit);
    const toDisplay = formatAmount(mittelTo, mittelUnit);
    if (fromDisplay && toDisplay && fromDisplay !== toDisplay) {
      mittelDisplay = `${fromDisplay} – ${toDisplay}`;
    } else if (fromDisplay) {
      mittelDisplay = fromDisplay;
    } else if (toDisplay) {
      mittelDisplay = toDisplay;
    }
  }
  if (!mittelDisplay) {
    mittelDisplay = "keine Angabe";
  }

  const wasserUnit = firstNonEmpty(
    aufwand.wasser_einheit,
    payload.wassermenge_einheit,
    payload.w_aufwand_einheit,
    payload.w_aufwand_von_einheit,
    payload.w_aufwand_bis_einheit
  );

  const wasserValue = firstNonEmpty(
    aufwand.wasser_menge,
    payload.wassermenge,
    payload.w_aufwand,
    payload.w_aufwand_bis,
    payload.w_aufwand_von,
    payload.wasseraufwand
  );

  const wasserFrom = firstNonEmpty(
    payload.w_aufwand_von,
    payload.wassermenge_von
  );
  const wasserTo = firstNonEmpty(
    payload.w_aufwand_bis,
    payload.wassermenge_bis
  );

  let wasserDisplay = formatAmount(wasserValue, wasserUnit);
  if (!wasserDisplay && (wasserFrom || wasserTo)) {
    const fromDisplay = formatAmount(wasserFrom, wasserUnit);
    const toDisplay = formatAmount(wasserTo, wasserUnit);
    if (fromDisplay && toDisplay && fromDisplay !== toDisplay) {
      wasserDisplay = `${fromDisplay} – ${toDisplay}`;
    } else if (fromDisplay) {
      wasserDisplay = fromDisplay;
    } else if (toDisplay) {
      wasserDisplay = toDisplay;
    }
  }

  const wasserText = wasserDisplay
    ? `, Wasser: ${escapeHtml(wasserDisplay)}`
    : "";

  return `${escapeHtml(
    aufwand.aufwand_bedingung || "Standard"
  )}: Mittel: ${escapeHtml(mittelDisplay)}${wasserText}`;
}

async function performAutoUpdateCheck(): Promise<void> {
  if (!services) {
    return;
  }
  try {
    const state = services.state.getState();
    const currentHash = state.zulassung.lastSyncHash;

    if (!currentHash) {
      return;
    }

    const updateCheck = await checkForUpdates(currentHash);
    const checkTime = new Date().toISOString();

    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      autoUpdateAvailable: updateCheck.available,
      autoUpdateVersion: updateCheck.newVersion,
      debug: {
        ...prev.debug,
        lastAutoUpdateCheck: {
          time: checkTime,
          result: updateCheck.available
            ? `Update verfügbar: ${updateCheck.newVersion}`
            : "Keine Updates",
        },
      },
    }));

    if (updateCheck.available) {
      renderIfVisible();
    }
  } catch (error) {
    console.warn("Auto-update check failed:", error);
  }
}

function toggleVisibility(state: AppState): void {
  if (!container) {
    return;
  }

  const section = container.querySelector<HTMLElement>(
    '[data-section="zulassung"]'
  );
  if (!section) {
    return;
  }

  const shouldShow =
    state.app.activeSection === "zulassung" && state.app.hasDatabase;
  section.classList.toggle("d-none", !shouldShow);

  if (shouldShow && !isSectionVisible) {
    isSectionVisible = true;
    render();
  } else if (!shouldShow && isSectionVisible) {
    isSectionVisible = false;
  }
}

async function loadInitialData(): Promise<void> {
  if (!services) {
    return;
  }

  try {
    if (!storage.isSupported || !storage.isSupported()) {
      return;
    }

    const [
      lastSync,
      lastSyncCounts,
      dataSource,
      apiStand,
      manifestVersion,
      lastSyncHash,
      manifestJson,
    ] = await Promise.all([
      storage.getBvlMeta("lastSyncIso"),
      storage.getBvlMeta("lastSyncCounts"),
      storage.getBvlMeta("dataSource"),
      storage.getBvlMeta("apiStand"),
      storage.getBvlMeta("manifestVersion"),
      storage.getBvlMeta("lastSyncHash"),
      storage.getBvlMeta("manifest"),
    ]);

    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      lastSync: lastSync || null,
      lastResultCounts: lastSyncCounts
        ? JSON.parse(String(lastSyncCounts))
        : null,
      dataSource: dataSource || null,
      apiStand: apiStand || null,
      manifestVersion: manifestVersion || null,
      lastSyncHash: lastSyncHash || null,
      debug: {
        ...prev.debug,
        manifest: manifestJson ? JSON.parse(String(manifestJson)) : null,
      },
    }));

    const [cultures, pests] = await Promise.all([
      storage.listBvlCultures(),
      storage.listBvlSchadorg(),
    ]);

    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      lookups: { cultures, pests },
    }));

    renderIfVisible();
  } catch (error) {
    console.error("Failed to load initial Zulassung data:", error);
  }
}

function render(): void {
  if (!container || !services) {
    return;
  }

  let section = container.querySelector<HTMLElement>(
    '[data-section="zulassung"]'
  );

  if (!section) {
    section = document.createElement("div");
    section.setAttribute("data-section", "zulassung");
    section.className = "section-content";
    container.appendChild(section);
  }

  const state = services.state.getState();
  const zulassungState = state.zulassung;

  section.innerHTML = `
    <div class="container py-4">
      <h2>BVL Zulassungsdaten</h2>
      ${renderStatusSection(zulassungState)}
      ${renderSyncSection(zulassungState)}
      ${renderFilterSection(zulassungState)}
      ${renderResultsSection(zulassungState)}
      ${renderDebugSection(zulassungState)}
    </div>
  `;

  attachEventHandlers(section);
}

function renderStatusSection(zulassungState: ZulassungState): string {
  if (!zulassungState.lastSync) {
    return `
      <div class="alert alert-info mb-3">
        <i class="bi bi-info-circle-fill me-2"></i>
        <strong>Keine Daten vorhanden.</strong> Bitte führen Sie eine Synchronisation durch, um BVL-Daten zu laden.
      </div>
    `;
  }

  const lastSyncDate = new Date(zulassungState.lastSync).toLocaleString(
    "de-DE"
  );
  const counts = zulassungState.lastResultCounts || {};
  const dataSource = zulassungState.dataSource || "BVL API";
  const apiStand = zulassungState.apiStand || null;
  const manifestVersion = zulassungState.manifestVersion || null;
  const lastSyncHash = zulassungState.lastSyncHash || null;
  const bioCount = counts.bvl_mittel_extras || 0;
  const totalMittel = counts.mittel || counts.bvl_mittel || 0;

  return `
    <div class="alert alert-success mb-3">
      <div class="d-flex justify-content-between align-items-start">
        <div>
          <i class="bi bi-check-circle-fill me-2"></i>
          <strong>Letzte Synchronisation:</strong> ${escapeHtml(lastSyncDate)}<br>
          <strong>Datenquelle:</strong> ${escapeHtml(dataSource)}<br>
          ${manifestVersion ? `<strong>Version:</strong> ${escapeHtml(manifestVersion)}<br>` : ""}
          ${apiStand ? `<strong>API-Stand:</strong> ${escapeHtml(apiStand)}<br>` : ""}
          ${lastSyncHash ? `<small class="text-muted">Hash: ${escapeHtml(lastSyncHash.substring(0, 12))}...</small><br>` : ""}
          <small class="mt-1 d-block">
            <i class="bi bi-database me-1"></i>
            Mittel: ${totalMittel}${
              bioCount > 0
                ? ` <span class="badge bg-success-subtle text-success-emphasis"><i class="bi bi-leaf-fill"></i> ${bioCount} Bio</span>`
                : ""
            }, Anwendungen: ${counts.awg || counts.bvl_awg || 0}, Kulturen: ${
              counts.awg_kultur || counts.bvl_awg_kultur || 0
            }, Schadorganismen: ${
              counts.awg_schadorg || counts.bvl_awg_schadorg || 0
            }
          </small>
        </div>
      </div>
    </div>
  `;
}

function renderSyncSection(zulassungState: ZulassungState): string {
  const isBusy = zulassungState.busy;
  const progress = zulassungState.progress;
  const error = zulassungState.error;

  const stepInfo: Record<
    string,
    { icon: string; color: string; label: string }
  > = {
    manifest: {
      icon: "bi-cloud-download",
      color: "bg-info",
      label: "Manifest",
    },
    download: {
      icon: "bi-cloud-arrow-down",
      color: "bg-info",
      label: "Download",
    },
    decompress: { icon: "bi-archive", color: "bg-primary", label: "Entpacken" },
    import: { icon: "bi-cpu", color: "bg-warning", label: "Import" },
    verify: { icon: "bi-check2", color: "bg-success", label: "Verifizierung" },
    done: {
      icon: "bi-check-circle-fill",
      color: "bg-success",
      label: "Fertig",
    },
  };

  const currentStep = progress.step
    ? stepInfo[progress.step] || stepInfo.done
    : null;

  return `
    <div class="card mb-3">
      <div class="card-body">
        <h5 class="card-title"><i class="bi bi-arrow-repeat me-2"></i>Synchronisation</h5>
        ${
          zulassungState.autoUpdateAvailable
            ? `
          <div class="alert alert-warning d-flex align-items-center" role="alert">
            <i class="bi bi-exclamation-triangle-fill me-2"></i>
            <div class="flex-grow-1">
              <strong>Neue Daten verfügbar!</strong><br>
              <small>Version ${escapeHtml(zulassungState.autoUpdateVersion || "unbekannt")} ist verfügbar.</small>
            </div>
            <button class="btn btn-warning btn-sm ms-2" id="btn-apply-update">
              <i class="bi bi-download me-1"></i>Jetzt aktualisieren
            </button>
          </div>
        `
            : ""
        }
        <button id="btn-sync" class="btn btn-primary" ${isBusy ? "disabled" : ""}>
          ${
            isBusy
              ? `<span class="spinner-border spinner-border-sm me-2"></span><i class="${
                  currentStep?.icon || "bi-arrow-repeat"
                } me-1"></i>`
              : '<i class="bi bi-arrow-repeat me-1"></i>'
          }
          ${isBusy ? "Synchronisiere..." : "Daten aktualisieren"}
        </button>
        ${
          progress.step && isBusy
            ? `
          <div class="mt-3">
            <div class="d-flex justify-content-between align-items-center mb-2">
              <small class="text-muted">
                <i class="${currentStep?.icon || "bi-arrow-repeat"} me-1"></i>
                ${escapeHtml(currentStep?.label || "Verarbeite")}: ${escapeHtml(progress.message)}
              </small>
              <small class="text-muted">${progress.percent}%</small>
            </div>
            <div class="progress" style="height: 20px;" role="progressbar" aria-valuenow="${progress.percent}" aria-valuemin="0" aria-valuemax="100" title="${escapeHtml(progress.message)}">
              <div class="progress-bar progress-bar-striped progress-bar-animated ${
                currentStep?.color || "bg-primary"
              }" style="width: ${progress.percent}%">
                ${progress.percent}%
              </div>
            </div>
          </div>
        `
            : ""
        }
        ${
          error
            ? `
          <div class="alert alert-danger mt-3 d-flex align-items-start">
            <i class="bi bi-exclamation-triangle-fill me-2 mt-1"></i>
            <div class="flex-grow-1">
              <strong>Fehler:</strong> ${escapeHtml(error)}
            </div>
            <button class="btn btn-sm btn-outline-danger ms-2" id="btn-show-debug">
              <i class="bi bi-bug me-1"></i>Debug anzeigen
            </button>
          </div>
        `
            : ""
        }
      </div>
    </div>
  `;
}

function renderFilterSection(zulassungState: ZulassungState): string {
  const { filters, lookups, busy } = zulassungState;
  const cultures = Array.isArray(lookups.cultures) ? lookups.cultures : [];
  const pests = Array.isArray(lookups.pests) ? lookups.pests : [];

  return `
    <div class="card mb-3 filter-section">
      <div class="card-body">
        <h5 class="card-title"><i class="bi bi-funnel me-2"></i>Filter</h5>
        <div class="row g-3">
          <div class="col-12">
            <label for="filter-text" class="form-label">Schnellsuche</label>
            <input type="search" id="filter-text" class="form-control" placeholder="Mittel, Kultur- oder Schaderreger-Name" value="${escapeHtml(filters.text || "")}">
            <small class="form-text text-muted">Durchsucht Mittelname, Kennnummer sowie Klartexte der Kulturen und Schadorganismen.</small>
          </div>
          <div class="col-md-4">
            <label for="filter-culture" class="form-label">
              <i class="bi bi-flower1 me-1"></i>Kultur
            </label>
            <select id="filter-culture" class="form-select">
              <option value="">Alle Kulturen</option>
              ${cultures
                .map(
                  (culture: any) =>
                    `<option value="${escapeHtml(culture.code)}" ${
                      filters.culture === culture.code ? "selected" : ""
                    }>${escapeHtml(culture.label || culture.code)} (${escapeHtml(culture.code)})</option>`
                )
                .join("")}
            </select>
          </div>
          <div class="col-md-4">
            <label for="filter-pest" class="form-label">
              <i class="bi bi-bug me-1"></i>Schadorganismus
            </label>
            <select id="filter-pest" class="form-select">
              <option value="">Alle Schadorganismen</option>
              ${pests
                .map(
                  (pest: any) =>
                    `<option value="${escapeHtml(pest.code)}" ${
                      filters.pest === pest.code ? "selected" : ""
                    }>${escapeHtml(pest.label || pest.code)} (${escapeHtml(pest.code)})</option>`
                )
                .join("")}
            </select>
          </div>
          <div class="col-md-4">
            <label class="form-label d-block">&nbsp;</label>
            <div class="form-check">
              <input class="form-check-input" type="checkbox" id="filter-bio" ${filters.bioOnly ? "checked" : ""}>
              <label class="form-check-label" for="filter-bio">
                <i class="bi bi-leaf-fill text-success me-1"></i>
                Nur Bio/Öko-zertifizierte Mittel
              </label>
            </div>
          </div>
        </div>
        <div class="form-check mt-3">
          <input class="form-check-input" type="checkbox" id="filter-expired" ${filters.includeExpired ? "checked" : ""}>
          <label class="form-check-label" for="filter-expired">
            <i class="bi bi-clock-history me-1"></i>
            Abgelaufene Zulassungen einschließen
          </label>
        </div>
        <div class="mt-3">
          <button id="btn-search" class="btn btn-primary" ${busy ? "disabled" : ""}>
            <i class="bi bi-search me-1"></i>Suchen
          </button>
          <button id="btn-clear-filters" class="btn btn-secondary ms-2">
            <i class="bi bi-x-circle me-1"></i>Filter zurücksetzen
          </button>
        </div>
      </div>
    </div>
  `;
}

function renderResultsSection(zulassungState: ZulassungState): string {
  const { results } = zulassungState;

  if (!Array.isArray(results) || results.length === 0) {
    return `
      <div class="card mb-3">
        <div class="card-body">
          <h5 class="card-title">Ergebnisse</h5>
          <p class="text-muted">Keine Ergebnisse. Bitte wählen Sie Filter aus und klicken Sie auf "Suchen".</p>
        </div>
      </div>
    `;
  }

  return `
    <div class="card mb-3">
      <div class="card-body">
        <h5 class="card-title">Ergebnisse (${results.length})</h5>
        <div class="list-group">
          ${results.map((result) => renderResultItem(result as ReportingResult)).join("")}
        </div>
      </div>
    </div>
  `;
}

function renderResultItem(result: ReportingResult): string {
  const status = result.status_json
    ? safeParseJson<Record<string, any>>(result.status_json) || {}
    : {};
  const extras = result.extras || {};
  const isBio =
    Boolean(result.is_bio) ||
    Boolean(result.is_oeko) ||
    Boolean(extras.is_bio) ||
    Boolean(extras.is_oeko);
  const certBody = result.certification_body || extras.certification_body;

  return `
    <div class="list-group-item">
      <div class="d-flex w-100 justify-content-between align-items-start">
        <h6 class="mb-1">${escapeHtml(result.name)}</h6>
        <small class="text-muted">${escapeHtml(result.kennr)}</small>
      </div>
      <div class="mb-2">
        <strong>Formulierung:</strong> ${escapeHtml(result.formulierung || "-")}<br>
        <strong>Status:</strong> ${escapeHtml(status.status || "-")}
      </div>
      <div class="mb-2">
        ${result.geringes_risiko ? '<span class="badge bg-success me-1"><i class="bi bi-shield-check me-1"></i>Geringes Risiko</span>' : ""}
        ${
          result.zul_ende
            ? `<span class="badge bg-warning text-dark me-1"><i class="bi bi-calendar-event me-1"></i>Gültig bis: ${escapeHtml(result.zul_ende)}</span>`
            : ""
        }
        ${
          isBio
            ? `<span class="badge bg-success-subtle text-success-emphasis me-1" title="${
                certBody
                  ? `Bio-zertifiziert – ${escapeHtml(certBody)}`
                  : "Bio/Öko-zertifiziert"
              }"><i class="bi bi-leaf-fill me-1"></i>Bio/Öko</span>`
            : ""
        }
      </div>
      ${renderResultWirkstoffe(result)}
      ${renderResultVertrieb(result)}
      ${renderResultGefahrhinweise(result)}
      ${renderResultKulturen(result)}
      ${renderResultSchadorganismen(result)}
      ${renderResultAufwaende(result)}
      ${renderResultWartezeiten(result)}
    </div>
  `;
}

function renderResultWirkstoffe(result: ReportingResult): string {
  if (!Array.isArray(result.wirkstoffe) || result.wirkstoffe.length === 0) {
    return "";
  }

  const list = result.wirkstoffe
    .map((entry) => {
      const gehalt = coerceNumber(entry.gehalt);
      const gehaltStr =
        gehalt !== null
          ? numberFormatter.format(gehalt)
          : String(entry.gehalt || "");
      return `
        <li>
          ${escapeHtml(entry.wirkstoff_name || entry.wirkstoff || "-")}
          ${entry.gehalt ? ` - ${escapeHtml(gehaltStr)} ${escapeHtml(entry.einheit || "")}` : ""}
        </li>
      `;
    })
    .join("");

  return `
    <div class="mt-2">
      <strong><i class="bi bi-droplet me-1"></i>Wirkstoffe:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultVertrieb(result: ReportingResult): string {
  if (!Array.isArray(result.vertrieb) || result.vertrieb.length === 0) {
    return "";
  }

  const entries = result.vertrieb
    .map((entry) => {
      const website = entry.website
        ? ` <a href="${escapeHtml(entry.website)}" target="_blank" rel="noopener" class="text-decoration-none"><i class="bi bi-box-arrow-up-right"></i></a>`
        : "";
      return `<div class="small">${escapeHtml(entry.hersteller_name || entry.firma || "-")}${website}</div>`;
    })
    .join("");

  return `
    <div class="mt-2">
      <strong><i class="bi bi-building me-1"></i>Hersteller/Vertrieb:</strong>
      ${entries}
    </div>
  `;
}

function renderResultGefahrhinweise(result: ReportingResult): string {
  if (
    !Array.isArray(result.gefahrhinweise) ||
    result.gefahrhinweise.length === 0
  ) {
    return "";
  }

  const badges = result.gefahrhinweise
    .map((hint) => {
      const code = hint.hinweis_kode || hint.h_code || hint.h_saetze || "";
      const text = hint.hinweis_text || hint.text || "";
      return `
        <span class="badge bg-danger" title="${escapeHtml(text)}" data-bs-toggle="tooltip">
          ${escapeHtml(code)}${text ? ' <i class="bi bi-info-circle-fill ms-1"></i>' : ""}
        </span>
      `;
    })
    .join("");

  return `
    <div class="mt-2">
      <strong><i class="bi bi-exclamation-triangle me-1"></i>Gefahrenhinweise:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}

function renderResultKulturen(result: ReportingResult): string {
  if (!Array.isArray(result.kulturen) || result.kulturen.length === 0) {
    return "";
  }

  const badges = result.kulturen
    .map(
      (entry) =>
        `<span class="badge ${entry.ausgenommen ? "bg-danger" : "bg-info"}" title="${escapeHtml(entry.kultur)}">${escapeHtml(
          entry.label || entry.kultur
        )}${entry.ausgenommen ? " (ausgenommen)" : ""}</span>`
    )
    .join(" ");

  return `
    <div class="mt-2">
      <strong><i class="bi bi-flower1 me-1"></i>Kulturen:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}

function renderResultSchadorganismen(result: ReportingResult): string {
  if (
    !Array.isArray(result.schadorganismen) ||
    result.schadorganismen.length === 0
  ) {
    return "";
  }

  const badges = result.schadorganismen
    .map(
      (entry) =>
        `<span class="badge ${entry.ausgenommen ? "bg-danger" : "bg-secondary"}" title="${escapeHtml(entry.schadorg)}">${escapeHtml(
          entry.label || entry.schadorg
        )}${entry.ausgenommen ? " (ausgenommen)" : ""}</span>`
    )
    .join(" ");

  return `
    <div class="mt-2">
      <strong><i class="bi bi-bug me-1"></i>Schadorganismen:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}

function renderResultAufwaende(result: ReportingResult): string {
  if (!Array.isArray(result.aufwaende) || result.aufwaende.length === 0) {
    return "";
  }

  const list = result.aufwaende
    .map((entry) => `<li>${renderAufwandRow(entry)}</li>`)
    .join("");

  return `
    <div class="mt-2">
      <strong><i class="bi bi-activity me-1"></i>Aufwände:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultWartezeiten(result: ReportingResult): string {
  if (!Array.isArray(result.wartezeiten) || result.wartezeiten.length === 0) {
    return "";
  }

  const list = result.wartezeiten
    .map((entry) => {
      const anwendungsbereich = entry.anwendungsbereich
        ? ` (${escapeHtml(entry.anwendungsbereich)})`
        : "";
      return `
        <li>
          ${escapeHtml(entry.kultur_label || entry.kultur)}: ${escapeHtml(entry.tage || "-")} Tage${anwendungsbereich}
        </li>
      `;
    })
    .join("");

  return `
    <div class="mt-2">
      <strong><i class="bi bi-clock me-1"></i>Wartezeiten:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderDebugSection(zulassungState: ZulassungState): string {
  const { debug, logs } = zulassungState;
  const manifestHtml = debug.manifest
    ? `
      <div class="mt-3">
        <h6><i class="bi bi-file-code me-1"></i>Manifest</h6>
        <details>
          <summary class="btn btn-sm btn-outline-secondary">JSON anzeigen</summary>
          <pre class="bg-dark text-light p-2 mt-2" style="font-size: 11px; max-height: 300px; overflow-y: auto;">${escapeHtml(
            JSON.stringify(debug.manifest, null, 2)
          )}</pre>
        </details>
      </div>
    `
    : "";

  const autoUpdateHtml = debug.lastAutoUpdateCheck
    ? `
      <div class="mt-3">
        <h6><i class="bi bi-clock-history me-1"></i>Letzter Auto-Update-Check</h6>
        <p class="small mb-0">
          <strong>Zeit:</strong> ${escapeHtml(
            new Date(debug.lastAutoUpdateCheck.time).toLocaleString("de-DE")
          )}<br>
          <strong>Ergebnis:</strong> ${escapeHtml(debug.lastAutoUpdateCheck.result || "OK")}
        </p>
      </div>
    `
    : "";

  const syncLogHtml =
    Array.isArray(debug.lastSyncLog) && debug.lastSyncLog.length > 0
      ? debug.lastSyncLog
          .map(
            (log: SyncLogEntry) => `
            <tr>
              <td><small>${escapeHtml(new Date(log.synced_at).toLocaleString("de-DE"))}</small></td>
              <td>${
                log.ok
                  ? '<span class="badge bg-success"><i class="bi bi-check-lg"></i> OK</span>'
                  : '<span class="badge bg-danger"><i class="bi bi-x-lg"></i> Fehler</span>'
              }</td>
              <td><small>${escapeHtml(log.message)}</small></td>
            </tr>
          `
          )
          .join("")
      : '<tr><td colspan="3" class="text-muted">Keine Logs vorhanden</td></tr>';

  const sessionLogsHtml = logs.length
    ? logs
        .slice(-50)
        .map((log) => {
          const badgeClass =
            log.level === "error"
              ? "bg-danger"
              : log.level === "warn"
                ? "bg-warning"
                : log.level === "debug"
                  ? "bg-secondary"
                  : "bg-primary";
          return `<div><span class="badge ${badgeClass} me-1">${escapeHtml(log.level.toUpperCase())}</span> ${escapeHtml(log.message)}</div>`;
        })
        .join("")
    : "";

  const schemaHtml = debug.schema
    ? `
      <div class="mt-3">
        <h6><i class="bi bi-diagram-3 me-1"></i>Schema-Informationen</h6>
        <p><strong>User Version:</strong> ${escapeHtml(debug.schema.user_version)}</p>
        <details>
          <summary class="btn btn-sm btn-outline-secondary">Tabellen anzeigen</summary>
          <pre class="bg-dark text-light p-2 mt-2" style="font-size: 11px; max-height: 400px; overflow-y: auto;">${escapeHtml(
            JSON.stringify(debug.schema.tables, null, 2)
          )}</pre>
        </details>
      </div>
    `
    : "";

  return `
    <div class="card mb-3">
      <div class="card-body">
        <h5 class="card-title">
          <button class="btn btn-sm btn-link text-decoration-none p-0" type="button" data-bs-toggle="collapse" data-bs-target="#debug-panel">
            <i class="bi bi-tools me-2"></i>Debug-Informationen ▼
          </button>
        </h5>
        <div class="collapse" id="debug-panel">
          ${manifestHtml}
          ${autoUpdateHtml}
          <div class="mt-3">
            <h6><i class="bi bi-list-ul me-1"></i>Sync-Logs (letzte 10)</h6>
            <div class="table-responsive">
              <table class="table table-sm">
                <thead>
                  <tr>
                    <th>Zeit</th>
                    <th>Status</th>
                    <th>Nachricht</th>
                  </tr>
                </thead>
                <tbody>${syncLogHtml}</tbody>
              </table>
            </div>
          </div>
          ${
            sessionLogsHtml
              ? `
            <div class="mt-3">
              <h6><i class="bi bi-terminal me-1"></i>Aktuelle Session Logs</h6>
              <div class="bg-dark text-light p-2" style="max-height: 200px; overflow-y: auto; font-family: monospace; font-size: 12px;">
                ${sessionLogsHtml}
              </div>
            </div>
          `
              : ""
          }
          ${schemaHtml}
        </div>
      </div>
    </div>
  `;
}

function attachEventHandlers(section: HTMLElement): void {
  const btnSync = section.querySelector<HTMLButtonElement>("#btn-sync");
  const btnSearch = section.querySelector<HTMLButtonElement>("#btn-search");
  const btnClearFilters =
    section.querySelector<HTMLButtonElement>("#btn-clear-filters");
  const btnShowDebug =
    section.querySelector<HTMLButtonElement>("#btn-show-debug");

  if (btnSync) {
    btnSync.addEventListener("click", handleSync);
  }

  if (btnSearch) {
    btnSearch.addEventListener("click", handleSearch);
  }

  if (btnClearFilters) {
    btnClearFilters.addEventListener("click", handleClearFilters);
  }

  if (btnShowDebug) {
    btnShowDebug.addEventListener("click", () => {
      const debugPanel = section.querySelector<HTMLElement>("#debug-panel");
      if (
        debugPanel &&
        typeof bootstrap !== "undefined" &&
        bootstrap?.Collapse
      ) {
        // eslint-disable-next-line new-cap
        new bootstrap.Collapse(debugPanel, { toggle: true });
      }
    });
  }

  const filterCulture =
    section.querySelector<HTMLSelectElement>("#filter-culture");
  const filterPest = section.querySelector<HTMLSelectElement>("#filter-pest");
  const filterText = section.querySelector<HTMLInputElement>("#filter-text");
  const filterExpired =
    section.querySelector<HTMLInputElement>("#filter-expired");
  const filterBio = section.querySelector<HTMLInputElement>("#filter-bio");
  const btnApplyUpdate =
    section.querySelector<HTMLButtonElement>("#btn-apply-update");

  if (filterCulture && services) {
    filterCulture.addEventListener("change", (event) => {
      const target = event.target as HTMLSelectElement;
      services!.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, culture: target.value || null },
      }));
    });
  }

  if (filterPest && services) {
    filterPest.addEventListener("change", (event) => {
      const target = event.target as HTMLSelectElement;
      services!.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, pest: target.value || null },
      }));
    });
  }

  if (filterText && services) {
    filterText.addEventListener("input", (event) => {
      const target = event.target as HTMLInputElement;
      services!.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, text: target.value },
      }));
    });

    filterText.addEventListener("keydown", (event) => {
      if (
        event.key === "Enter" &&
        services &&
        !services.state.getState().zulassung.busy
      ) {
        handleSearch();
      }
    });
  }

  if (filterExpired && services) {
    filterExpired.addEventListener("change", (event) => {
      const target = event.target as HTMLInputElement;
      services!.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, includeExpired: target.checked },
      }));
    });
  }

  if (filterBio && services) {
    filterBio.addEventListener("change", (event) => {
      const target = event.target as HTMLInputElement;
      services!.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, bioOnly: target.checked },
      }));
    });
  }

  if (btnApplyUpdate) {
    btnApplyUpdate.addEventListener("click", () => {
      handleSync();
    });
  }
}

async function handleSync(): Promise<void> {
  if (!services) {
    return;
  }

  services.state.updateSlice("zulassung", (prev) => ({
    ...prev,
    busy: true,
    error: null,
    logs: [],
    progress: { step: "start", percent: 0, message: "Starte..." },
  }));

  render();

  try {
    const result = await syncBvlData(storage, {
      onProgress: (progress: ProgressInfo) => {
        services!.state.updateSlice("zulassung", (prev) => ({
          ...prev,
          progress,
        }));
        render();
      },
      onLog: (log: any) => {
        services!.state.updateSlice("zulassung", (prev) => ({
          ...prev,
          logs: [...prev.logs, log],
        }));
      },
    });

    const meta = result.meta as Record<string, any>;

    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      busy: false,
      lastSync: meta.lastSyncIso ?? prev.lastSync,
      lastResultCounts: meta.lastSyncCounts ?? prev.lastResultCounts,
      dataSource: meta.dataSource ?? prev.dataSource,
      apiStand: meta.apiStand ?? prev.apiStand,
      manifestVersion: meta.manifestVersion ?? prev.manifestVersion,
      lastSyncHash: meta.lastSyncHash ?? prev.lastSyncHash,
      progress: { step: null, percent: 0, message: "" },
      autoUpdateAvailable: false,
      autoUpdateVersion: null,
    }));

    await loadInitialData();

    const [syncLog, schema] = await Promise.all([
      storage.listBvlSyncLog({ limit: 10 }),
      storage.diagnoseBvlSchema(),
    ]);

    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      debug: {
        ...prev.debug,
        schema,
        lastSyncLog: syncLog,
      },
    }));

    render();
  } catch (error: any) {
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      busy: false,
      error: error?.message || "Unbekannter Fehler",
      progress: { step: null, percent: 0, message: "" },
    }));
    render();
  }
}

async function handleSearch(): Promise<void> {
  if (!services) {
    return;
  }

  const state = services.state.getState();
  const { filters } = state.zulassung;
  const normalizedFilters = {
    ...filters,
    text: filters.text ? filters.text.trim() : "",
  };

  services.state.updateSlice("zulassung", (prev) => ({
    ...prev,
    busy: true,
  }));

  render();

  try {
    const results = await storage.queryZulassung(normalizedFilters);

    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      busy: false,
      filters: { ...prev.filters, text: normalizedFilters.text },
      results,
    }));

    render();
  } catch (error: any) {
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      busy: false,
      error: error?.message || "Unbekannter Fehler",
    }));

    render();
  }
}

function handleClearFilters(): void {
  if (!services) {
    return;
  }

  services.state.updateSlice("zulassung", (prev) => ({
    ...prev,
    filters: {
      culture: null,
      pest: null,
      text: "",
      includeExpired: false,
      bioOnly: false,
    },
    results: [],
  }));

  render();
}

export function initZulassung(
  target: Element | null,
  providedServices: Services
): void {
  if (!target || initialized) {
    return;
  }

  container = target as HTMLElement;
  services = providedServices;
  initialized = true;

  render();

  services.state.subscribe((state) => {
    toggleVisibility(state);
  });

  services.events.subscribe?.("database:connected", async () => {
    await loadInitialData();
    setTimeout(() => {
      void performAutoUpdateCheck();
    }, 2000);
  });

  const initialState = services.state.getState();
  toggleVisibility(initialState);

  if (initialState.app.hasDatabase) {
    void (async () => {
      await loadInitialData();
      setTimeout(() => {
        void performAutoUpdateCheck();
      }, 2000);
    })();
  }
}
