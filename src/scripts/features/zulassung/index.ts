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

function formatDateHtml(value: unknown): string | null {
  if (value === null || value === undefined) {
    return null;
  }

  const raw = String(value).trim();
  if (!raw) {
    return null;
  }

  const parsed = Date.parse(raw);
  if (Number.isNaN(parsed)) {
    return escapeHtml(raw);
  }

  const formatted = new Date(parsed).toLocaleDateString("de-DE");
  if (formatted === raw) {
    return escapeHtml(formatted);
  }

  return `<span title="${escapeHtml(raw)}">${escapeHtml(formatted)}</span>`;
}

function normalizeText(value: unknown): string {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value).trim();
}

function formatAddressDetails(
  adresse: Record<string, any> | null | undefined
): string {
  if (!adresse || typeof adresse !== "object") {
    return "";
  }

  const lines: string[] = [];
  for (let index = 1; index <= 6; index += 1) {
    const key = `anschrift_${index}`;
    const value = adresse[key];
    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed) {
        lines.push(escapeHtml(trimmed));
      }
    }
  }

  if (adresse.land) {
    lines.push(escapeHtml(String(adresse.land)));
  }

  const contactParts: string[] = [];
  if (adresse.telefon) {
    contactParts.push(`Tel: ${escapeHtml(String(adresse.telefon))}`);
  }
  if (adresse.fax) {
    contactParts.push(`Fax: ${escapeHtml(String(adresse.fax))}`);
  }
  if (adresse.e_mail) {
    const email = String(adresse.e_mail).trim();
    if (email) {
      contactParts.push(
        `E-Mail: <a href="mailto:${encodeURIComponent(email)}" class="text-decoration-none">${escapeHtml(email)}</a>`
      );
    }
  }

  const website = firstNonEmpty(adresse.website, adresse.homepage);
  if (website) {
    const url = String(website).trim();
    if (url) {
      contactParts.push(
        `<a href="${escapeHtml(url)}" target="_blank" rel="noopener" class="text-decoration-none"><i class="bi bi-box-arrow-up-right me-1"></i>Website</a>`
      );
    }
  }

  const reference =
    adresse.__meta && adresse.__meta.primary_ref
      ? String(adresse.__meta.primary_ref)
      : null;

  const linesHtml = lines.length ? `<div>${lines.join("<br>")}</div>` : "";
  const contactsHtml = contactParts.length
    ? `<div>${contactParts.join(" · ")}</div>`
    : "";
  const referenceHtml = reference
    ? `<div>Nr.: ${escapeHtml(reference)}</div>`
    : "";

  const content = `${linesHtml}${contactsHtml}${referenceHtml}`;
  return content ? `<div class="text-muted small mt-1">${content}</div>` : "";
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
            Mittel: ${totalMittel}, Anwendungen: ${counts.awg || counts.bvl_awg || 0}, Kulturen: ${
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
              <input class="form-check-input" type="checkbox" id="filter-expired" ${filters.includeExpired ? "checked" : ""}>
              <label class="form-check-label" for="filter-expired">
                <i class="bi bi-clock-history me-1"></i>
                Abgelaufene Zulassungen einschließen
              </label>
            </div>
          </div>
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

  return `
    <div class="list-group-item">
      <div class="d-flex w-100 justify-content-between align-items-start">
        <h6 class="mb-1">${escapeHtml(result.name)}</h6>
        <small class="text-muted">AWG: ${escapeHtml(result.awg_id || "-")}</small>
      </div>
      <div class="mb-2">
        <strong>Zulassungsnummer:</strong> ${escapeHtml(result.kennr || "-")}<br>
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
          result.zulassungsende
            ? `<span class="badge bg-info text-dark me-1"><i class="bi bi-calendar-range me-1"></i>Anwendung gültig bis: ${escapeHtml(result.zulassungsende)}</span>`
            : ""
        }
      </div>
      ${renderResultWirkstoffe(result)}
      ${renderResultWirkstoffGehalt(result)}
      ${renderResultZusatzstoffe(result)}
      ${renderResultStaerkung(result)}
      ${renderResultAntraege(result)}
      ${renderResultHinweise(result)}
      ${renderResultGefahrhinweise(result)}
      ${renderResultGefahrensymbole(result)}
      ${renderResultSicherheitshinweise(result)}
      ${renderResultSignalwoerter(result)}
      ${renderResultParallelimporte(result)}
      ${renderResultVertrieb(result)}
      ${renderResultZusatzstoffVertrieb(result)}
      ${renderResultStaerkungVertrieb(result)}
      ${renderResultKulturen(result)}
      ${renderResultSchadorganismen(result)}
      ${renderResultAufwaende(result)}
      ${renderResultAuflagen(result)}
      ${renderResultAwgPartner(result)}
      ${renderResultAwgPartnerAufwand(result)}
      ${renderResultAwgBemerkungen(result)}
      ${renderResultAwgVerwendungszwecke(result)}
      ${renderResultWartezeiten(result)}
      ${renderResultAwgWartezeitAusnahmen(result)}
      ${renderResultAwgZeitpunkte(result)}
      ${renderResultAwgZulassung(result)}
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

  const dedupe = new Set<string>();
  const entries = result.vertrieb
    .map((entry) => {
      const adresse = (entry as Record<string, any>).adresse || null;
      const keyCandidate =
        (adresse && adresse.__meta && adresse.__meta.primary_ref) ||
        entry.adresse_nr ||
        entry.vertriebsfirma_nr ||
        entry.hersteller_nr ||
        entry.vertriebsfirma ||
        null;
      const key = keyCandidate ? String(keyCandidate) : JSON.stringify(entry);
      if (dedupe.has(key)) {
        return null;
      }
      dedupe.add(key);

      const displayName =
        firstNonEmpty(
          entry.hersteller_name,
          entry.hersteller,
          entry.firmenname,
          entry.firma,
          entry.vertriebsfirma_name,
          entry.vertriebsfirma,
          adresse?.firmenname,
          adresse?.firma
        ) || "-";

      const websiteSource = firstNonEmpty(
        entry.website,
        adresse?.website,
        adresse?.homepage
      );
      const websiteLink =
        websiteSource && String(websiteSource).trim()
          ? ` <a href="${escapeHtml(String(websiteSource).trim())}" target="_blank" rel="noopener" class="text-decoration-none"><i class="bi bi-box-arrow-up-right"></i></a>`
          : "";

      const addressDetails = formatAddressDetails(adresse);
      const fallbackReference =
        !addressDetails && keyCandidate
          ? `<div class="text-muted small mt-1">Nr.: ${escapeHtml(String(keyCandidate))}</div>`
          : "";

      return `<div class="mb-2"><div class="small fw-semibold">${escapeHtml(displayName)}${websiteLink}</div>${addressDetails || fallbackReference}</div>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!entries) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-building me-1"></i>Hersteller/Vertrieb:</strong>
      <div>${entries}</div>
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

function renderResultWirkstoffGehalt(result: ReportingResult): string {
  if (
    !Array.isArray(result.wirkstoff_gehalt) ||
    result.wirkstoff_gehalt.length === 0
  ) {
    return "";
  }
  const entries = result.wirkstoff_gehalt
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const wirknr = normalizeText(
        entry.wirknr ?? entry.wirkstoffnr ?? entry.wirk_nr
      );
      const variant = normalizeText(entry.wirkvar ?? entry.variante);
      const gehaltPrim = firstNonEmpty(
        entry.gehalt_rein_grundstruktur,
        entry.gehalt_rein,
        entry.gehalt
      );
      const gehaltEinheit = firstNonEmpty(
        entry.gehalt_einheit,
        entry.einheit,
        entry.gehalt_einheit
      );
      const gehalt =
        gehaltPrim !== null ? formatAmount(gehaltPrim, gehaltEinheit) : null;
      const bio = formatAmount(entry.gehalt_bio, entry.gehalt_bio_einheit);

      const detailParts: string[] = [];
      if (gehalt) {
        detailParts.push(`Gehalt: ${escapeHtml(gehalt)}`);
      }
      if (bio) {
        detailParts.push(`Bio: ${escapeHtml(bio)}`);
      }
      if (variant) {
        detailParts.push(`Variante: ${escapeHtml(variant)}`);
      }

      const header = wirknr
        ? `<span class="fw-semibold">${escapeHtml(wirknr)}</span>`
        : "";

      if (!header && detailParts.length === 0) {
        return null;
      }

      const details = detailParts.join(" · ");
      return `<li>${header}${header && details ? " – " : ""}${details}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!entries) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-beaker me-1"></i>Wirkstoffgehalte:</strong>
      <ul class="small mb-0">${entries}</ul>
    </div>
  `;
}

function renderResultZusatzstoffe(result: ReportingResult): string {
  if (!Array.isArray(result.zusatzstoffe) || result.zusatzstoffe.length === 0) {
    return "";
  }

  const list = result.zusatzstoffe
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const name = normalizeText(
        entry.mittelname ?? entry.mittel ?? entry.name ?? "-"
      );
      const start = formatDateHtml(entry.genehmigung_am);
      const end = formatDateHtml(entry.genehmigungsende);
      const applicant = normalizeText(
        entry.antragsteller ?? entry.antragsteller_name
      );
      const applicantNr = normalizeText(entry.antragsteller_nr);

      const metaParts: string[] = [];
      if (start) {
        metaParts.push(`ab ${start}`);
      }
      if (end) {
        metaParts.push(`bis ${end}`);
      }
      if (applicant) {
        metaParts.push(`Antragsteller: ${escapeHtml(applicant)}`);
      }
      if (applicantNr) {
        metaParts.push(`Nr.: ${escapeHtml(applicantNr)}`);
      }

      return `<li><span class="fw-semibold">${escapeHtml(
        name || "-"
      )}</span>${metaParts.length ? ` – ${metaParts.join(" · ")}` : ""}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-puzzle me-1"></i>Zusatzstoffe:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultZusatzstoffVertrieb(result: ReportingResult): string {
  if (
    !Array.isArray(result.zusatzstoff_vertrieb) ||
    result.zusatzstoff_vertrieb.length === 0
  ) {
    return "";
  }

  const list = result.zusatzstoff_vertrieb
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const name = normalizeText(
        entry.vertriebsfirma_name ??
          entry.vertriebsfirma ??
          entry.firma ??
          entry.hersteller
      );
      const number = normalizeText(entry.vertriebsfirma_nr ?? entry.nr);

      if (!name && !number) {
        return null;
      }

      const parts: string[] = [];
      if (name) {
        parts.push(escapeHtml(name));
      }
      if (number) {
        parts.push(
          `<span class="text-muted">Nr.: ${escapeHtml(number)}</span>`
        );
      }

      return `<li>${parts.join(" · ")}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-diagram-2 me-1"></i>Zusatzstoff-Vertrieb:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultStaerkung(result: ReportingResult): string {
  if (!Array.isArray(result.staerkung) || result.staerkung.length === 0) {
    return "";
  }

  const list = result.staerkung
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const name = normalizeText(entry.mittelname ?? entry.mittel ?? "-");
      const start = formatDateHtml(entry.genehmigung_am);
      const applicant = normalizeText(entry.antragsteller);
      const applicantNr = normalizeText(entry.antragsteller_nr);

      const metaParts: string[] = [];
      if (start) {
        metaParts.push(`Genehmigt am ${start}`);
      }
      if (applicant) {
        metaParts.push(`Antragsteller: ${escapeHtml(applicant)}`);
      }
      if (applicantNr) {
        metaParts.push(`Nr.: ${escapeHtml(applicantNr)}`);
      }

      return `<li><span class="fw-semibold">${escapeHtml(
        name || "-"
      )}</span>${metaParts.length ? ` – ${metaParts.join(" · ")}` : ""}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-leaf me-1"></i>Stärkungsmittel:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultStaerkungVertrieb(result: ReportingResult): string {
  if (
    !Array.isArray(result.staerkung_vertrieb) ||
    result.staerkung_vertrieb.length === 0
  ) {
    return "";
  }

  const list = result.staerkung_vertrieb
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const company = normalizeText(
        entry.vertriebsfirma_name ??
          entry.vertriebsfirma ??
          entry.firma ??
          entry.hersteller
      );
      const number = normalizeText(entry.vertriebsfirma_nr ?? entry.nr);

      if (!company && !number) {
        return null;
      }

      const parts: string[] = [];
      if (company) {
        parts.push(escapeHtml(company));
      }
      if (number) {
        parts.push(
          `<span class="text-muted">Nr.: ${escapeHtml(number)}</span>`
        );
      }

      return `<li>${parts.join(" · ")}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-diagram-2-fill me-1"></i>Stärkungsmittel-Vertrieb:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultAntraege(result: ReportingResult): string {
  if (!Array.isArray(result.antraege) || result.antraege.length === 0) {
    return "";
  }

  const list = result.antraege
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const number = normalizeText(entry.antragnr ?? entry.nr);
      const applicant = normalizeText(
        entry.antragsteller ?? entry.antragsteller_name
      );
      const applicantNr = normalizeText(entry.antragsteller_nr);

      if (!number && !applicant && !applicantNr) {
        return null;
      }

      const parts: string[] = [];
      if (number) {
        parts.push(`Antrag ${escapeHtml(number)}`);
      }
      if (applicant) {
        parts.push(`von ${escapeHtml(applicant)}`);
      }
      if (applicantNr) {
        parts.push(
          `<span class="text-muted">Nr.: ${escapeHtml(applicantNr)}</span>`
        );
      }

      return `<li>${parts.join(" · ")}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-inbox me-1"></i>Anträge:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultHinweise(result: ReportingResult): string {
  if (!Array.isArray(result.hinweise) || result.hinweise.length === 0) {
    return "";
  }

  const hintMap = new Map<string, string[]>();

  for (const entry of result.hinweise) {
    if (!entry || typeof entry !== "object") {
      continue;
    }

    const code = normalizeText(entry.hinweis ?? entry.code);
    if (!code) {
      continue;
    }

    const level = normalizeText(entry.ebene ?? result.kennr ?? "-") || "-";
    if (!hintMap.has(level)) {
      hintMap.set(level, []);
    }
    hintMap.get(level)!.push(code);
  }

  if (!hintMap.size) {
    return "";
  }

  const rows = Array.from(hintMap.entries())
    .map(([level, codes]) => {
      const uniqueCodes = Array.from(new Set(codes.filter(Boolean)));
      if (!uniqueCodes.length) {
        return null;
      }
      const badges = uniqueCodes
        .map(
          (code) =>
            `<span class="badge bg-secondary">${escapeHtml(code)}</span>`
        )
        .join(" ");
      return `<li><span class="fw-semibold">${escapeHtml(
        level
      )}</span>: ${badges}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!rows) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-info-circle me-1"></i>Hinweise:</strong>
      <ul class="small mb-0 d-flex flex-column gap-1">${rows}</ul>
    </div>
  `;
}

function renderResultGefahrensymbole(result: ReportingResult): string {
  if (
    !Array.isArray(result.gefahrensymbole) ||
    result.gefahrensymbole.length === 0
  ) {
    return "";
  }

  const unique = Array.from(
    new Set(
      result.gefahrensymbole
        .map((entry) =>
          entry && typeof entry === "object"
            ? normalizeText(entry.gefahrensymbol ?? entry.symbol ?? entry.code)
            : ""
        )
        .filter((code) => code)
    )
  );

  if (!unique.length) {
    return "";
  }

  const badges = unique
    .map(
      (code) =>
        `<span class="badge bg-dark text-light border border-light">${escapeHtml(code)}</span>`
    )
    .join(" ");

  return `
    <div class="mt-2">
      <strong><i class="bi bi-sign-stop me-1"></i>Gefahrensymbole:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}

function renderResultSicherheitshinweise(result: ReportingResult): string {
  if (
    !Array.isArray(result.sicherheitshinweise) ||
    result.sicherheitshinweise.length === 0
  ) {
    return "";
  }

  const unique = Array.from(
    new Set(
      result.sicherheitshinweise
        .map((entry) =>
          entry && typeof entry === "object"
            ? normalizeText(
                entry.sicherheitshinweis ??
                  entry.p_code ??
                  entry.p_satz ??
                  entry.code
              )
            : ""
        )
        .filter((code) => code)
    )
  );

  if (!unique.length) {
    return "";
  }

  const badges = unique
    .map(
      (code) =>
        `<span class="badge bg-warning text-dark">${escapeHtml(code)}</span>`
    )
    .join(" ");

  return `
    <div class="mt-2">
      <strong><i class="bi bi-shield-exclamation me-1"></i>Sicherheitshinweise:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}

function renderResultSignalwoerter(result: ReportingResult): string {
  if (
    !Array.isArray(result.signalwoerter) ||
    result.signalwoerter.length === 0
  ) {
    return "";
  }

  const unique = Array.from(
    new Set(
      result.signalwoerter
        .map((entry) =>
          entry && typeof entry === "object"
            ? normalizeText(entry.signalwort ?? entry.signal_word ?? entry.code)
            : ""
        )
        .filter((code) => code)
    )
  );

  if (!unique.length) {
    return "";
  }

  const badges = unique
    .map(
      (value) => `<span class="badge bg-secondary">${escapeHtml(value)}</span>`
    )
    .join(" ");

  return `
    <div class="mt-2">
      <strong><i class="bi bi-signpost me-1"></i>Signalwörter:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}

function renderResultParallelimporte(result: ReportingResult): string {
  const sections: string[] = [];

  const buildList = (
    entries: Array<Record<string, any>> | null | undefined,
    title: string,
    variant: "valid" | "expired"
  ) => {
    if (!Array.isArray(entries) || entries.length === 0) {
      return;
    }

    const items = entries
      .map((entry) => {
        if (!entry || typeof entry !== "object") {
          return null;
        }

        const name = normalizeText(
          entry.pi_mittelname ?? entry.mittelname ?? "-"
        );
        const importer = normalizeText(
          entry.importeur_txt ?? entry.importeur_name ?? entry.importeur
        );
        const importerCode = normalizeText(entry.importeur);
        const importerNr = normalizeText(entry.importeur_nr);
        const reference = normalizeText(
          entry.pi_referenz_kennr ?? entry.referenz_kennr
        );
        const kennziffer = normalizeText(
          entry.pi_kennziffer ?? entry.kennziffer
        );
        const status = normalizeText(entry.pi_status ?? entry.status);
        const gueltig = formatDateHtml(entry.gueltig);
        const bescheidNr = normalizeText(
          entry.pi_bescheidnr ?? entry.bescheid_nr
        );
        const bescheidDatum = formatDateHtml(entry.bescheid_datum);
        const bescheinigung = normalizeText(entry.bescheinigung);

        const headline = `<span class="fw-semibold">${escapeHtml(
          name || "-"
        )}</span>`;

        const metaParts: string[] = [];
        if (importer) {
          const suffix =
            importerCode && importerCode !== importer
              ? ` (${escapeHtml(importerCode)})`
              : "";
          metaParts.push(`Importeur: ${escapeHtml(importer)}${suffix}`);
        }
        if (importerNr) {
          metaParts.push(`Nr.: ${escapeHtml(importerNr)}`);
        }
        if (reference) {
          metaParts.push(`Referenz: ${escapeHtml(reference)}`);
        }
        if (kennziffer) {
          metaParts.push(`Kennziffer: ${escapeHtml(kennziffer)}`);
        }
        if (status) {
          metaParts.push(`Status: ${escapeHtml(status)}`);
        }
        if (gueltig) {
          const label = variant === "expired" ? "Ausgelaufen am" : "Gültig bis";
          metaParts.push(`${label} ${gueltig}`);
        }
        if (bescheidNr) {
          metaParts.push(`Bescheid ${escapeHtml(bescheidNr)}`);
        }
        if (bescheidDatum) {
          metaParts.push(`vom ${bescheidDatum}`);
        }
        if (bescheinigung) {
          metaParts.push(`Bescheinigung: ${escapeHtml(bescheinigung)}`);
        }

        const meta =
          metaParts.length > 0
            ? `<div class="small text-muted">${metaParts.join(" · ")}</div>`
            : "";

        return `<li>${headline}${meta}</li>`;
      })
      .filter((entry): entry is string => Boolean(entry))
      .join("");

    if (!items) {
      return;
    }

    sections.push(`
      <div class="mt-2">
        <em class="d-block">${escapeHtml(title)}:</em>
        <ul class="small mb-0">${items}</ul>
      </div>
    `);
  };

  buildList(result.parallelimporte_gueltig, "Gültige Parallelimporte", "valid");
  buildList(
    result.parallelimporte_abgelaufen,
    "Abgelaufene Parallelimporte",
    "expired"
  );

  if (!sections.length) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-arrow-left-right me-1"></i>Parallelimporte:</strong>
      ${sections.join("")}
    </div>
  `;
}

function renderResultAuflagen(result: ReportingResult): string {
  if (!Array.isArray(result.auflagen) || result.auflagen.length === 0) {
    return "";
  }

  const list = result.auflagen
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const code = normalizeText(entry.auflage ?? entry.code);
      const level = normalizeText(entry.ebene);
      const culture = normalizeText(entry.kultur);
      const condition = normalizeText(entry.weitere_bedingung);
      const technique = normalizeText(entry.anwendungstechnik);
      const distance = normalizeText(entry.abstand);
      const reducedDistance = normalizeText(entry.redu_abstand);
      const requirement = normalizeText(entry.anwendbest);

      const metaParts: string[] = [];
      if (level) {
        metaParts.push(`Ebene: ${escapeHtml(level)}`);
      }
      if (culture) {
        metaParts.push(`Kultur: ${escapeHtml(culture)}`);
      }
      if (condition) {
        metaParts.push(escapeHtml(condition));
      }
      if (technique) {
        metaParts.push(`Technik: ${escapeHtml(technique)}`);
      }
      if (distance) {
        metaParts.push(`Abstand: ${escapeHtml(distance)}`);
      }
      if (reducedDistance) {
        metaParts.push(`reduz. Abstand: ${escapeHtml(reducedDistance)}`);
      }

      const reductions =
        Array.isArray((entry as any).reduzierung) &&
        (entry as any).reduzierung.length > 0
          ? (entry as any).reduzierung
              .map((redu: Record<string, any>) => {
                if (!redu || typeof redu !== "object") {
                  return null;
                }
                const category = normalizeText(redu.kategorie);
                const value = normalizeText(redu.redu_abstand ?? redu.wert);
                const parts: string[] = [];
                if (category) {
                  parts.push(escapeHtml(category));
                }
                if (value) {
                  parts.push(escapeHtml(value));
                }
                if (!parts.length) {
                  return null;
                }
                return `<span class="badge bg-light text-dark border">${parts.join(" – ")}</span>`;
              })
              .filter((redu: string | null): redu is string => Boolean(redu))
              .join(" ")
          : "";

      const header = code
        ? `<span class="fw-semibold">${escapeHtml(code)}</span>`
        : `<span class="fw-semibold">Auflage</span>`;
      const requirementBadge = requirement
        ? ` <span class="badge bg-secondary">${escapeHtml(requirement)}</span>`
        : "";
      const metaHtml =
        metaParts.length > 0
          ? `<div class="small text-muted">${metaParts.join(" · ")}</div>`
          : "";
      const reductionHtml = reductions
        ? `<div class="mt-1 d-flex flex-wrap gap-1">${reductions}</div>`
        : "";

      return `<li>${header}${requirementBadge}${metaHtml}${reductionHtml}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-clipboard-check me-1"></i>Auflagen:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultAwgPartner(result: ReportingResult): string {
  if (!Array.isArray(result.awg_partner) || result.awg_partner.length === 0) {
    return "";
  }

  const list = result.awg_partner
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const partnerKennr = normalizeText(entry.partner_kennr ?? entry.kennr);
      const art = normalizeText(entry.mischung_art ?? entry.art);

      if (!partnerKennr && !art) {
        return null;
      }

      const parts: string[] = [];
      if (partnerKennr) {
        parts.push(`Partner: ${escapeHtml(partnerKennr)}`);
      }
      if (art) {
        parts.push(`Art: ${escapeHtml(art)}`);
      }

      return `<li>${parts.join(" · ")}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-diagram-3 me-1"></i>Mischpartner:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultAwgPartnerAufwand(result: ReportingResult): string {
  if (
    !Array.isArray(result.awg_partner_aufwand) ||
    result.awg_partner_aufwand.length === 0
  ) {
    return "";
  }

  const list = result.awg_partner_aufwand
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const partnerKennr = normalizeText(entry.partner_kennr ?? entry.kennr);
      const condition = normalizeText(
        entry.aufwandbedingung ?? entry.aufwand_bedingung
      );
      const amount = formatAmount(
        entry.m_aufwand ?? entry.mittel_menge,
        entry.m_aufwand_einheit ?? entry.mittel_einheit
      );

      const parts: string[] = [];
      if (partnerKennr) {
        parts.push(`Partner: ${escapeHtml(partnerKennr)}`);
      }
      if (condition) {
        parts.push(`Bedingung: ${escapeHtml(condition)}`);
      }
      if (amount) {
        parts.push(`Menge: ${escapeHtml(amount)}`);
      }

      if (!parts.length) {
        return null;
      }

      return `<li>${parts.join(" · ")}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-droplet-half me-1"></i>Mischpartner-Aufwand:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultAwgBemerkungen(result: ReportingResult): string {
  if (
    !Array.isArray(result.awg_bemerkungen) ||
    result.awg_bemerkungen.length === 0
  ) {
    return "";
  }

  const list = result.awg_bemerkungen
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const note = normalizeText(entry.auflage_bem ?? entry.bemerkung);
      const area = normalizeText(entry.auflage_bereich ?? entry.bereich);

      if (!note && !area) {
        return null;
      }

      const parts: string[] = [];
      if (note) {
        parts.push(escapeHtml(note));
      }
      if (area) {
        parts.push(
          `<span class="text-muted">Bereich: ${escapeHtml(area)}</span>`
        );
      }

      return `<li>${parts.join(" · ")}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-chat-left-text me-1"></i>AWG-Bemerkungen:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultAwgVerwendungszwecke(result: ReportingResult): string {
  if (
    !Array.isArray(result.awg_verwendungszwecke) ||
    result.awg_verwendungszwecke.length === 0
  ) {
    return "";
  }

  const unique = Array.from(
    new Set(
      result.awg_verwendungszwecke
        .map((entry) =>
          entry && typeof entry === "object"
            ? normalizeText(entry.verwendungszweck ?? entry.code)
            : ""
        )
        .filter((code) => code)
    )
  );

  if (!unique.length) {
    return "";
  }

  const badges = unique
    .map((code) => `<span class="badge bg-primary">${escapeHtml(code)}</span>`)
    .join(" ");

  return `
    <div class="mt-2">
      <strong><i class="bi bi-list-task me-1"></i>Verwendungszwecke:</strong>
      <div class="d-flex flex-wrap gap-1">${badges}</div>
    </div>
  `;
}

function renderResultAwgWartezeitAusnahmen(result: ReportingResult): string {
  if (
    !Array.isArray(result.awg_wartezeit_ausnahmen) ||
    result.awg_wartezeit_ausnahmen.length === 0
  ) {
    return "";
  }

  const labelMap = new Map<string, string>();
  if (Array.isArray(result.kulturen)) {
    for (const entry of result.kulturen) {
      if (!entry || typeof entry !== "object") {
        continue;
      }
      const code = normalizeText((entry as any).kultur);
      const label = normalizeText((entry as any).label);
      if (code && label) {
        labelMap.set(code, label);
      }
    }
  }
  if (Array.isArray(result.wartezeiten)) {
    for (const entry of result.wartezeiten) {
      if (!entry || typeof entry !== "object") {
        continue;
      }
      const code = normalizeText((entry as any).kultur);
      const label = normalizeText((entry as any).kultur_label);
      if (code && label && !labelMap.has(code)) {
        labelMap.set(code, label);
      }
    }
  }

  const list = result.awg_wartezeit_ausnahmen
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const wartezeitNr = normalizeText(entry.awg_wartezeit_nr);
      const kulturCode = normalizeText(entry.kultur);

      if (!wartezeitNr && !kulturCode) {
        return null;
      }

      const displayLabel = kulturCode
        ? labelMap.get(kulturCode) || kulturCode
        : "";

      const parts: string[] = [];
      if (displayLabel) {
        parts.push(escapeHtml(displayLabel));
      }
      if (wartezeitNr) {
        parts.push(
          `<span class="text-muted">Nr.: ${escapeHtml(wartezeitNr)}</span>`
        );
      }

      return `<li>${parts.join(" · ")}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-hourglass-split me-1"></i>Wartezeit-Ausnahmen:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultAwgZeitpunkte(result: ReportingResult): string {
  if (
    !Array.isArray(result.awg_zeitpunkte) ||
    result.awg_zeitpunkte.length === 0
  ) {
    return "";
  }

  const list = result.awg_zeitpunkte
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const zeitpunkt = normalizeText(entry.zeitpunkt ?? entry.code);
      const sortierNr = normalizeText(entry.sortier_nr);
      const operand = normalizeText(entry.operand_zu_vorher ?? entry.operand);

      if (!zeitpunkt && !sortierNr && !operand) {
        return null;
      }

      const parts: string[] = [];
      if (zeitpunkt) {
        parts.push(escapeHtml(zeitpunkt));
      }
      if (sortierNr) {
        parts.push(
          `<span class="text-muted">Pos.: ${escapeHtml(sortierNr)}</span>`
        );
      }
      if (operand) {
        parts.push(
          `<span class="text-muted">Operand: ${escapeHtml(operand)}</span>`
        );
      }

      return `<li>${parts.join(" · ")}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-calendar-week me-1"></i>Anwendungszeitpunkte:</strong>
      <ul class="small mb-0">${list}</ul>
    </div>
  `;
}

function renderResultAwgZulassung(result: ReportingResult): string {
  if (
    !Array.isArray(result.awg_zulassung) ||
    result.awg_zulassung.length === 0
  ) {
    return "";
  }

  const list = result.awg_zulassung
    .map((entry) => {
      if (!entry || typeof entry !== "object") {
        return null;
      }

      const end = formatDateHtml(entry.zul_ende ?? entry.gueltig_bis);
      if (!end) {
        return null;
      }

      return `<li>Gültig bis ${end}</li>`;
    })
    .filter((entry): entry is string => Boolean(entry))
    .join("");

  if (!list) {
    return "";
  }

  return `
    <div class="mt-2">
      <strong><i class="bi bi-calendar-check me-1"></i>AWG-Zulassung:</strong>
      <ul class="small mb-0">${list}</ul>
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
