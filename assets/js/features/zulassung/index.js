/**
 * Zulassung Feature Module
 * Handles BVL approval data display and synchronization
 */

import { syncBvlData } from "../../core/bvlSync.js";
import { checkForUpdates } from "../../core/bvlDataset.js";
import * as storage from "../../core/storage/sqlite.js";

let container = null;
let services = null;
let isSectionVisible = false;

const numberFormatter = new Intl.NumberFormat("de-DE", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 3,
});

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function safeParseJson(value) {
  if (!value) {
    return null;
  }
  try {
    return JSON.parse(value);
  } catch (error) {
    console.warn("Failed to parse JSON payload in Zulassung view", error);
    return null;
  }
}

function firstNonEmpty(...values) {
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

function coerceNumber(value) {
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

function formatAmount(value, unit) {
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

function renderIfVisible() {
  if (isSectionVisible) {
    render();
  }
}

function renderAufwandRow(aufwand) {
  const payload = safeParseJson(aufwand.payload_json) || {};

  const mittelUnit = firstNonEmpty(
    aufwand.mittel_einheit,
    aufwand.aufwandmenge_einheit,
    aufwand.mittel_unit,
    payload.aufwandmenge_einheit,
    payload.m_aufwand_einheit,
    payload.max_aufwandmenge_einheit
  );

  const mittelValue = firstNonEmpty(
    aufwand.mittel_menge,
    aufwand.mittel_value,
    aufwand.aufwandmenge,
    payload.m_aufwand,
    payload.max_aufwandmenge,
    payload.m_aufwand_bis,
    payload.m_aufwand_von,
    payload.aufwandmenge
  );

  const mittelFrom = firstNonEmpty(
    aufwand.mittel_menge_min,
    payload.mittel_menge_von,
    payload.aufwandmenge_min,
    payload.m_aufwand_von,
    payload.max_aufwandmenge_von
  );
  const mittelTo = firstNonEmpty(
    aufwand.mittel_menge_max,
    payload.mittel_menge_bis,
    payload.aufwandmenge_max,
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
    aufwand.wasser_unit,
    payload.wassermenge_einheit,
    payload.w_aufwand_einheit,
    payload.w_aufwand_von_einheit,
    payload.w_aufwand_bis_einheit
  );

  const wasserValue = firstNonEmpty(
    aufwand.wasser_menge,
    aufwand.wasser_value,
    aufwand.wassermenge,
    payload.wassermenge,
    payload.w_aufwand,
    payload.w_aufwand_bis,
    payload.w_aufwand_von,
    payload.wasseraufwand
  );

  const wasserFrom = firstNonEmpty(
    aufwand.wasser_menge_min,
    payload.wasser_menge_von,
    payload.wassermenge_min,
    payload.w_aufwand_von,
    payload.wassermenge_von
  );
  const wasserTo = firstNonEmpty(
    aufwand.wasser_menge_max,
    payload.wasser_menge_bis,
    payload.wassermenge_max,
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

export function initZulassung(mainContainer, appServices) {
  container = mainContainer;
  services = appServices;

  // Initialize section
  render();

  // Subscribe to state changes for visibility toggle
  services.state.subscribe((state) => {
    toggleVisibility(state);
  });

  services.events.subscribe("database:connected", async () => {
    await loadInitialData();

    // Perform auto-update check after loading initial data
    setTimeout(() => {
      performAutoUpdateCheck();
    }, 2000);
  });

  toggleVisibility(services.state.getState());
}

async function performAutoUpdateCheck() {
  try {
    const state = services.state.getState();
    const currentHash = state.zulassung.lastSyncHash;

    if (!currentHash) {
      // No sync yet, skip auto-update check
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

function toggleVisibility(state) {
  const section = container.querySelector('[data-section="zulassung"]');
  if (!section) return;

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

function hide() {
  const existingSection = container.querySelector('[data-section="zulassung"]');
  if (existingSection) {
    existingSection.style.display = "none";
  }
}

async function loadInitialData() {
  try {
    // Check if storage is available (worker initialized)
    if (!storage.isSupported || !storage.isSupported()) {
      return;
    }

    const lastSync = await storage.getBvlMeta("lastSyncIso");
    const lastSyncCounts = await storage.getBvlMeta("lastSyncCounts");
    const dataSource = await storage.getBvlMeta("dataSource");
    const apiStand = await storage.getBvlMeta("apiStand");
    const manifestVersion = await storage.getBvlMeta("manifestVersion");
    const lastSyncHash = await storage.getBvlMeta("lastSyncHash");
    const manifestJson = await storage.getBvlMeta("manifest");

    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      lastSync: lastSync || null,
      lastResultCounts: lastSyncCounts ? JSON.parse(lastSyncCounts) : null,
      dataSource: dataSource || null,
      apiStand: apiStand || null,
      manifestVersion: manifestVersion || null,
      lastSyncHash: lastSyncHash || null,
      debug: {
        ...prev.debug,
        manifest: manifestJson ? JSON.parse(manifestJson) : null,
      },
    }));

    const cultures = await storage.listBvlCultures();
    const pests = await storage.listBvlSchadorg();

    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      lookups: { cultures, pests },
    }));

    renderIfVisible();
  } catch (error) {
    console.error("Failed to load initial Zulassung data:", error);
  }
}

function render() {
  let section = container.querySelector('[data-section="zulassung"]');

  if (!section) {
    section = document.createElement("div");
    section.setAttribute("data-section", "zulassung");
    section.className = "section-content";
    container.appendChild(section);
  }

  const state = services.state.getState();
  const { zulassung } = state;

  section.innerHTML = `
    <div class="container py-4">
      <h2>BVL Zulassungsdaten</h2>
      
      ${renderStatusSection(zulassung)}
      ${renderSyncSection(zulassung)}
      ${renderFilterSection(zulassung)}
      ${renderResultsSection(zulassung)}
      ${renderDebugSection(zulassung)}
    </div>
  `;

  attachEventHandlers(section);
}

function renderStatusSection(zulassung) {
  if (!zulassung.lastSync) {
    return `
      <div class="alert alert-info mb-3">
        <i class="bi bi-info-circle-fill me-2"></i>
        <strong>Keine Daten vorhanden.</strong> Bitte führen Sie eine Synchronisation durch, um BVL-Daten zu laden.
      </div>
    `;
  }

  const lastSyncDate = new Date(zulassung.lastSync).toLocaleString("de-DE");
  const counts = zulassung.lastResultCounts || {};
  const dataSource = zulassung.dataSource || "BVL API";
  const apiStand = zulassung.apiStand || null;
  const manifestVersion = zulassung.manifestVersion || null;
  const lastSyncHash = zulassung.lastSyncHash || null;

  const totalMittel = counts.mittel || counts.bvl_mittel || 0;

  return `
    <div class="alert alert-success mb-3">
      <div class="d-flex justify-content-between align-items-start">
        <div>
          <i class="bi bi-check-circle-fill me-2"></i>
          <strong>Letzte Synchronisation:</strong> ${lastSyncDate}<br>
          <strong>Datenquelle:</strong> ${escapeHtml(dataSource)}<br>
          ${manifestVersion ? `<strong>Version:</strong> ${escapeHtml(manifestVersion)}<br>` : ""}
          ${apiStand ? `<strong>API-Stand:</strong> ${escapeHtml(apiStand)}<br>` : ""}
          ${lastSyncHash ? `<small class="text-muted">Hash: ${escapeHtml(lastSyncHash.substring(0, 12))}...</small><br>` : ""}
          <small class="mt-1 d-block">
            <i class="bi bi-database me-1"></i>
            Mittel: ${totalMittel}, 
            Anwendungen: ${counts.awg || counts.bvl_awg || 0}, 
            Kulturen: ${counts.awg_kultur || counts.bvl_awg_kultur || 0}, 
            Schadorganismen: ${counts.awg_schadorg || counts.bvl_awg_schadorg || 0}
          </small>
        </div>
      </div>
    </div>
  `;
}

function renderSyncSection(zulassung) {
  const isBusy = zulassung.busy;
  const progress = zulassung.progress;
  const error = zulassung.error;

  // Map progress steps to icons and colors
  const stepInfo = {
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
          zulassung.autoUpdateAvailable
            ? `
          <div class="alert alert-warning d-flex align-items-center" role="alert">
            <i class="bi bi-exclamation-triangle-fill me-2"></i>
            <div class="flex-grow-1">
              <strong>Neue Daten verfügbar!</strong><br>
              <small>Version ${escapeHtml(zulassung.autoUpdateVersion || "unbekannt")} ist verfügbar.</small>
            </div>
            <button class="btn btn-warning btn-sm ms-2" id="btn-apply-update">
              <i class="bi bi-download me-1"></i>Jetzt aktualisieren
            </button>
          </div>
        `
            : ""
        }
        
        <button 
          id="btn-sync" 
          class="btn btn-primary" 
          ${isBusy ? "disabled" : ""}
        >
          ${
            isBusy
              ? `<span class="spinner-border spinner-border-sm me-2"></span><i class="${currentStep?.icon || "bi-arrow-repeat"} me-1"></i>`
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
                ${currentStep?.label || "Verarbeite"}: ${escapeHtml(progress.message)}
              </small>
              <small class="text-muted">${progress.percent}%</small>
            </div>
            <div class="progress" style="height: 20px;" role="progressbar" 
                 aria-valuenow="${progress.percent}" 
                 aria-valuemin="0" 
                 aria-valuemax="100"
                 title="${escapeHtml(progress.message)}">
              <div 
                class="progress-bar progress-bar-striped progress-bar-animated ${currentStep?.color || "bg-primary"}" 
                style="width: ${progress.percent}%"
              >
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

function renderFilterSection(zulassung) {
  const { filters, lookups, busy } = zulassung;
  const cultures = Array.isArray(lookups.cultures) ? lookups.cultures : [];
  const pests = Array.isArray(lookups.pests) ? lookups.pests : [];

  return `
    <div class="card mb-3 filter-section">
      <div class="card-body">
        <h5 class="card-title"><i class="bi bi-funnel me-2"></i>Filter</h5>
        
        <div class="row g-3">
          <div class="col-12">
            <label for="filter-text" class="form-label">Schnellsuche</label>
            <input 
              type="search" 
              id="filter-text" 
              class="form-control" 
              placeholder="Mittel, Kultur- oder Schaderreger-Name"
              value="${escapeHtml(filters.text || "")}"
            >
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
                  (c) =>
                    `<option value="${escapeHtml(c.code)}" ${
                      filters.culture === c.code ? "selected" : ""
                    }>${escapeHtml(c.label || c.code)} (${escapeHtml(
                      c.code
                    )})</option>`
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
                  (p) =>
                    `<option value="${escapeHtml(p.code)}" ${
                      filters.pest === p.code ? "selected" : ""
                    }>${escapeHtml(p.label || p.code)} (${escapeHtml(
                      p.code
                    )})</option>`
                )
                .join("")}
            </select>
          </div>
          
        </div>
        
        <div class="form-check mt-3">
          <input 
            class="form-check-input" 
            type="checkbox" 
            id="filter-expired"
            ${filters.includeExpired ? "checked" : ""}
          >
          <label class="form-check-label" for="filter-expired">
            <i class="bi bi-clock-history me-1"></i>
            Abgelaufene Zulassungen einschließen
          </label>
        </div>
        
        <div class="mt-3">
          <button 
            id="btn-search" 
            class="btn btn-primary"
            ${busy ? "disabled" : ""}
          >
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

function renderResultsSection(zulassung) {
  const { results } = zulassung;

  if (results.length === 0) {
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
          ${results.map((result) => renderResultItem(result)).join("")}
        </div>
      </div>
    </div>
  `;
}

function renderResultItem(result) {
  const status = result.status_json ? JSON.parse(result.status_json) : {};

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
        ${
          result.geringes_risiko
            ? '<span class="badge bg-success me-1"><i class="bi bi-shield-check me-1"></i>Geringes Risiko</span>'
            : ""
        }
        ${
          result.zul_ende
            ? `<span class="badge bg-warning text-dark me-1"><i class="bi bi-calendar-event me-1"></i>Gültig bis: ${escapeHtml(
                result.zul_ende
              )}</span>`
            : ""
        }
      </div>
      
      ${
        result.wirkstoffe && result.wirkstoffe.length > 0
          ? `
        <div class="mt-2">
          <strong><i class="bi bi-droplet me-1"></i>Wirkstoffe:</strong>
          <ul class="small mb-0">
            ${result.wirkstoffe
              .map((w) => {
                const gehalt = coerceNumber(w.gehalt);
                const gehaltStr =
                  gehalt !== null
                    ? numberFormatter.format(gehalt)
                    : String(w.gehalt || "");
                return `
                    <li>
                      ${escapeHtml(w.wirkstoff_name || w.wirkstoff || "-")}
                      ${w.gehalt ? ` - ${escapeHtml(gehaltStr)} ${escapeHtml(w.einheit || "")}` : ""}
                    </li>
                  `;
              })
              .join("")}
          </ul>
        </div>
      `
          : ""
      }
      
      ${
        result.vertrieb && result.vertrieb.length > 0
          ? `
        <div class="mt-2">
          <strong><i class="bi bi-building me-1"></i>Hersteller/Vertrieb:</strong>
          ${result.vertrieb
            .map(
              (v) => `
            <div class="small">
              ${escapeHtml(v.hersteller_name || v.firma || "-")}
              ${
                v.website
                  ? ` <a href="${escapeHtml(v.website)}" target="_blank" rel="noopener" class="text-decoration-none"><i class="bi bi-box-arrow-up-right"></i></a>`
                  : ""
              }
            </div>
          `
            )
            .join("")}
        </div>
      `
          : ""
      }
      
      ${
        result.gefahrhinweise && result.gefahrhinweise.length > 0
          ? `
        <div class="mt-2">
          <strong><i class="bi bi-exclamation-triangle me-1"></i>Gefahrenhinweise:</strong>
          <div class="d-flex flex-wrap gap-1">
            ${result.gefahrhinweise
              .map((g) => {
                const code = g.hinweis_kode || g.h_code || g.h_saetze || "";
                const text = g.hinweis_text || g.text || "";
                return `
                    <span class="badge bg-danger" title="${escapeHtml(text)}" data-bs-toggle="tooltip">
                      ${escapeHtml(code)}${text ? ` <i class="bi bi-info-circle-fill ms-1"></i>` : ""}
                    </span>
                  `;
              })
              .join("")}
          </div>
        </div>
      `
          : ""
      }
      
      ${
        result.kulturen && result.kulturen.length > 0
          ? `
        <div class="mt-2">
          <strong><i class="bi bi-flower1 me-1"></i>Kulturen:</strong>
          <div class="d-flex flex-wrap gap-1">
            ${result.kulturen
              .map(
                (k) =>
                  `<span class="badge ${
                    k.ausgenommen ? "bg-danger" : "bg-info"
                  }" title="${escapeHtml(k.kultur)}">${escapeHtml(
                    k.label || k.kultur
                  )}${k.ausgenommen ? " (ausgenommen)" : ""}</span>`
              )
              .join(" ")}
          </div>
        </div>
      `
          : ""
      }
      
      ${
        result.schadorganismen && result.schadorganismen.length > 0
          ? `
        <div class="mt-2">
          <strong><i class="bi bi-bug me-1"></i>Schadorganismen:</strong>
          <div class="d-flex flex-wrap gap-1">
            ${result.schadorganismen
              .map(
                (s) =>
                  `<span class="badge ${
                    s.ausgenommen ? "bg-danger" : "bg-secondary"
                  }" title="${escapeHtml(s.schadorg)}">${escapeHtml(
                    s.label || s.schadorg
                  )}${s.ausgenommen ? " (ausgenommen)" : ""}</span>`
              )
              .join(" ")}
          </div>
        </div>
      `
          : ""
      }
      
      ${
        result.aufwaende && result.aufwaende.length > 0
          ? `
        <div class="mt-2">
          <strong><i class="bi bi-activity me-1"></i>Aufwände:</strong>
          <ul class="small mb-0">
            ${result.aufwaende
              .map(
                (a) => `
              <li>
                ${renderAufwandRow(a)}
              </li>
            `
              )
              .join("")}
          </ul>
        </div>
      `
          : ""
      }
      
      ${
        result.wartezeiten && result.wartezeiten.length > 0
          ? `
        <div class="mt-2">
          <strong><i class="bi bi-clock me-1"></i>Wartezeiten:</strong>
          <ul class="small mb-0">
            ${result.wartezeiten
              .map(
                (w) => `
              <li>
                ${escapeHtml(w.kultur_label || w.kultur)}: ${w.tage || "-"} Tage
                ${
                  w.anwendungsbereich
                    ? ` (${escapeHtml(w.anwendungsbereich)})`
                    : ""
                }
              </li>
            `
              )
              .join("")}
          </ul>
        </div>
      `
          : ""
      }
    </div>
  `;
}

function renderDebugSection(zulassung) {
  const { debug, logs } = zulassung;

  return `
    <div class="card mb-3">
      <div class="card-body">
        <h5 class="card-title">
          <button 
            class="btn btn-sm btn-link text-decoration-none p-0" 
            type="button" 
            data-bs-toggle="collapse" 
            data-bs-target="#debug-panel"
          >
            <i class="bi bi-tools me-2"></i>Debug-Informationen ▼
          </button>
        </h5>
        
        <div class="collapse" id="debug-panel">
          ${
            debug.manifest
              ? `
            <div class="mt-3">
              <h6><i class="bi bi-file-code me-1"></i>Manifest</h6>
              <details>
                <summary class="btn btn-sm btn-outline-secondary">JSON anzeigen</summary>
                <pre class="bg-dark text-light p-2 mt-2" style="font-size: 11px; max-height: 300px; overflow-y: auto;">${JSON.stringify(
                  debug.manifest,
                  null,
                  2
                )}</pre>
              </details>
            </div>
          `
              : ""
          }
          
          ${
            debug.lastAutoUpdateCheck
              ? `
            <div class="mt-3">
              <h6><i class="bi bi-clock-history me-1"></i>Letzter Auto-Update-Check</h6>
              <p class="small mb-0">
                <strong>Zeit:</strong> ${new Date(
                  debug.lastAutoUpdateCheck.time
                ).toLocaleString("de-DE")}<br>
                <strong>Ergebnis:</strong> ${escapeHtml(
                  debug.lastAutoUpdateCheck.result || "OK"
                )}
              </p>
            </div>
          `
              : ""
          }
          
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
                <tbody>
                  ${
                    debug.lastSyncLog && debug.lastSyncLog.length > 0
                      ? debug.lastSyncLog
                          .map(
                            (log) => `
                      <tr>
                        <td><small>${new Date(log.synced_at).toLocaleString(
                          "de-DE"
                        )}</small></td>
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
                      : '<tr><td colspan="3" class="text-muted">Keine Logs vorhanden</td></tr>'
                  }
                </tbody>
              </table>
            </div>
          </div>
          
          ${
            logs.length > 0
              ? `
            <div class="mt-3">
              <h6><i class="bi bi-terminal me-1"></i>Aktuelle Session Logs</h6>
              <div class="bg-dark text-light p-2" style="max-height: 200px; overflow-y: auto; font-family: monospace; font-size: 12px;">
                ${logs
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
                    return `<div><span class="badge ${badgeClass} me-1">${log.level.toUpperCase()}</span> ${escapeHtml(
                      log.message
                    )}</div>`;
                  })
                  .join("")}
              </div>
            </div>
          `
              : ""
          }
          
          ${
            debug.schema
              ? `
            <div class="mt-3">
              <h6><i class="bi bi-diagram-3 me-1"></i>Schema-Informationen</h6>
              <p><strong>User Version:</strong> ${debug.schema.user_version}</p>
              <details>
                <summary class="btn btn-sm btn-outline-secondary">Tabellen anzeigen</summary>
                <pre class="bg-dark text-light p-2 mt-2" style="font-size: 11px; max-height: 400px; overflow-y: auto;">${JSON.stringify(
                  debug.schema.tables,
                  null,
                  2
                )}</pre>
              </details>
            </div>
          `
              : ""
          }
        </div>
      </div>
    </div>
  `;
}

function attachEventHandlers(section) {
  const btnSync = section.querySelector("#btn-sync");
  const btnSearch = section.querySelector("#btn-search");
  const btnClearFilters = section.querySelector("#btn-clear-filters");
  const btnShowDebug = section.querySelector("#btn-show-debug");

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
      const debugPanel = section.querySelector("#debug-panel");
      if (debugPanel) {
        const collapse = new bootstrap.Collapse(debugPanel, { toggle: true });
      }
    });
  }

  const filterCulture = section.querySelector("#filter-culture");
  const filterPest = section.querySelector("#filter-pest");
  const filterText = section.querySelector("#filter-text");
  const filterExpired = section.querySelector("#filter-expired");
  const btnApplyUpdate = section.querySelector("#btn-apply-update");

  if (filterCulture) {
    filterCulture.addEventListener("change", (e) => {
      services.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, culture: e.target.value || null },
      }));
    });
  }

  if (filterPest) {
    filterPest.addEventListener("change", (e) => {
      services.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, pest: e.target.value || null },
      }));
    });
  }

  if (filterText) {
    filterText.addEventListener("input", (e) => {
      services.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, text: e.target.value },
      }));
    });

    filterText.addEventListener("keydown", (e) => {
      if (e.key === "Enter" && !services.state.getState().zulassung.busy) {
        handleSearch();
      }
    });
  }

  if (filterExpired) {
    filterExpired.addEventListener("change", (e) => {
      services.state.updateSlice("zulassung", (prev) => ({
        ...prev,
        filters: { ...prev.filters, includeExpired: e.target.checked },
      }));
    });
  }

  if (btnApplyUpdate) {
    btnApplyUpdate.addEventListener("click", () => {
      // Trigger sync which will download the new version
      handleSync();
    });
  }
}

async function handleSync() {
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
      onProgress: (progress) => {
        services.state.updateSlice("zulassung", (prev) => ({
          ...prev,
          progress,
        }));
        render();
      },
      onLog: (log) => {
        services.state.updateSlice("zulassung", (prev) => ({
          ...prev,
          logs: [...prev.logs, log],
        }));
      },
    });

    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      busy: false,
      lastSync: result.meta.lastSyncIso,
      lastResultCounts: result.meta.lastSyncCounts,
      dataSource: result.meta.dataSource,
      apiStand: result.meta.apiStand,
      manifestVersion: result.meta.manifestVersion,
      lastSyncHash: result.meta.lastSyncHash,
      progress: { step: null, percent: 0, message: "" },
      autoUpdateAvailable: false,
      autoUpdateVersion: null,
    }));

    await loadInitialData();

    const syncLog = await storage.listBvlSyncLog({ limit: 10 });
    const schema = await storage.diagnoseBvlSchema();

    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      debug: {
        ...prev.debug,
        schema,
        lastSyncLog: syncLog,
      },
    }));

    render();
  } catch (error) {
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      busy: false,
      error: error.message,
      progress: { step: null, percent: 0, message: "" },
    }));

    render();
  }
}

async function handleSearch() {
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
  } catch (error) {
    services.state.updateSlice("zulassung", (prev) => ({
      ...prev,
      busy: false,
      error: error.message,
    }));

    render();
  }
}

function handleClearFilters() {
  services.state.updateSlice("zulassung", (prev) => ({
    ...prev,
    filters: { culture: null, pest: null, text: "", includeExpired: false },
    results: [],
  }));

  render();
}
