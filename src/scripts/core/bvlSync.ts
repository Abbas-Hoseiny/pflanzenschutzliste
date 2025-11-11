// @ts-nocheck
/**
 * BVL Sync Orchestrator
 * Coordinates fetching and storing BVL data with progress reporting
 * Supports manifest-based sync (default) and optional direct API sync for development
 */

import { fetchCollection, computeDatasetHashes } from "./bvlClient.js";
import {
  fetchManifest,
  downloadDatabase,
  ManifestError,
  DownloadError,
} from "./bvlDataset.js";

const USE_MANIFEST_SYNC = true; // Feature flag for manifest-based sync

const SYNC_ENDPOINTS = [
  { key: "mittel", endpoint: "mittel", category: "core", label: "Mittel" },
  { key: "awg", endpoint: "awg", category: "core", label: "Anwendungen" },
  {
    key: "awg_kultur",
    endpoint: "awg_kultur",
    category: "core",
    label: "Anwendungs-Kulturen",
  },
  {
    key: "awg_schadorg",
    endpoint: "awg_schadorg",
    category: "core",
    label: "Anwendungs-Schadorganismen",
  },
  {
    key: "awg_aufwand",
    endpoint: "awg_aufwand",
    category: "core",
    label: "Anwendungs-Aufwände",
  },
  {
    key: "awg_wartezeit",
    endpoint: "awg_wartezeit",
    category: "core",
    label: "Wartezeiten",
  },
  {
    key: "adresse",
    endpoint: "adresse",
    category: "payload",
    label: "Adressen",
    primaryRefField: "ADRESSE_NR",
  },
  {
    key: "antrag",
    endpoint: "antrag",
    category: "payload",
    label: "Anträge",
    primaryRefField: "KENNR",
    secondaryRefField: "ANTRAGNR",
  },
  {
    key: "auflage_redu",
    endpoint: "auflage_redu",
    category: "payload",
    label: "Auflagen reduziert",
    primaryRefField: "AUFLAGENR",
  },
  {
    key: "auflagen",
    endpoint: "auflagen",
    category: "payload",
    label: "Auflagen",
    primaryRefField: "KENNR",
    secondaryRefField: "EBENE",
  },
  {
    key: "awg_bem",
    endpoint: "awg_bem",
    category: "payload",
    label: "Anwendungs-Bemerkungen",
    primaryRefField: "AWG_ID",
  },
  {
    key: "awg_partner",
    endpoint: "awg_partner",
    category: "payload",
    label: "Anwendungs-Partner",
    primaryRefField: "AWG_ID",
    secondaryRefField: "KENNR_PARTNER",
  },
  {
    key: "awg_partner_aufwand",
    endpoint: "awg_partner_aufwand",
    category: "payload",
    label: "Partner-Aufwände",
    primaryRefField: "AWG_ID",
    secondaryRefField: "KENNR_PARTNER",
  },
  {
    key: "awg_verwendungszweck",
    endpoint: "awg_verwendungszweck",
    category: "payload",
    label: "Verwendungszwecke",
    primaryRefField: "AWG_ID",
  },
  {
    key: "awg_wartezeit_ausg_kultur",
    endpoint: "awg_wartezeit_ausg_kultur",
    category: "payload",
    label: "Wartezeit-Ausnahmen",
    primaryRefField: "AWG_ID",
  },
  {
    key: "awg_zeitpunkt",
    endpoint: "awg_zeitpunkt",
    category: "payload",
    label: "Anwendungszeitpunkte",
    primaryRefField: "AWG_ID",
  },
  {
    key: "awg_zulassung",
    endpoint: "awg_zulassung",
    category: "payload",
    label: "Zulassungsdetails",
    primaryRefField: "AWG_ID",
  },
  {
    key: "ghs_gefahrenhinweise",
    endpoint: "ghs_gefahrenhinweise",
    category: "payload",
    label: "GHS Gefahrenhinweise",
    primaryRefField: "KENNR",
  },
  {
    key: "ghs_gefahrensymbole",
    endpoint: "ghs_gefahrensymbole",
    category: "payload",
    label: "GHS Symbole",
    primaryRefField: "KENNR",
  },
  {
    key: "ghs_sicherheitshinweise",
    endpoint: "ghs_sicherheitshinweise",
    category: "payload",
    label: "GHS Sicherheitshinweise",
    primaryRefField: "KENNR",
  },
  {
    key: "ghs_signalwoerter",
    endpoint: "ghs_signalwoerter",
    category: "payload",
    label: "GHS Signalwörter",
    primaryRefField: "KENNR",
  },
  {
    key: "hinweis",
    endpoint: "hinweis",
    category: "payload",
    label: "Hinweise",
    primaryRefField: "KENNR",
  },
  {
    key: "kodeliste",
    endpoint: "kodeliste",
    category: "payload",
    label: "Kodlisten",
    primaryRefField: "KODELISTE_NR",
  },
  {
    key: "kodeliste_feldname",
    endpoint: "kodeliste_feldname",
    category: "payload",
    label: "Feldnamen",
    primaryRefField: "FELD",
  },
  {
    key: "kultur_gruppe",
    endpoint: "kultur_gruppe",
    category: "payload",
    label: "Kulturgruppen",
    primaryRefField: "GRUPPE",
  },
  {
    key: "mittel_abgelaufen",
    endpoint: "mittel_abgelaufen",
    category: "payload",
    label: "Abgelaufene Mittel",
    primaryRefField: "KENNR",
  },
  {
    key: "mittel_abpackung",
    endpoint: "mittel_abpackung",
    category: "payload",
    label: "Abpackungen",
    primaryRefField: "KENNR",
  },
  {
    key: "mittel_gefahren_symbol",
    endpoint: "mittel_gefahren_symbol",
    category: "payload",
    label: "Gefahrensymbole",
    primaryRefField: "KENNR",
  },
  {
    key: "mittel_vertrieb",
    endpoint: "mittel_vertrieb",
    category: "payload",
    label: "Vertrieb",
    primaryRefField: "KENNR",
    secondaryRefField: "ADRESSE_NR",
  },
  {
    key: "mittel_wirkbereich",
    endpoint: "mittel_wirkbereich",
    category: "payload",
    label: "Wirkbereiche",
    primaryRefField: "KENNR",
  },
  {
    key: "parallelimport_abgelaufen",
    endpoint: "parallelimport_abgelaufen",
    category: "payload",
    label: "Parallelimport (alt)",
    primaryRefField: "KENNR",
  },
  {
    key: "parallelimport_gueltig",
    endpoint: "parallelimport_gueltig",
    category: "payload",
    label: "Parallelimport (gültig)",
    primaryRefField: "KENNR",
  },
  {
    key: "schadorg_gruppe",
    endpoint: "schadorg_gruppe",
    category: "payload",
    label: "Schadorganismus-Gruppen",
    primaryRefField: "GRUPPE",
  },
  {
    key: "staerkung",
    endpoint: "staerkung",
    category: "payload",
    label: "Pflanzenstärkung",
    primaryRefField: "KENNR",
  },
  {
    key: "staerkung_vertrieb",
    endpoint: "staerkung_vertrieb",
    category: "payload",
    label: "Pflanzenstärkung Vertrieb",
    primaryRefField: "KENNR",
    secondaryRefField: "ADRESSE_NR",
  },
  {
    key: "stand",
    endpoint: "stand",
    category: "payload",
    label: "API Stand",
  },
  {
    key: "wirkstoff",
    endpoint: "wirkstoff",
    category: "payload",
    label: "Wirkstoffe",
    primaryRefField: "KENNR",
  },
  {
    key: "wirkstoff_gehalt",
    endpoint: "wirkstoff_gehalt",
    category: "payload",
    label: "Wirkstoffgehalt",
    primaryRefField: "KENNR",
  },
  {
    key: "zusatzstoff",
    endpoint: "zusatzstoff",
    category: "payload",
    label: "Zusatzstoffe",
    primaryRefField: "KENNR",
  },
  {
    key: "zusatzstoff_vertrieb",
    endpoint: "zusatzstoff_vertrieb",
    category: "payload",
    label: "Zusatzstoff Vertrieb",
    primaryRefField: "KENNR",
    secondaryRefField: "ADRESSE_NR",
  },
];

const LOOKUP_CONFIG = [
  {
    key: "lookupCultures",
    endpoint: "kode",
    params: { kodeliste: 948, sprache: "DE" },
    progressKey: "lookup:kulturen",
    label: "Kulturen (Klartexte)",
  },
  {
    key: "lookupPests",
    endpoint: "kode",
    params: { kodeliste: 947, sprache: "DE" },
    progressKey: "lookup:schadorg",
    label: "Schadorganismen (Klartexte)",
  },
];

function extractFieldValue(item, fieldName) {
  if (!item || !fieldName) {
    return null;
  }

  const candidates = [
    fieldName,
    fieldName.toLowerCase(),
    fieldName.toUpperCase(),
  ];

  for (const key of candidates) {
    if (Object.prototype.hasOwnProperty.call(item, key)) {
      const value = item[key];
      if (value === null || value === undefined || value === "") {
        return null;
      }
      return String(value);
    }
  }

  return null;
}

/**
 * Sync BVL data with progress and logging callbacks
 */
export async function syncBvlData(storage, options = {}) {
  const { onProgress = () => {}, onLog = () => {} } = options;

  const startTime = Date.now();
  const log = (level, message, data = null) => {
    onLog({
      level,
      message,
      data,
      timestamp: new Date().toISOString(),
    });
  };

  try {
    onProgress({
      step: "start",
      percent: 0,
      message: "Starte Synchronisation...",
    });
    log("info", "Starting BVL data sync");

    if (!navigator.onLine) {
      throw new Error("Keine Internetverbindung verfügbar");
    }

    // Use manifest-based sync by default
    if (USE_MANIFEST_SYNC) {
      return await syncFromManifest(storage, {
        onProgress,
        onLog,
        log,
        startTime,
      });
    }

    // Optional: direct API sync (development/testing only)
    return await syncFromApi(storage, { onProgress, onLog, log, startTime });
  } catch (error) {
    const errorMessage = error.message || "Unbekannter Fehler";
    log("error", "Sync failed", { error: errorMessage, stack: error.stack });

    const meta = {
      lastError: errorMessage,
      lastErrorTime: new Date().toISOString(),
    };

    try {
      await storage.setBvlMeta("lastError", errorMessage);
      await storage.appendBvlSyncLog({
        synced_at: new Date().toISOString(),
        ok: 0,
        message: errorMessage,
        payload_hash: null,
      });
    } catch (e) {
      log("error", "Failed to write error state", { error: e.message });
    }

    onProgress({
      step: "error",
      percent: 0,
      message: `Fehler: ${errorMessage}`,
    });

    throw error;
  }
}

/**
 * Sync from manifest-based database
 */
async function syncFromManifest(
  storage,
  { onProgress, onLog, log, startTime }
) {
  onProgress({
    step: "manifest",
    percent: 5,
    message: "Lade Manifest...",
  });
  log("info", "Fetching manifest");

  const manifest = await fetchManifest();
  log("info", "Manifest loaded", {
    version: manifest.version,
    files: manifest.files.length,
  });

  // Check if we need to update
  const previousHash = await storage.getBvlMeta("lastSyncHash");
  const manifestHash = manifest.hash || manifest.version;

  if (previousHash === manifestHash) {
    onProgress({
      step: "done",
      percent: 100,
      message: "Keine Aktualisierung erforderlich",
    });

    const meta = {
      lastSyncHash: manifestHash,
      lastSyncIso: new Date().toISOString(),
      lastSyncCounts: manifest.counts || manifest.tables || {},
      lastError: null,
    };

    await storage.appendBvlSyncLog({
      synced_at: meta.lastSyncIso,
      ok: 1,
      message: "no-change (manifest)",
      payload_hash: manifestHash,
    });

    log("info", "No changes detected, sync complete");
    return { status: "no-change", meta, manifest };
  }

  onProgress({
    step: "download",
    percent: 10,
    message: "Lade Datenbank...",
  });
  log("info", "Downloading database from manifest");

  const { data, format } = await downloadDatabase(manifest, {
    onProgress: (progress) => {
      if (progress.step === "download") {
        onProgress({
          step: "download",
          percent: 10 + Math.round(progress.percent * 0.5), // 10-60%
          message: progress.message,
        });
      } else if (progress.step === "decompress") {
        onProgress({
          step: "decompress",
          percent: 60 + Math.round((progress.percent - 70) * 0.2), // 60-70%
          message: progress.message,
        });
      }
    },
  });

  log("info", `Database downloaded (${format}, ${data.length} bytes)`);

  onProgress({
    step: "import",
    percent: 70,
    message: "Importiere Datenbank...",
  });
  log("info", "Importing database");

  const importStart = Date.now();
  const result = await storage.importBvlSqlite(data, manifest);
  const importDuration = Date.now() - importStart;

  log("info", `Database import complete in ${importDuration}ms`, {
    counts: result.counts,
  });

  onProgress({
    step: "verify",
    percent: 95,
    message: "Verifiziere Daten...",
  });
  log("info", "Verifying data");

  const totalDuration = Date.now() - startTime;
  await storage.appendBvlSyncLog({
    synced_at: new Date().toISOString(),
    ok: 1,
    message: `success (manifest, ${totalDuration}ms)`,
    payload_hash: manifestHash,
  });

  onProgress({
    step: "done",
    percent: 100,
    message: "Synchronisation abgeschlossen",
  });

  const meta = {
    lastSyncHash: manifestHash,
    lastSyncIso: new Date().toISOString(),
    lastSyncCounts: result.counts,
    dataSource: `pflanzenschutzliste-data@${manifest.version}`,
    manifestVersion: manifest.version,
    apiStand: manifest.api_version || manifest.build?.finished_at || null,
    lastError: null,
  };

  log("info", `Sync complete in ${totalDuration}ms`, { meta, manifest });

  return { status: "success", meta, manifest };
}

/**
 * Sync from direct API (fallback)
 */
async function syncFromApi(storage, { onProgress, onLog, log, startTime }) {
  const datasets = {};
  const fetchTimes = {};
  let totalProgress = 0;
  const fetchTasks = [
    ...SYNC_ENDPOINTS.map((definition) => ({
      type: definition.category === "core" ? "dataset" : "payload",
      definition,
    })),
    ...LOOKUP_CONFIG.map((lookup) => ({ type: "lookup", ...lookup })),
  ];
  const progressPerTask = 70 / fetchTasks.length;

  try {
    for (const task of fetchTasks) {
      const taskStart = Date.now();
      let progressKey;
      let message;
      if (task.type === "dataset" || task.type === "payload") {
        progressKey = task.definition.endpoint;
        message = `Lade ${task.definition.label || task.definition.endpoint}...`;
      } else {
        progressKey = task.progressKey;
        message = `Lade ${task.label}...`;
      }

      onProgress({
        step: `fetch:${progressKey}`,
        percent: Math.round(10 + totalProgress),
        message,
      });

      try {
        const items = await fetchCollection(
          task.type === "lookup" ? task.endpoint : task.definition.endpoint,
          {
            params:
              task.type === "lookup" ? task.params : task.definition?.params,
            onProgress: (progress) => {
              const keyLabel =
                task.type === "lookup"
                  ? task.progressKey
                  : task.definition?.endpoint;
              log("debug", `Fetch progress for ${keyLabel}`, progress);
            },
          }
        );

        if (task.type === "lookup") {
          const datasetKey = task.key;
          datasets[datasetKey] = items;
          fetchTimes[datasetKey] = Date.now() - taskStart;
          log(
            "info",
            `Fetched ${items.length} items from ${datasetKey} in ${fetchTimes[datasetKey]}ms`
          );
        } else {
          const datasetKey = task.definition.key;
          datasets[datasetKey] = items;
          fetchTimes[datasetKey] = Date.now() - taskStart;
          log(
            "info",
            `Fetched ${items.length} items from ${task.definition.endpoint} in ${fetchTimes[datasetKey]}ms`
          );
        }
        totalProgress += progressPerTask;
      } catch (error) {
        const identifier =
          task.type === "lookup"
            ? task.label
            : task.definition?.label || task.definition?.endpoint;
        log("error", `Failed to fetch ${identifier}`, {
          error: error.message,
          type: error.name,
          status: error.status,
          attempt: error.attempt,
        });
        throw new Error(
          `Fehler beim Laden von ${identifier}: ${error.message}`
        );
      }
    }

    onProgress({
      step: "transform",
      percent: 80,
      message: "Verarbeite Daten...",
    });
    log("info", "Transforming data");

    const transformed = transformBvlData(datasets);
    const hashes = await computeDatasetHashes(datasets);

    log("info", "Data transformation complete", {
      counts: Object.entries(transformed).reduce((acc, [key, val]) => {
        acc[key] = Array.isArray(val) ? val.length : 0;
        return acc;
      }, {}),
      hashes,
    });

    const previousHash = await storage.getBvlMeta("lastSyncHash");
    if (previousHash === hashes.combined) {
      onProgress({
        step: "done",
        percent: 100,
        message: "Keine Aktualisierung erforderlich",
      });

      const meta = {
        lastSyncHash: hashes.combined,
        lastSyncIso: new Date().toISOString(),
        lastSyncCounts: Object.entries(transformed).reduce(
          (acc, [key, val]) => {
            acc[key] = Array.isArray(val) ? val.length : 0;
            return acc;
          },
          {}
        ),
        lastError: null,
      };

      await storage.appendBvlSyncLog({
        synced_at: meta.lastSyncIso,
        ok: 1,
        message: "no-change",
        payload_hash: hashes.combined,
      });

      log("info", "No changes detected, sync complete");
      return { status: "no-change", meta };
    }

    onProgress({
      step: "write",
      percent: 85,
      message: "Schreibe in Datenbank...",
    });
    log("info", "Writing to database");

    const writeStart = Date.now();
    await storage.importBvlDataset(transformed, {
      hash: hashes.combined,
      fetchTimes,
    });
    const writeDuration = Date.now() - writeStart;

    log("info", `Database write complete in ${writeDuration}ms`);

    onProgress({
      step: "verify",
      percent: 95,
      message: "Verifiziere Daten...",
    });
    log("info", "Verifying data");

    const meta = {
      lastSyncHash: hashes.combined,
      lastSyncIso: new Date().toISOString(),
      lastSyncCounts: Object.entries(transformed).reduce((acc, [key, val]) => {
        acc[key] = Array.isArray(val) ? val.length : 0;
        return acc;
      }, {}),
      lastError: null,
    };

    for (const [key, value] of Object.entries(meta)) {
      await storage.setBvlMeta(
        key,
        typeof value === "object" ? JSON.stringify(value) : value
      );
    }

    const totalDuration = Date.now() - startTime;
    await storage.appendBvlSyncLog({
      synced_at: meta.lastSyncIso,
      ok: 1,
      message: `success (${totalDuration}ms)`,
      payload_hash: hashes.combined,
    });

    onProgress({
      step: "done",
      percent: 100,
      message: "Synchronisation abgeschlossen",
    });
    log("info", `Sync complete in ${totalDuration}ms`, { meta });

    return { status: "success", meta };
  } catch (error) {
    const errorMessage = error.message || "Unbekannter Fehler";
    log("error", "API sync failed", {
      error: errorMessage,
      stack: error.stack,
    });
    throw error;
  }
}

/**
 * Transform raw BVL data into database-ready format
 */
function parseNullableNumber(value) {
  if (value === null || value === undefined) {
    return null;
  }

  if (typeof value === "string") {
    let normalized = value.replace(/\s+/g, "");
    if (normalized === "") {
      return null;
    }

    if (normalized.includes(",")) {
      normalized = normalized.replace(/\./g, "").replace(/,/g, ".");
    }

    if (normalized === "") {
      return null;
    }
    const parsed = Number(normalized);
    return Number.isNaN(parsed) ? null : parsed;
  }

  if (typeof value === "number") {
    return Number.isNaN(value) ? null : value;
  }

  return null;
}

function coalesceValue(...candidates) {
  for (const value of candidates) {
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

function transformBvlData(datasets) {
  const result = {
    mittel: [],
    awg: [],
    awg_kultur: [],
    awg_schadorg: [],
    awg_aufwand: [],
    awg_wartezeit: [],
    culturesLookup: [],
    pestsLookup: [],
    apiPayloads: [],
    payloadCounts: {},
  };

  const endpointLookup = new Map(
    SYNC_ENDPOINTS.map((definition) => [definition.key, definition])
  );

  const pushPayload = (definition, item) => {
    if (!definition) {
      return;
    }
    const primaryRef = definition.primaryRefField
      ? extractFieldValue(item, definition.primaryRefField)
      : null;
    const secondaryRef = definition.secondaryRefField
      ? extractFieldValue(item, definition.secondaryRefField)
      : null;
    const tertiaryRef = definition.tertiaryRefField
      ? extractFieldValue(item, definition.tertiaryRefField)
      : null;

    result.apiPayloads.push({
      endpoint: definition.endpoint,
      key: definition.key,
      primary_ref: primaryRef,
      secondary_ref: secondaryRef,
      tertiary_ref: tertiaryRef,
      payload_json: JSON.stringify(item),
    });

    result.payloadCounts[definition.key] =
      (result.payloadCounts[definition.key] || 0) + 1;
  };

  if (datasets.mittel) {
    result.mittel = datasets.mittel.map((item) => ({
      kennr: item.kennr || "",
      name: item.mittelname || "",
      formulierung: item.formulierung || "",
      zul_erstmalig: item.zul_erstmalig || null,
      zul_ende: item.zul_ende || null,
      geringes_risiko: item.geringes_risiko === "J" ? 1 : 0,
      payload_json: JSON.stringify(item),
    }));
  }

  if (datasets.awg) {
    result.awg = datasets.awg.map((item) => ({
      awg_id: item.awg_id || "",
      kennr: item.kennr || "",
      status_json: JSON.stringify({
        status: item.status || "",
        wachstumsstadium: item.wachstumsstadium || "",
      }),
      zulassungsende: item.zulassungsende || null,
    }));
  }

  if (datasets.awg_kultur) {
    result.awg_kultur = datasets.awg_kultur.map((item) => ({
      awg_id: item.awg_id || "",
      kultur: item.kultur || "",
      ausgenommen: item.ausgenommen === "J" ? 1 : 0,
      sortier_nr: parseInt(item.sortier_nr) || 0,
    }));
  }

  if (datasets.awg_schadorg) {
    result.awg_schadorg = datasets.awg_schadorg.map((item) => ({
      awg_id: item.awg_id || "",
      schadorg: item.schadorg || "",
      ausgenommen: item.ausgenommen === "J" ? 1 : 0,
      sortier_nr: parseInt(item.sortier_nr) || 0,
    }));
  }

  if (datasets.awg_aufwand) {
    result.awg_aufwand = datasets.awg_aufwand.map((item) => ({
      awg_id: item.awg_id || "",
      aufwand_bedingung: item.aufwandbedingung || "",
      sortier_nr: parseInt(item.sortier_nr) || 0,
      mittel_menge: parseNullableNumber(
        coalesceValue(
          item.max_aufwandmenge,
          item.m_aufwand,
          item.m_aufwand_bis,
          item.m_aufwand_von,
          item.aufwandmenge
        )
      ),
      mittel_einheit:
        coalesceValue(
          item.aufwandmenge_einheit,
          item.m_aufwand_einheit,
          item.m_aufwand_bis_einheit,
          item.m_aufwand_von_einheit
        ) || null,
      wasser_menge: parseNullableNumber(
        coalesceValue(
          item.wassermenge,
          item.w_aufwand_bis,
          item.w_aufwand_von,
          item.wasseraufwand
        )
      ),
      wasser_einheit:
        coalesceValue(
          item.wassermenge_einheit,
          item.w_aufwand_einheit,
          item.wasseraufwand_einheit,
          item.w_aufwand_von_einheit,
          item.w_aufwand_bis_einheit
        ) || null,
      payload_json: JSON.stringify(item),
    }));
  }

  if (datasets.awg_wartezeit) {
    result.awg_wartezeit = datasets.awg_wartezeit.map((item) => ({
      awg_wartezeit_nr: parseInt(item.awg_wartezeit_nr) || 0,
      awg_id: item.awg_id || "",
      kultur: item.kultur || "",
      sortier_nr: parseInt(item.sortier_nr) || 0,
      tage: parseInt(item.wartezeit_tage) || null,
      bemerkung_kode: item.bemerkung_kode || null,
      anwendungsbereich: item.anwendungsbereich || null,
      erlaeuterung: item.erlaeuterung || null,
      payload_json: JSON.stringify(item),
    }));
  }

  if (datasets.lookupCultures) {
    result.culturesLookup = datasets.lookupCultures
      .filter((item) =>
        item.sprache ? item.sprache.toUpperCase() === "DE" : true
      )
      .map((item) => ({
        code: item.kode || "",
        label: item.kodetext || item.kode || "",
      }));
  }

  if (datasets.lookupPests) {
    result.pestsLookup = datasets.lookupPests
      .filter((item) =>
        item.sprache ? item.sprache.toUpperCase() === "DE" : true
      )
      .map((item) => ({
        code: item.kode || "",
        label: item.kodetext || item.kode || "",
      }));
  }

  for (const definition of SYNC_ENDPOINTS) {
    if (definition.category !== "payload") {
      continue;
    }
    const items = datasets[definition.key];
    if (!Array.isArray(items) || items.length === 0) {
      continue;
    }
    for (const item of items) {
      pushPayload(definition, item);
    }
  }

  return result;
}
