/**
 * BVL Sync Orchestrator
 * Coordinates fetching and storing BVL data with progress reporting
 * Supports both manifest-based (preferred) and direct API sync (fallback)
 */

import { fetchCollection, computeDatasetHashes } from "./bvlClient.js";
import {
  fetchManifest,
  downloadDatabase,
  ManifestError,
  DownloadError,
} from "./bvlDataset.js";

const USE_MANIFEST_SYNC = true; // Feature flag for manifest-based sync

const ENDPOINTS = [
  "mittel",
  "awg",
  "awg_kultur",
  "awg_schadorg",
  "awg_aufwand",
  "awg_wartezeit",
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

    // Try manifest-based sync first if enabled
    if (USE_MANIFEST_SYNC) {
      try {
        return await syncFromManifest(storage, { onProgress, onLog, log, startTime });
      } catch (error) {
        log("warn", "Manifest-based sync failed, falling back to API sync", {
          error: error.message,
        });
        // Fall through to API sync
      }
    }

    // Fallback to direct API sync
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
async function syncFromManifest(storage, { onProgress, onLog, log, startTime }) {
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
      lastSyncCounts: manifest.tables || {},
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
    dataSource: `pflanzenschutz-db@${manifest.version}`,
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
    ...ENDPOINTS.map((endpoint) => ({ type: "dataset", endpoint })),
    ...LOOKUP_CONFIG.map((lookup) => ({ type: "lookup", ...lookup })),
  ];
  const progressPerTask = 70 / fetchTasks.length;

  try {
    for (const task of fetchTasks) {
      const taskStart = Date.now();
      const progressStep =
        task.type === "dataset"
          ? `fetch:${task.endpoint}`
          : `fetch:${task.progressKey}`;
      const message =
        task.type === "dataset"
          ? `Lade ${task.endpoint}...`
          : `Lade ${task.label}...`;

      onProgress({
        step: progressStep,
        percent: Math.round(10 + totalProgress),
        message,
      });

      try {
        const items = await fetchCollection(
          task.type === "dataset" ? task.endpoint : task.endpoint,
          {
            params: task.type === "lookup" ? task.params : undefined,
            onProgress: (progress) => {
              const progressKey =
                task.type === "dataset" ? task.endpoint : task.progressKey;
              log("debug", `Fetch progress for ${progressKey}`, progress);
            },
          }
        );

        const datasetKey = task.type === "dataset" ? task.endpoint : task.key;
        datasets[datasetKey] = items;
        fetchTimes[datasetKey] = Date.now() - taskStart;

        const identifier =
          task.type === "dataset" ? datasetKey : task.progressKey;
        log(
          "info",
          `Fetched ${items.length} items from ${identifier} in ${fetchTimes[datasetKey]}ms`
        );
        totalProgress += progressPerTask;
      } catch (error) {
        const identifier = task.type === "dataset" ? task.endpoint : task.label;
        log("error", `Failed to fetch ${identifier}`, {
          error: error.message,
          type: error.name,
          status: error.status,
          attempt: error.attempt,
        });
        throw new Error(`Fehler beim Laden von ${identifier}: ${error.message}`);
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
    log("error", "API sync failed", { error: errorMessage, stack: error.stack });
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

  return result;
}
