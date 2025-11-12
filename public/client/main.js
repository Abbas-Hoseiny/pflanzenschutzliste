var __defProp = Object.defineProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};

// src/config/defaults.json
var defaults_default = {
  meta: {
    version: 1,
    company: {
      name: "Bio-Pflanzenschutz \u2013 All in One",
      headline: "Bio-Pflanzenschutz \u2013 All in One",
      logoUrl: "https://jungpflanzen.bio/wp-content/uploads/2024/01/Baerthele_Jungpflanzen_Markenzeichen.svg",
      contactEmail: "abbas.hoseiny@gmx.de",
      address: ""
    },
    defaults: {
      waterPerKisteL: 5,
      kistenProAr: 300,
      form: {
        creator: "",
        location: "",
        crop: "",
        quantity: ""
      }
    },
    measurementMethods: [
      {
        id: "perKiste",
        label: "pro Kiste",
        type: "factor",
        unit: "Kiste",
        requires: ["kisten"],
        config: {
          sourceField: "kisten"
        }
      },
      {
        id: "percentWater",
        label: "% vom Wasser",
        type: "percentOf",
        unit: "Liter",
        requires: ["kisten", "waterVolume"],
        config: {
          baseField: "waterVolume"
        }
      }
    ]
  },
  mediums: [
    {
      id: "water",
      name: "Wasser",
      unit: "L",
      methodId: "perKiste",
      value: 0.0166
    },
    {
      id: "elotVis",
      name: "Elot-Vis",
      unit: "ml",
      methodId: "perKiste",
      value: 0.83
    },
    {
      id: "schachtelhalm",
      name: "Schachtelhalm",
      unit: "ml",
      methodId: "perKiste",
      value: 0.83
    }
  ],
  history: []
};

// src/scripts/core/labels.ts
var DEFAULT_FIELD_LABELS = {
  calculation: {
    fields: {
      creator: {
        label: "Erstellt von",
        placeholder: "Name der verantwortlichen Person"
      },
      location: {
        label: "Standort / Abteil",
        placeholder: "z. B. Gew\xE4chshaus 1"
      },
      crop: {
        label: "Kultur",
        placeholder: "z. B. Salat"
      },
      quantity: {
        label: "Anzahl Kisten",
        placeholder: "z. B. 42",
        unit: "Kisten"
      }
    },
    summary: {
      water: "Gesamtwasser (L)",
      area: "Fl\xE4che (Ar / m\xB2)"
    },
    tableColumns: {
      medium: "Mittel",
      unit: "Einheit",
      method: "Methode",
      value: "Wert",
      perQuantity: "Kisten",
      areaAr: "Ar",
      areaSqm: "m\xB2",
      total: "Gesamt"
    },
    resultTitle: "Ben\xF6tigte Mittel"
  },
  history: {
    tableColumns: {
      date: "Datum",
      creator: "Erstellt von",
      location: "Standort",
      crop: "Kultur",
      quantity: "Kisten"
    },
    detail: {
      title: "Historieneintrag",
      creator: "Erstellt von",
      location: "Standort / Abteil",
      crop: "Kultur",
      quantity: "Kisten"
    },
    summaryTitle: "Historie (Zusammenfassung)",
    mediumsHeading: "Mittel & Gesamtmengen"
  },
  reporting: {
    tableColumns: {
      date: "Datum",
      creator: "Erstellt von",
      location: "Standort",
      crop: "Kultur",
      quantity: "Kisten",
      mediums: "Mittel & Gesamtmengen"
    },
    infoAll: "Alle Eintr\xE4ge",
    infoEmpty: "Keine Eintr\xE4ge vorhanden",
    infoPrefix: "Zeitraum",
    printTitle: "Auswertung"
  }
};
function deepMerge(target, source) {
  const result = Array.isArray(target) ? [...target] : { ...target };
  if (!source || typeof source !== "object") {
    return result;
  }
  for (const [key, value] of Object.entries(source)) {
    if (value && typeof value === "object" && !Array.isArray(value)) {
      const baseValue = key in result ? result[key] : {};
      result[key] = deepMerge(baseValue, value);
    } else {
      result[key] = Array.isArray(value) ? [...value] : value;
    }
  }
  return result;
}
function cloneLabels(labels) {
  return JSON.parse(JSON.stringify(labels));
}
function getDefaultFieldLabels() {
  return cloneLabels(DEFAULT_FIELD_LABELS);
}
function resolveFieldLabels(custom = {}) {
  return deepMerge(getDefaultFieldLabels(), custom);
}

// src/scripts/core/state.ts
var listeners = /* @__PURE__ */ new Set();
var state = {
  app: {
    ready: false,
    version: null,
    hasFileAccess: false,
    hasDatabase: false,
    activeSection: "calc",
    storageDriver: "memory"
  },
  company: {
    name: "",
    headline: "",
    logoUrl: "",
    contactEmail: "",
    address: "",
    accentColor: ""
  },
  defaults: {
    waterPerKisteL: 5,
    kistenProAr: 300,
    form: {
      creator: "",
      location: "",
      crop: "",
      quantity: ""
    }
  },
  measurementMethods: [],
  mediums: [],
  history: [],
  fieldLabels: getDefaultFieldLabels(),
  calcContext: null,
  zulassung: {
    filters: {
      culture: null,
      pest: null,
      text: "",
      includeExpired: false
    },
    results: [],
    lastSync: null,
    lastResultCounts: null,
    dataSource: null,
    apiStand: null,
    manifestVersion: null,
    lastSyncHash: null,
    busy: false,
    progress: { step: null, percent: 0, message: "" },
    error: null,
    logs: [],
    debug: {
      schema: null,
      lastSyncLog: [],
      manifest: null,
      lastAutoUpdateCheck: null
    },
    lookups: { cultures: [], pests: [] },
    autoUpdateAvailable: false,
    autoUpdateVersion: null
  },
  ui: {
    notifications: []
  }
};
function getState() {
  return state;
}
function subscribeState(listener) {
  listeners.add(listener);
  return () => listeners.delete(listener);
}
function notify(prevState) {
  for (const listener of listeners) {
    try {
      listener(state, prevState);
    } catch (err) {
      console.error("state listener error", err);
    }
  }
}
function patchState(patch) {
  const prevState = state;
  state = { ...state, ...patch };
  notify(prevState);
  return state;
}
function updateSlice(sliceKey, updater) {
  const currentSlice = state[sliceKey];
  const nextSlice = typeof updater === "function" ? updater(
    currentSlice,
    state
  ) : updater;
  if (nextSlice === currentSlice) {
    return state;
  }
  return patchState({ [sliceKey]: nextSlice });
}

// src/scripts/core/config.ts
var cachedDefaults = null;
function mergeDefaults(base = {}, incoming = {}) {
  const merged = { ...base, ...incoming };
  merged.form = {
    ...base.form || { creator: "", location: "", crop: "", quantity: "" },
    ...incoming.form || {}
  };
  return merged;
}
async function loadDefaultsConfig() {
  if (cachedDefaults) {
    return cachedDefaults;
  }
  const defaults = JSON.parse(JSON.stringify(defaults_default));
  cachedDefaults = defaults;
  const current = getState();
  patchState({
    app: {
      ...current.app,
      version: defaults.meta?.version ?? 1
    }
  });
  patchState({
    company: { ...current.company, ...defaults.meta?.company ?? {} },
    defaults: mergeDefaults(current.defaults, defaults.meta?.defaults ?? {}),
    measurementMethods: [...defaults.meta?.measurementMethods ?? []],
    mediums: [...defaults.mediums ?? []],
    history: [...defaults.history ?? []],
    fieldLabels: resolveFieldLabels(defaults.meta?.fieldLabels ?? {})
  });
  return cachedDefaults;
}

// src/scripts/core/storage/fileSystem.ts
var fileSystem_exports = {};
__export(fileSystem_exports, {
  create: () => create,
  getContext: () => getContext,
  isSupported: () => isSupported,
  open: () => open,
  reset: () => reset,
  save: () => save
});
var FILE_OPTIONS = {
  description: "JSON-Datei",
  accept: { "application/json": [".json"] }
};
var fileHandle = null;
async function writeFile(handle, data) {
  const writable = await handle.createWritable();
  await writable.write(JSON.stringify(data, null, 2));
  await writable.close();
}
async function readFile(handle) {
  const file = await handle.getFile();
  return file.text();
}
function isSupported() {
  return typeof window !== "undefined" && typeof window.showSaveFilePicker === "function";
}
async function create(initialData, suggestedName = "database.json") {
  if (!isSupported()) {
    throw new Error("File System Access API not available");
  }
  fileHandle = await window.showSaveFilePicker({
    suggestedName,
    types: [FILE_OPTIONS]
  });
  await writeFile(fileHandle, initialData);
  return { data: initialData, context: { fileHandle } };
}
async function open() {
  if (!isSupported()) {
    throw new Error("File System Access API not available");
  }
  const [handle] = await window.showOpenFilePicker({ types: [FILE_OPTIONS] });
  fileHandle = handle;
  const text = await readFile(fileHandle);
  const data = JSON.parse(text);
  return { data, context: { fileHandle } };
}
async function save(data) {
  if (!fileHandle) {
    throw new Error("Kein Dateihandle vorhanden");
  }
  await writeFile(fileHandle, data);
  return { context: { fileHandle } };
}
function getContext() {
  return { fileHandle };
}
function reset() {
  fileHandle = null;
}

// src/scripts/core/storage/fallback.ts
var fallback_exports = {};
__export(fallback_exports, {
  create: () => create2,
  getContext: () => getContext2,
  isSupported: () => isSupported2,
  open: () => open2,
  reset: () => reset2,
  save: () => save2
});
var STORAGE_KEY = "pflanzenschutzliste-db";
function hasLocalStorage() {
  try {
    const key = "__storage_test__";
    window.localStorage.setItem(key, "1");
    window.localStorage.removeItem(key);
    return true;
  } catch (err) {
    return false;
  }
}
function isSupported2() {
  return typeof window !== "undefined" && hasLocalStorage();
}
async function create2(initialData) {
  if (!isSupported2()) {
    throw new Error("LocalStorage nicht verf\xFCgbar");
  }
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(initialData));
  return { data: initialData, context: { storageKey: STORAGE_KEY } };
}
async function open2() {
  if (!isSupported2()) {
    throw new Error("LocalStorage nicht verf\xFCgbar");
  }
  const raw = window.localStorage.getItem(STORAGE_KEY);
  if (!raw) {
    throw new Error("Keine gespeicherten Daten gefunden");
  }
  return { data: JSON.parse(raw), context: { storageKey: STORAGE_KEY } };
}
async function save2(data) {
  if (!isSupported2()) {
    throw new Error("LocalStorage nicht verf\xFCgbar");
  }
  window.localStorage.setItem(STORAGE_KEY, JSON.stringify(data));
  return { context: { storageKey: STORAGE_KEY } };
}
function getContext2() {
  return { storageKey: STORAGE_KEY };
}
function reset2() {
  if (!isSupported2()) {
    return;
  }
  window.localStorage.removeItem(STORAGE_KEY);
}

// src/scripts/core/storage/sqlite.ts
var sqlite_exports = {};
__export(sqlite_exports, {
  appendBvlSyncLog: () => appendBvlSyncLog,
  create: () => create3,
  diagnoseBvlSchema: () => diagnoseBvlSchema,
  exec: () => exec,
  exportDB: () => exportDB,
  exportSnapshot: () => exportSnapshot,
  getBvlMeta: () => getBvlMeta,
  getBvlSyncStatus: () => getBvlSyncStatus,
  getContext: () => getContext3,
  importBvlDataset: () => importBvlDataset,
  importBvlSqlite: () => importBvlSqlite,
  importDB: () => importDB,
  importSnapshot: () => importSnapshot,
  isSupported: () => isSupported3,
  listBvlCultures: () => listBvlCultures,
  listBvlSchadorg: () => listBvlSchadorg,
  listBvlSyncLog: () => listBvlSyncLog,
  open: () => open3,
  query: () => query,
  queryBvl: () => queryBvl,
  queryZulassung: () => queryZulassung,
  reset: () => reset3,
  save: () => save3,
  setBvlMeta: () => setBvlMeta
});
var worker = null;
var messageId = 0;
var pendingMessages = /* @__PURE__ */ new Map();
var fileHandle2 = null;
function isSupported3() {
  if (typeof Worker === "undefined" || typeof WebAssembly === "undefined") {
    return false;
  }
  if (typeof window !== "undefined" && !window.isSecureContext) {
    return false;
  }
  return true;
}
function callWorker(action, payload) {
  return new Promise((resolve, reject) => {
    if (!worker) {
      reject(new Error("Worker not initialized"));
      return;
    }
    const id = ++messageId;
    pendingMessages.set(id, { resolve, reject });
    worker.postMessage({ id, action, payload });
    setTimeout(() => {
      if (pendingMessages.has(id)) {
        pendingMessages.delete(id);
        reject(new Error(`Worker call timeout: ${action}`));
      }
    }, 3e4);
  });
}
async function initWorker() {
  if (worker) {
    return;
  }
  try {
    worker = new Worker(new URL("./sqliteWorker.js", import.meta.url), {
      type: "module"
    });
    worker.onmessage = (event) => {
      const { id, ok, result, error } = event.data;
      if (pendingMessages.has(id)) {
        const { resolve, reject } = pendingMessages.get(id);
        pendingMessages.delete(id);
        if (ok) {
          resolve(result);
        } else {
          reject(new Error(error || "Worker error"));
        }
      }
    };
    worker.onerror = (error) => {
      console.error("Worker error:", error);
    };
    await callWorker("init", {});
  } catch (error) {
    console.error("Failed to initialize worker:", error);
    throw error;
  }
}
async function create3(initialData, suggestedName = "pflanzenschutz.sqlite") {
  if (!isSupported3()) {
    throw new Error("SQLite-WASM is not supported in this browser");
  }
  await initWorker();
  await callWorker("importSnapshot", initialData);
  if (typeof window.showSaveFilePicker === "function") {
    try {
      fileHandle2 = await window.showSaveFilePicker({
        suggestedName,
        types: [
          {
            description: "SQLite Database",
            accept: { "application/x-sqlite3": [".sqlite", ".db"] }
          }
        ]
      });
      const exported = await callWorker("exportDB");
      const writable = await fileHandle2.createWritable();
      await writable.write(new Uint8Array(exported.data));
      await writable.close();
    } catch (err) {
      console.warn("Could not save SQLite file:", err);
      fileHandle2 = null;
    }
  }
  return { data: initialData, context: { fileHandle: fileHandle2 } };
}
async function open3() {
  if (!isSupported3()) {
    throw new Error("SQLite-WASM is not supported in this browser");
  }
  await initWorker();
  if (typeof window.showOpenFilePicker === "function") {
    try {
      const [handle] = await window.showOpenFilePicker({
        types: [
          {
            description: "SQLite Database or JSON",
            accept: {
              "application/x-sqlite3": [".sqlite", ".db"],
              "application/json": [".json"]
            }
          }
        ]
      });
      fileHandle2 = handle;
      const file = await handle.getFile();
      const arrayBuffer = await file.arrayBuffer();
      if (file.name.endsWith(".json")) {
        const text = await file.text();
        const data = JSON.parse(text);
        await callWorker("importSnapshot", data);
        return { data, context: { fileHandle: fileHandle2 } };
      } else {
        await callWorker("importDB", arrayBuffer);
        const data = await callWorker("exportSnapshot");
        return { data, context: { fileHandle: fileHandle2 } };
      }
    } catch (err) {
      console.error("Failed to open file:", err);
      throw err;
    }
  } else {
    throw new Error("File System Access API not available");
  }
}
async function save3(data) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  await callWorker("importSnapshot", data);
  if (fileHandle2) {
    try {
      const exported = await callWorker("exportDB");
      const writable = await fileHandle2.createWritable();
      await writable.write(new Uint8Array(exported.data));
      await writable.close();
    } catch (err) {
      console.error("Failed to save to file:", err);
    }
  }
  return { context: { fileHandle: fileHandle2 } };
}
function getContext3() {
  return { fileHandle: fileHandle2 };
}
function reset3() {
  if (worker) {
    worker.terminate();
    worker = null;
  }
  pendingMessages.clear();
  fileHandle2 = null;
  messageId = 0;
}
async function query(sql, params) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("query", { sql, params });
}
async function exec(sql) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  await callWorker("exec", { sql });
}
async function exportSnapshot() {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("exportSnapshot");
}
async function importSnapshot(data) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  await callWorker("importSnapshot", data);
}
async function exportDB() {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("exportDB");
}
async function importDB(arrayBuffer) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  await callWorker("importDB", arrayBuffer);
}
async function importBvlDataset(dataset) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("importBvlDataset", dataset);
}
async function importBvlSqlite(data, manifest) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("importBvlSqlite", { data, manifest });
}
async function getBvlMeta(key) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("getBvlMeta", key);
}
async function setBvlMeta(key, value) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("setBvlMeta", { key, value });
}
async function appendBvlSyncLog(entry) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("appendBvlSyncLog", entry);
}
async function listBvlSyncLog(options = {}) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("listBvlSyncLog", options);
}
async function queryZulassung(params) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("queryZulassung", params);
}
async function queryBvl(filters) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("queryBvl", filters);
}
async function getBvlSyncStatus() {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("getBvlSyncStatus", {});
}
async function listBvlCultures(options = {}) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("listBvlCultures", options);
}
async function listBvlSchadorg(options = {}) {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("listBvlSchadorg", options);
}
async function diagnoseBvlSchema() {
  if (!worker) {
    throw new Error("Database not initialized");
  }
  return await callWorker("diagnoseBvlSchema", {});
}

// src/scripts/core/storage/index.ts
var DRIVERS = {
  sqlite: sqlite_exports,
  filesystem: fileSystem_exports,
  localstorage: fallback_exports
};
function detectPreferredDriver() {
  if (isSupported3()) {
    return "sqlite";
  }
  if (isSupported()) {
    return "filesystem";
  }
  if (isSupported2()) {
    return "localstorage";
  }
  return "memory";
}
var activeDriverKey = detectPreferredDriver();
function setActiveDriver(driverKey) {
  if (driverKey !== "memory" && !DRIVERS[driverKey]) {
    throw new Error(`Unbekannter Storage-Treiber: ${driverKey}`);
  }
  activeDriverKey = driverKey;
  patchState({
    app: {
      ...getState().app,
      storageDriver: driverKey,
      hasFileAccess: driverKey === "filesystem"
    }
  });
}

// src/scripts/core/eventBus.ts
var subscribers = /* @__PURE__ */ new Map();
var counter = 0;
function subscribe(eventName, handler) {
  if (!subscribers.has(eventName)) {
    subscribers.set(eventName, /* @__PURE__ */ new Map());
  }
  const token = `h_${eventName}_${counter++}`;
  subscribers.get(eventName).set(token, handler);
  return () => {
    const handlers = subscribers.get(eventName);
    if (handlers) {
      handlers.delete(token);
      if (!handlers.size) {
        subscribers.delete(eventName);
      }
    }
  };
}
function emit(eventName, payload) {
  const handlers = subscribers.get(eventName);
  if (!handlers) {
    return;
  }
  for (const handler of handlers.values()) {
    try {
      handler(payload);
    } catch (err) {
      console.error(`eventBus handler error for ${eventName}`, err);
    }
  }
}

// src/scripts/features/starfield.ts
var initialized = false;
function initStarfield() {
  if (initialized) {
    return;
  }
  const canvas = document.getElementById(
    "starCanvas"
  );
  if (!canvas) {
    return;
  }
  const canvasEl = canvas;
  const maybeContext = canvasEl.getContext("2d");
  if (!maybeContext) {
    return;
  }
  const ctx = maybeContext;
  const stars = [];
  const numStars = 150;
  function resizeCanvas() {
    canvasEl.width = window.innerWidth;
    canvasEl.height = window.innerHeight;
  }
  window.addEventListener("resize", resizeCanvas);
  resizeCanvas();
  for (let i = 0; i < numStars; i += 1) {
    stars.push({
      x: Math.random() * canvasEl.width,
      y: Math.random() * canvasEl.height,
      r: Math.random() * 2 + 0.5,
      dx: (Math.random() - 0.5) * 0.3,
      dy: (Math.random() - 0.5) * 0.3
    });
  }
  function animate() {
    ctx.clearRect(0, 0, canvasEl.width, canvasEl.height);
    ctx.fillStyle = "#fff";
    for (const star of stars) {
      ctx.beginPath();
      ctx.arc(star.x, star.y, star.r, 0, Math.PI * 2);
      ctx.fill();
      star.x += star.dx;
      star.y += star.dy;
      if (star.x < 0) star.x = canvasEl.width;
      if (star.x > canvasEl.width) star.x = 0;
      if (star.y < 0) star.y = canvasEl.height;
      if (star.y > canvasEl.height) star.y = 0;
    }
    requestAnimationFrame(animate);
  }
  animate();
  initialized = true;
}

// src/scripts/core/bootstrap.ts
function setupUnloadWarning(stateService) {
  const handler = (event) => {
    event.preventDefault();
    event.returnValue = "Die Verbindung zur Datenbank wird getrennt. Ungespeicherte \xC4nderungen k\xF6nnen verloren gehen.";
    return event.returnValue;
  };
  let active = false;
  const update = (state2) => {
    const shouldWarn = Boolean(state2.app?.hasDatabase);
    if (shouldWarn && !active) {
      window.addEventListener("beforeunload", handler);
      active = true;
    } else if (!shouldWarn && active) {
      window.removeEventListener("beforeunload", handler);
      active = false;
    }
  };
  update(stateService.getState());
  stateService.subscribe(update);
}
function getRegions() {
  const root = document.getElementById("app-root");
  if (!root) {
    throw new Error("app-root Container fehlt");
  }
  return {
    startup: root.querySelector('[data-region="startup"]'),
    shell: root.querySelector('[data-region="shell"]'),
    main: root.querySelector('[data-region="main"]'),
    footer: root.querySelector('[data-region="footer"]')
  };
}
async function bootstrap() {
  getRegions();
  const driverKey = detectPreferredDriver();
  if (driverKey !== "memory") {
    setActiveDriver(driverKey);
  }
  await loadDefaultsConfig();
  const services = {
    state: {
      getState,
      patchState,
      updateSlice,
      subscribe: subscribeState
    },
    events: {
      emit,
      subscribe
    }
  };
  initStarfield();
  setupUnloadWarning(services.state);
  patchState({
    app: {
      ...getState().app,
      ready: true
    }
  });
}

// src/scripts/main.ts
if (typeof window !== "undefined" && typeof document !== "undefined") {
  let startApp = function() {
    bootstrap().catch((error) => {
      console.error("bootstrap failed", error);
    });
  };
  startApp2 = startApp;
  const bootstrapFlag = "__pflanzenschutz_bootstrapped__";
  const win = window;
  if (!win[bootstrapFlag]) {
    win[bootstrapFlag] = true;
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", startApp, { once: true });
    } else {
      startApp();
    }
  }
}
var startApp2;
