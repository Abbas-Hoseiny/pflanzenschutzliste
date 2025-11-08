/**
 * SQLite Web Worker
 * Handles all database operations in a separate thread to prevent UI blocking
 */

let sqlite3 = null;
let db = null;
let isInitialized = false;
let currentMode = "memory";

// SQLite-WASM CDN URL
const SQLITE_WASM_CDN =
  "https://cdn.jsdelivr.net/npm/@sqlite.org/sqlite-wasm@3.46.1-build1/sqlite-wasm/jswasm/";

// Message handler
self.onmessage = async function (event) {
  const { id, action, payload } = event.data;

  try {
    let result;

    switch (action) {
      case "init":
        result = await initDatabase(payload);
        break;
      case "importSnapshot":
        result = await importSnapshot(payload);
        break;
      case "exportSnapshot":
        result = await exportSnapshot();
        break;
      case "upsertMedium":
        result = await upsertMedium(payload);
        break;
      case "deleteMedium":
        result = await deleteMedium(payload);
        break;
      case "listMediums":
        result = await listMediums();
        break;
      case "listHistory":
        result = await listHistory(payload);
        break;
      case "getHistoryEntry":
        result = await getHistoryEntry(payload);
        break;
      case "appendHistoryEntry":
        result = await appendHistoryEntry(payload);
        break;
      case "deleteHistoryEntry":
        result = await deleteHistoryEntry(payload);
        break;
      case "exportDB":
        result = await exportDB();
        break;
      case "importDB":
        result = await importDB(payload);
        break;
      case "importBvlDataset":
        result = await importBvlDataset(payload);
        break;
      case "importBvlSqlite":
        result = await importBvlSqlite(payload);
        break;
      case "getBvlMeta":
        result = await getBvlMeta(payload);
        break;
      case "setBvlMeta":
        result = await setBvlMeta(payload);
        break;
      case "appendBvlSyncLog":
        result = await appendBvlSyncLog(payload);
        break;
      case "listBvlSyncLog":
        result = await listBvlSyncLog(payload);
        break;
      case "queryZulassung":
        result = await queryZulassung(payload);
        break;
      case "listBvlCultures":
        result = await listBvlCultures(payload);
        break;
      case "listBvlSchadorg":
        result = await listBvlSchadorg(payload);
        break;
      case "diagnoseBvlSchema":
        result = await diagnoseBvlSchema();
        break;
      default:
        throw new Error(`Unknown action: ${action}`);
    }

    self.postMessage({ id, ok: true, result });
  } catch (error) {
    self.postMessage({
      id,
      ok: false,
      error: error.message || String(error),
    });
  }
};

/**
 * Initialize SQLite database
 */
async function initDatabase(options = {}) {
  if (isInitialized) {
    return { success: true, message: "Already initialized" };
  }

  try {
    // Load SQLite WASM module
    const sqlite3InitModule = await import(
      SQLITE_WASM_CDN + "sqlite3.mjs"
    ).then((m) => m.default);

    sqlite3 = await sqlite3InitModule({
      print: console.log,
      printErr: console.error,
      locateFile: (file) => SQLITE_WASM_CDN + file,
    });

    // Determine storage mode
    const mode = options.mode || detectMode();
    db = createDatabaseInstance(mode);
    currentMode = mode;

    configureDatabase();

    // Apply schema for freshly created databases
    await applySchema();

    isInitialized = true;

    return {
      success: true,
      mode,
      message: `Database initialized in ${mode} mode`,
    };
  } catch (error) {
    console.error("Failed to initialize database:", error);
    throw new Error(`Database initialization failed: ${error.message}`);
  }
}

/**
 * Detect best storage mode
 */
function detectMode() {
  if (typeof sqlite3?.opfs !== "undefined") {
    return "opfs";
  }
  return "memory";
}

function createDatabaseInstance(mode = "memory") {
  if (mode === "opfs" && sqlite3?.opfs) {
    return new sqlite3.oo1.OpfsDb("/pflanzenschutz.sqlite");
  }
  return new sqlite3.oo1.DB();
}

function configureDatabase(targetDb = db) {
  if (!targetDb) {
    throw new Error("Database not initialized");
  }
  targetDb.exec(`
    PRAGMA foreign_keys = ON;
    PRAGMA journal_mode = WAL;
    PRAGMA synchronous = NORMAL;
    PRAGMA temp_store = MEMORY;
    PRAGMA cache_size = -20000;
  `);
}

/**
 * Apply database schema
 */
async function applySchema() {
  if (!db) throw new Error("Database not initialized");

  // Check current version
  const currentVersion = db.selectValue("PRAGMA user_version") || 0;

  if (currentVersion === 0) {
    // Apply initial schema
    const schemaSql = `
      CREATE TABLE IF NOT EXISTS meta (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL
      );
      
      CREATE TABLE IF NOT EXISTS measurement_methods (
        id TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        type TEXT NOT NULL,
        unit TEXT NOT NULL,
        requires TEXT,
        config TEXT
      );
      
      CREATE TABLE IF NOT EXISTS mediums (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        unit TEXT NOT NULL,
        method_id TEXT NOT NULL,
        value REAL NOT NULL,
        FOREIGN KEY(method_id) REFERENCES measurement_methods(id)
      );
      
      CREATE TABLE IF NOT EXISTS history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at TEXT NOT NULL,
        header_json TEXT NOT NULL
      );
      
      CREATE TABLE IF NOT EXISTS history_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        history_id INTEGER NOT NULL,
        medium_id TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        FOREIGN KEY(history_id) REFERENCES history(id) ON DELETE CASCADE
      );
      
      CREATE INDEX IF NOT EXISTS idx_history_created_at ON history(created_at DESC);
      CREATE INDEX IF NOT EXISTS idx_history_items_history_id ON history_items(history_id);
      CREATE INDEX IF NOT EXISTS idx_mediums_method_id ON mediums(method_id);
      
      PRAGMA user_version = 1;
    `;

    db.exec(schemaSql);
  }

  // Migration to version 2: Add BVL tables
  if (currentVersion < 2) {
    console.log("Migrating database to version 2...");

    db.exec("BEGIN TRANSACTION");

    try {
      // Drop all existing BVL tables first
      db.exec(`
        PRAGMA foreign_keys = OFF;
  DROP TABLE IF EXISTS bvl_lookup_schadorg;
  DROP TABLE IF EXISTS bvl_lookup_kultur;
  DROP TABLE IF EXISTS bvl_awg_wartezeit;
  DROP TABLE IF EXISTS bvl_awg_aufwand;
  DROP TABLE IF EXISTS bvl_awg_schadorg;
  DROP TABLE IF EXISTS bvl_awg_kultur;
  DROP TABLE IF EXISTS bvl_awg;
  DROP TABLE IF EXISTS bvl_mittel;
  DROP TABLE IF EXISTS bvl_meta;
  DROP TABLE IF EXISTS bvl_sync_log;
      `);

      // Create new BVL schema
      db.exec(`
        CREATE TABLE bvl_meta (
          key TEXT PRIMARY KEY,
          value TEXT
        );
        
        CREATE TABLE bvl_mittel (
          kennr TEXT PRIMARY KEY,
          name TEXT,
          formulierung TEXT,
          zul_erstmalig TEXT,
          zul_ende TEXT,
          geringes_risiko INTEGER,
          payload_json TEXT
        );
        
        CREATE TABLE bvl_awg (
          awg_id TEXT PRIMARY KEY,
          kennr TEXT REFERENCES bvl_mittel(kennr) ON DELETE CASCADE,
          status_json TEXT,
          zulassungsende TEXT
        );
        
        CREATE TABLE bvl_awg_kultur (
          awg_id TEXT REFERENCES bvl_awg(awg_id) ON DELETE CASCADE,
          kultur TEXT,
          ausgenommen INTEGER,
          sortier_nr INTEGER,
          PRIMARY KEY (awg_id, kultur, ausgenommen)
        );
        
        CREATE TABLE bvl_awg_schadorg (
          awg_id TEXT REFERENCES bvl_awg(awg_id) ON DELETE CASCADE,
          schadorg TEXT,
          ausgenommen INTEGER,
          sortier_nr INTEGER,
          PRIMARY KEY (awg_id, schadorg, ausgenommen)
        );
        
        CREATE TABLE bvl_awg_aufwand (
          awg_id TEXT REFERENCES bvl_awg(awg_id) ON DELETE CASCADE,
          aufwand_bedingung TEXT,
          sortier_nr INTEGER,
          mittel_menge REAL,
          mittel_einheit TEXT,
          wasser_menge REAL,
          wasser_einheit TEXT,
          payload_json TEXT,
          PRIMARY KEY (awg_id, aufwand_bedingung, sortier_nr)
        );
        
        CREATE TABLE bvl_awg_wartezeit (
          awg_wartezeit_nr INTEGER,
          awg_id TEXT REFERENCES bvl_awg(awg_id) ON DELETE CASCADE,
          kultur TEXT,
          sortier_nr INTEGER,
          tage INTEGER,
          bemerkung_kode TEXT,
          anwendungsbereich TEXT,
          erlaeuterung TEXT,
          payload_json TEXT,
          PRIMARY KEY (awg_wartezeit_nr, awg_id)
        );
        
        CREATE TABLE bvl_sync_log (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          synced_at TEXT,
          ok INTEGER,
          message TEXT,
          payload_hash TEXT
        );

        CREATE TABLE bvl_lookup_kultur (
          code TEXT PRIMARY KEY,
          label TEXT
        );

        CREATE TABLE bvl_lookup_schadorg (
          code TEXT PRIMARY KEY,
          label TEXT
        );
        
        CREATE INDEX idx_awg_kennr ON bvl_awg(kennr);
        CREATE INDEX idx_awg_kultur_kultur ON bvl_awg_kultur(kultur);
        CREATE INDEX idx_awg_schadorg_schadorg ON bvl_awg_schadorg(schadorg);
        CREATE INDEX idx_awg_aufwand_awg ON bvl_awg_aufwand(awg_id);
        CREATE INDEX idx_awg_wartezeit_awg ON bvl_awg_wartezeit(awg_id);
        CREATE INDEX idx_lookup_kultur_label ON bvl_lookup_kultur(label);
        CREATE INDEX idx_lookup_schadorg_label ON bvl_lookup_schadorg(label);
        
        PRAGMA foreign_keys = ON;
        PRAGMA user_version = 2;
      `);

      db.exec("COMMIT");
      console.log("Database migrated to version 2 successfully");
    } catch (error) {
      db.exec("ROLLBACK");
      console.error("Migration to version 2 failed:", error);
      throw error;
    }
  }
}

/**
 * Import a complete snapshot from JSON format
 */
async function importSnapshot(snapshot) {
  if (!db) throw new Error("Database not initialized");

  db.exec("BEGIN TRANSACTION");

  try {
    // Clear existing data
    db.exec(`
      DELETE FROM history_items;
      DELETE FROM history;
      DELETE FROM mediums;
      DELETE FROM measurement_methods;
      DELETE FROM meta;
    `);

    // Import meta data
    if (snapshot.meta) {
      const metaEntries = {
        version: snapshot.meta.version || 1,
        company: JSON.stringify(snapshot.meta.company || {}),
        defaults: JSON.stringify(snapshot.meta.defaults || {}),
        fieldLabels: JSON.stringify(snapshot.meta.fieldLabels || {}),
        measurementMethods: JSON.stringify(
          snapshot.meta.measurementMethods || []
        ),
      };

      const stmt = db.prepare(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)"
      );
      for (const [key, value] of Object.entries(metaEntries)) {
        stmt
          .bind([
            key,
            typeof value === "string" ? value : JSON.stringify(value),
          ])
          .step();
        stmt.reset();
      }
      stmt.finalize();

      // Import measurement methods
      if (
        snapshot.meta.measurementMethods &&
        Array.isArray(snapshot.meta.measurementMethods)
      ) {
        const methodStmt = db.prepare(
          "INSERT OR REPLACE INTO measurement_methods (id, label, type, unit, requires, config) VALUES (?, ?, ?, ?, ?, ?)"
        );
        for (const method of snapshot.meta.measurementMethods) {
          methodStmt
            .bind([
              method.id,
              method.label,
              method.type,
              method.unit,
              JSON.stringify(method.requires || []),
              JSON.stringify(method.config || {}),
            ])
            .step();
          methodStmt.reset();
        }
        methodStmt.finalize();
      }
    }

    // Import mediums
    if (snapshot.mediums && Array.isArray(snapshot.mediums)) {
      const mediumStmt = db.prepare(
        "INSERT OR REPLACE INTO mediums (id, name, unit, method_id, value) VALUES (?, ?, ?, ?, ?)"
      );
      for (const medium of snapshot.mediums) {
        mediumStmt
          .bind([
            medium.id,
            medium.name,
            medium.unit,
            medium.methodId || medium.method_id,
            medium.value,
          ])
          .step();
        mediumStmt.reset();
      }
      mediumStmt.finalize();
    }

    // Import history
    if (snapshot.history && Array.isArray(snapshot.history)) {
      const historyStmt = db.prepare(
        "INSERT INTO history (created_at, header_json) VALUES (?, ?)"
      );
      const itemsStmt = db.prepare(
        "INSERT INTO history_items (history_id, medium_id, payload_json) VALUES (?, ?, ?)"
      );

      for (const entry of snapshot.history) {
        const header = entry.header ? { ...entry.header } : { ...entry };
        delete header.items;
        const createdAt =
          entry.savedAt ||
          header.savedAt ||
          header.createdAt ||
          new Date().toISOString();
        if (!header.createdAt) {
          header.createdAt = createdAt;
        }

        historyStmt.bind([createdAt, JSON.stringify(header)]).step();
        const historyId = db.selectValue("SELECT last_insert_rowid()");
        historyStmt.reset();

        const items =
          entry.items && Array.isArray(entry.items) ? entry.items : [];
        for (const item of items) {
          itemsStmt
            .bind([
              historyId,
              item.mediumId || item.medium_id || "",
              JSON.stringify(item),
            ])
            .step();
          itemsStmt.reset();
        }
      }

      historyStmt.finalize();
      itemsStmt.finalize();
    }

    db.exec("COMMIT");
    return { success: true, message: "Snapshot imported successfully" };
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  }
}

/**
 * Export complete database as JSON snapshot
 */
async function exportSnapshot() {
  if (!db) throw new Error("Database not initialized");

  const snapshot = {
    meta: {
      version: 1,
      company: {},
      defaults: {},
      fieldLabels: {},
      measurementMethods: [],
    },
    mediums: [],
    history: [],
  };

  // Export meta
  db.exec({
    sql: "SELECT key, value FROM meta",
    callback: (row) => {
      const key = row[0];
      const value = row[1];
      try {
        const parsed = JSON.parse(value);
        if (key === "company") snapshot.meta.company = parsed;
        else if (key === "defaults") snapshot.meta.defaults = parsed;
        else if (key === "fieldLabels") snapshot.meta.fieldLabels = parsed;
        else if (key === "version") snapshot.meta.version = parsed;
      } catch (e) {
        console.warn(`Failed to parse meta key ${key}:`, e);
      }
    },
  });

  // Export measurement methods
  db.exec({
    sql: "SELECT id, label, type, unit, requires, config FROM measurement_methods",
    callback: (row) => {
      snapshot.meta.measurementMethods.push({
        id: row[0],
        label: row[1],
        type: row[2],
        unit: row[3],
        requires: JSON.parse(row[4] || "[]"),
        config: JSON.parse(row[5] || "{}"),
      });
    },
  });

  // Export mediums
  db.exec({
    sql: "SELECT id, name, unit, method_id, value FROM mediums",
    callback: (row) => {
      snapshot.mediums.push({
        id: row[0],
        name: row[1],
        unit: row[2],
        methodId: row[3],
        value: row[4],
      });
    },
  });

  // Export history
  const historyMap = new Map();
  db.exec({
    sql: "SELECT id, created_at, header_json FROM history ORDER BY created_at DESC",
    callback: (row) => {
      historyMap.set(row[0], {
        header: JSON.parse(row[2] || "{}"),
        items: [],
      });
    },
  });

  // Export history items
  db.exec({
    sql: "SELECT history_id, medium_id, payload_json FROM history_items",
    callback: (row) => {
      const historyId = row[0];
      if (historyMap.has(historyId)) {
        historyMap.get(historyId).items.push(JSON.parse(row[2]));
      }
    },
  });

  snapshot.history = Array.from(historyMap.values()).map((entry) => ({
    ...entry.header,
    items: entry.items,
  }));

  return snapshot;
}

/**
 * CRUD operations for mediums
 */
async function upsertMedium(medium) {
  if (!db) throw new Error("Database not initialized");

  const stmt = db.prepare(
    "INSERT OR REPLACE INTO mediums (id, name, unit, method_id, value) VALUES (?, ?, ?, ?, ?)"
  );
  stmt
    .bind([
      medium.id,
      medium.name,
      medium.unit,
      medium.methodId || medium.method_id,
      medium.value,
    ])
    .step();
  stmt.finalize();

  return { success: true, id: medium.id };
}

async function deleteMedium(id) {
  if (!db) throw new Error("Database not initialized");

  const stmt = db.prepare("DELETE FROM mediums WHERE id = ?");
  stmt.bind([id]).step();
  stmt.finalize();

  return { success: true };
}

async function listMediums() {
  if (!db) throw new Error("Database not initialized");

  const mediums = [];
  db.exec({
    sql: "SELECT id, name, unit, method_id, value FROM mediums",
    callback: (row) => {
      mediums.push({
        id: row[0],
        name: row[1],
        unit: row[2],
        methodId: row[3],
        value: row[4],
      });
    },
  });

  return mediums;
}

/**
 * History operations with paging
 */
async function listHistory({ page = 1, pageSize = 50 } = {}) {
  if (!db) throw new Error("Database not initialized");

  const offset = (page - 1) * pageSize;
  const history = [];

  db.exec({
    sql: `
      SELECT id, created_at, header_json 
      FROM history 
      ORDER BY created_at DESC 
      LIMIT ? OFFSET ?
    `,
    bind: [pageSize, offset],
    callback: (row) => {
      const header = JSON.parse(row[2] || "{}");
      history.push({
        id: row[0],
        ...header,
      });
    },
  });

  const totalCount = db.selectValue("SELECT COUNT(*) FROM history") || 0;

  return {
    items: history,
    page,
    pageSize,
    totalCount,
    totalPages: Math.ceil(totalCount / pageSize),
  };
}

async function getHistoryEntry(id) {
  if (!db) throw new Error("Database not initialized");

  let entry = null;

  db.exec({
    sql: "SELECT id, created_at, header_json FROM history WHERE id = ?",
    bind: [id],
    callback: (row) => {
      const header = JSON.parse(row[2] || "{}");
      entry = {
        id: row[0],
        ...header,
        items: [],
      };
    },
  });

  if (!entry) {
    throw new Error("History entry not found");
  }

  db.exec({
    sql: "SELECT medium_id, payload_json FROM history_items WHERE history_id = ?",
    bind: [id],
    callback: (row) => {
      entry.items.push(JSON.parse(row[1]));
    },
  });

  return entry;
}

async function appendHistoryEntry(entry) {
  if (!db) throw new Error("Database not initialized");

  db.exec("BEGIN TRANSACTION");

  try {
    const header = entry.header ? { ...entry.header } : { ...entry };
    delete header.items;
    const createdAt =
      entry.savedAt ||
      header.savedAt ||
      header.createdAt ||
      new Date().toISOString();
    if (!header.createdAt) {
      header.createdAt = createdAt;
    }

    const stmt = db.prepare(
      "INSERT INTO history (created_at, header_json) VALUES (?, ?)"
    );
    stmt.bind([createdAt, JSON.stringify(header)]).step();
    const historyId = db.selectValue("SELECT last_insert_rowid()");
    stmt.finalize();

    const items = entry.items && Array.isArray(entry.items) ? entry.items : [];
    if (items.length) {
      const itemStmt = db.prepare(
        "INSERT INTO history_items (history_id, medium_id, payload_json) VALUES (?, ?, ?)"
      );
      for (const item of items) {
        itemStmt
          .bind([
            historyId,
            item.mediumId || item.medium_id || "",
            JSON.stringify(item),
          ])
          .step();
        itemStmt.reset();
      }
      itemStmt.finalize();
    }

    db.exec("COMMIT");
    return { success: true, id: historyId };
  } catch (error) {
    db.exec("ROLLBACK");
    throw error;
  }
}

async function deleteHistoryEntry(id) {
  if (!db) throw new Error("Database not initialized");

  // CASCADE will handle history_items deletion
  const stmt = db.prepare("DELETE FROM history WHERE id = ?");
  stmt.bind([id]).step();
  stmt.finalize();

  return { success: true };
}

/**
 * Export database as binary SQLite file
 */
async function exportDB() {
  if (!db) throw new Error("Database not initialized");

  const exported = sqlite3.capi.sqlite3_js_db_export(db.pointer);
  return { data: Array.from(exported) };
}

/**
 * Import database from binary SQLite file
 */
async function importDB(data) {
  if (!db) throw new Error("Database not initialized");
  const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);

  if (currentMode === "opfs" && sqlite3?.oo1?.OpfsDb && sqlite3?.opfs) {
    // Import directly into OPFS-backed database
    db.close();
    await sqlite3.oo1.OpfsDb.importDb("/pflanzenschutz.sqlite", bytes);
    db = createDatabaseInstance("opfs");
    configureDatabase();
    currentMode = "opfs";
    isInitialized = true;
    return { success: true, mode: "opfs" };
  }

  // In-memory fallback using sqlite3_deserialize
  db.close();
  const newDb = new sqlite3.oo1.DB();
  const scope = sqlite3.wasm.scopedAllocPush();
  try {
    const pData = sqlite3.wasm.allocFromTypedArray(bytes);
    const pSchema = sqlite3.wasm.allocCString("main");
    const flags =
      (sqlite3.capi.SQLITE_DESERIALIZE_FREEONCLOSE || 0) |
      (sqlite3.capi.SQLITE_DESERIALIZE_RESIZEABLE || 0);
    const rc = sqlite3.capi.sqlite3_deserialize(
      newDb.pointer,
      pSchema,
      pData,
      bytes.byteLength,
      bytes.byteLength,
      flags
    );
    if (rc !== sqlite3.capi.SQLITE_OK) {
      newDb.close();
      throw new Error(
        `sqlite3_deserialize failed: ${
          sqlite3.capi.sqlite3_js_rc_str(rc) || rc
        }`
      );
    }
  } finally {
    sqlite3.wasm.scopedAllocPop(scope);
  }

  db = newDb;
  configureDatabase();
  currentMode = "memory";
  isInitialized = true;
  return { success: true, mode: "memory" };
}

/**
 * BVL-related functions
 */

async function importBvlDataset(payload) {
  if (!db) throw new Error("Database not initialized");

  const {
    mittel,
    awg,
    awg_kultur,
    awg_schadorg,
    awg_aufwand,
    awg_wartezeit,
    culturesLookup,
    pestsLookup,
  } = payload;
  const debug = payload.debug || false;

  db.exec("BEGIN TRANSACTION");

  try {
    // Clear existing BVL data
    db.exec(`
      DELETE FROM bvl_awg_wartezeit;
      DELETE FROM bvl_awg_aufwand;
      DELETE FROM bvl_awg_schadorg;
      DELETE FROM bvl_awg_kultur;
      DELETE FROM bvl_awg;
      DELETE FROM bvl_mittel;
      DELETE FROM bvl_lookup_kultur;
      DELETE FROM bvl_lookup_schadorg;
    `);

    let counts = {};

    // Import mittel
    if (mittel && mittel.length > 0) {
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO bvl_mittel 
        (kennr, name, formulierung, zul_erstmalig, zul_ende, geringes_risiko, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `);

      for (const item of mittel) {
        stmt
          .bind([
            item.kennr,
            item.name,
            item.formulierung,
            item.zul_erstmalig,
            item.zul_ende,
            item.geringes_risiko,
            item.payload_json,
          ])
          .step();
        stmt.reset();
      }
      stmt.finalize();
      counts.mittel = mittel.length;
      if (debug) console.debug(`Imported ${mittel.length} mittel`);
    }

    // Import awg
    if (awg && awg.length > 0) {
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO bvl_awg 
        (awg_id, kennr, status_json, zulassungsende)
        VALUES (?, ?, ?, ?)
      `);

      for (const item of awg) {
        stmt
          .bind([
            item.awg_id,
            item.kennr,
            item.status_json,
            item.zulassungsende,
          ])
          .step();
        stmt.reset();
      }
      stmt.finalize();
      counts.awg = awg.length;
      if (debug) console.debug(`Imported ${awg.length} awg`);
    }

    // Import awg_kultur
    if (awg_kultur && awg_kultur.length > 0) {
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO bvl_awg_kultur 
        (awg_id, kultur, ausgenommen, sortier_nr)
        VALUES (?, ?, ?, ?)
      `);

      for (const item of awg_kultur) {
        stmt
          .bind([item.awg_id, item.kultur, item.ausgenommen, item.sortier_nr])
          .step();
        stmt.reset();
      }
      stmt.finalize();
      counts.awg_kultur = awg_kultur.length;
      if (debug) console.debug(`Imported ${awg_kultur.length} awg_kultur`);
    }

    // Import awg_schadorg
    if (awg_schadorg && awg_schadorg.length > 0) {
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO bvl_awg_schadorg 
        (awg_id, schadorg, ausgenommen, sortier_nr)
        VALUES (?, ?, ?, ?)
      `);

      for (const item of awg_schadorg) {
        stmt
          .bind([item.awg_id, item.schadorg, item.ausgenommen, item.sortier_nr])
          .step();
        stmt.reset();
      }
      stmt.finalize();
      counts.awg_schadorg = awg_schadorg.length;
      if (debug) console.debug(`Imported ${awg_schadorg.length} awg_schadorg`);
    }

    // Import awg_aufwand
    if (awg_aufwand && awg_aufwand.length > 0) {
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO bvl_awg_aufwand 
        (awg_id, aufwand_bedingung, sortier_nr, mittel_menge, mittel_einheit, 
         wasser_menge, wasser_einheit, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `);

      for (const item of awg_aufwand) {
        stmt
          .bind([
            item.awg_id,
            item.aufwand_bedingung,
            item.sortier_nr,
            item.mittel_menge,
            item.mittel_einheit,
            item.wasser_menge,
            item.wasser_einheit,
            item.payload_json,
          ])
          .step();
        stmt.reset();
      }
      stmt.finalize();
      counts.awg_aufwand = awg_aufwand.length;
      if (debug) console.debug(`Imported ${awg_aufwand.length} awg_aufwand`);
    }

    // Import awg_wartezeit
    if (awg_wartezeit && awg_wartezeit.length > 0) {
      const stmt = db.prepare(`
        INSERT OR REPLACE INTO bvl_awg_wartezeit 
        (awg_wartezeit_nr, awg_id, kultur, sortier_nr, tage, 
         bemerkung_kode, anwendungsbereich, erlaeuterung, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);

      for (const item of awg_wartezeit) {
        stmt
          .bind([
            item.awg_wartezeit_nr,
            item.awg_id,
            item.kultur,
            item.sortier_nr,
            item.tage,
            item.bemerkung_kode,
            item.anwendungsbereich,
            item.erlaeuterung,
            item.payload_json,
          ])
          .step();
        stmt.reset();
      }
      stmt.finalize();
      counts.awg_wartezeit = awg_wartezeit.length;
      if (debug)
        console.debug(`Imported ${awg_wartezeit.length} awg_wartezeit`);
    }

    if (culturesLookup && culturesLookup.length > 0) {
      const stmt = db.prepare(
        `
        INSERT OR REPLACE INTO bvl_lookup_kultur (code, label)
        VALUES (?, ?)
      `
      );

      for (const item of culturesLookup) {
        stmt.bind([item.code, item.label]).step();
        stmt.reset();
      }
      stmt.finalize();
      counts.lookup_kultur = culturesLookup.length;
      if (debug)
        console.debug(`Imported ${culturesLookup.length} lookup_kultur`);
    }

    if (pestsLookup && pestsLookup.length > 0) {
      const stmt = db.prepare(
        `
        INSERT OR REPLACE INTO bvl_lookup_schadorg (code, label)
        VALUES (?, ?)
      `
      );

      for (const item of pestsLookup) {
        stmt.bind([item.code, item.label]).step();
        stmt.reset();
      }
      stmt.finalize();
      counts.lookup_schadorg = pestsLookup.length;
      if (debug)
        console.debug(`Imported ${pestsLookup.length} lookup_schadorg`);
    }

    db.exec("COMMIT");
    return { success: true, counts };
  } catch (error) {
    db.exec("ROLLBACK");
    console.error("Failed to import BVL dataset:", error);
    throw error;
  }
}

async function importBvlSqlite(payload) {
  if (!db) throw new Error("Database not initialized");
  if (!sqlite3) throw new Error("SQLite not initialized");

  const { data, manifest } = payload;
  
  if (!data || !(data instanceof Uint8Array || data instanceof Array)) {
    throw new Error("Invalid data: expected Uint8Array or Array");
  }

  const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);

  // Create a temporary in-memory database with the imported data
  const remoteDb = new sqlite3.oo1.DB();
  
  // Deserialize the database
  const scope = sqlite3.wasm.scopedAllocPush();
  try {
    const pData = sqlite3.wasm.allocFromTypedArray(bytes);
    const pSchema = sqlite3.wasm.allocCString("main");
    const flags =
      (sqlite3.capi.SQLITE_DESERIALIZE_FREEONCLOSE || 0) |
      (sqlite3.capi.SQLITE_DESERIALIZE_RESIZEABLE || 0);
    const rc = sqlite3.capi.sqlite3_deserialize(
      remoteDb.pointer,
      pSchema,
      pData,
      bytes.byteLength,
      bytes.byteLength,
      flags
    );
    if (rc !== sqlite3.capi.SQLITE_OK) {
      remoteDb.close();
      throw new Error(
        `sqlite3_deserialize failed: ${
          sqlite3.capi.sqlite3_js_rc_str(rc) || rc
        }`
      );
    }
  } finally {
    sqlite3.wasm.scopedAllocPop(scope);
  }

  // Get list of BVL tables from remote database
  const bvlTables = [];
  remoteDb.exec({
    sql: "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'bvl_%' ORDER BY name",
    callback: (row) => {
      bvlTables.push(row[0]);
    },
  });

  console.log(`Found ${bvlTables.length} BVL tables in remote database:`, bvlTables);

  // Begin transaction on main database
  db.exec("BEGIN TRANSACTION");

  try {
    const counts = {};

    // Import each BVL table
    for (const tableName of bvlTables) {
      // Skip sync log as we don't want to overwrite it
      if (tableName === "bvl_sync_log") {
        continue;
      }

      // Get column names from remote table
      const columns = [];
      remoteDb.exec({
        sql: `PRAGMA table_info(${tableName})`,
        callback: (row) => {
          columns.push(row[1]); // column name
        },
      });

      if (columns.length === 0) {
        console.warn(`Table ${tableName} has no columns, skipping`);
        continue;
      }

      // Check if table exists in main database
      let tableExists = false;
      db.exec({
        sql: "SELECT name FROM sqlite_master WHERE type='table' AND name = ?",
        bind: [tableName],
        callback: () => {
          tableExists = true;
        },
      });

      if (!tableExists) {
        // Create the table by copying schema from remote
        const createSql = remoteDb.selectValue(
          `SELECT sql FROM sqlite_master WHERE type='table' AND name = ?`,
          [tableName]
        );
        if (createSql) {
          console.log(`Creating table ${tableName}`);
          db.exec(createSql);
        } else {
          console.warn(`Could not get CREATE statement for ${tableName}, skipping`);
          continue;
        }
      }

      // Delete existing data from main table
      db.exec(`DELETE FROM ${tableName}`);

      // Get columns that exist in main table
      const mainColumns = [];
      db.exec({
        sql: `PRAGMA table_info(${tableName})`,
        callback: (row) => {
          mainColumns.push(row[1]);
        },
      });

      // Find common columns
      const commonColumns = columns.filter((col) => mainColumns.includes(col));
      
      if (commonColumns.length === 0) {
        console.warn(`No common columns between remote and main ${tableName}, skipping`);
        continue;
      }

      const colList = commonColumns.join(", ");

      // Attach remote database
      db.exec(`ATTACH DATABASE ':memory:' AS remote`);
      
      // Copy the remote database into the attached database
      const remoteCopy = new sqlite3.oo1.DB(":memory:");
      const remoteExport = sqlite3.capi.sqlite3_js_db_export(remoteDb.pointer);
      
      const scope2 = sqlite3.wasm.scopedAllocPush();
      try {
        const pData2 = sqlite3.wasm.allocFromTypedArray(remoteExport);
        const pSchema2 = sqlite3.wasm.allocCString("main");
        const flags2 =
          (sqlite3.capi.SQLITE_DESERIALIZE_FREEONCLOSE || 0) |
          (sqlite3.capi.SQLITE_DESERIALIZE_RESIZEABLE || 0);
        const rc2 = sqlite3.capi.sqlite3_deserialize(
          remoteCopy.pointer,
          pSchema2,
          pData2,
          remoteExport.byteLength,
          remoteExport.byteLength,
          flags2
        );
        if (rc2 !== sqlite3.capi.SQLITE_OK) {
          throw new Error(`Failed to copy remote database: ${rc2}`);
        }
      } finally {
        sqlite3.wasm.scopedAllocPop(scope2);
      }

      // Instead of ATTACH, let's copy data row by row
      const rows = [];
      remoteDb.exec({
        sql: `SELECT ${colList} FROM ${tableName}`,
        callback: (row) => {
          rows.push([...row]);
        },
      });

      if (rows.length > 0) {
        const placeholders = commonColumns.map(() => "?").join(", ");
        const insertSql = `INSERT OR REPLACE INTO ${tableName} (${colList}) VALUES (${placeholders})`;
        const stmt = db.prepare(insertSql);

        for (const row of rows) {
          stmt.bind(row).step();
          stmt.reset();
        }
        stmt.finalize();

        counts[tableName] = rows.length;
        console.log(`Imported ${rows.length} rows into ${tableName}`);
      } else {
        counts[tableName] = 0;
      }

      remoteCopy.close();
    }

    // Update metadata from manifest
    if (manifest) {
      const metaUpdates = {
        dataSource: `pflanzenschutz-db@${manifest.version}`,
        lastSyncIso: new Date().toISOString(),
        lastSyncHash: manifest.hash || manifest.version,
      };

      if (manifest.tables) {
        metaUpdates.lastSyncCounts = JSON.stringify(manifest.tables);
      }

      if (manifest.api_version) {
        metaUpdates.apiStand = manifest.api_version;
      } else if (manifest.build && manifest.build.finished_at) {
        metaUpdates.apiStand = manifest.build.finished_at;
      }

      const metaStmt = db.prepare(
        "INSERT OR REPLACE INTO bvl_meta (key, value) VALUES (?, ?)"
      );
      for (const [key, value] of Object.entries(metaUpdates)) {
        metaStmt
          .bind([key, typeof value === "string" ? value : JSON.stringify(value)])
          .step();
        metaStmt.reset();
      }
      metaStmt.finalize();
    }

    // Verify database integrity
    const integrityResult = db.selectValue("PRAGMA integrity_check");
    if (integrityResult !== "ok") {
      throw new Error(`Database integrity check failed: ${integrityResult}`);
    }

    db.exec("COMMIT");
    remoteDb.close();

    console.log("BVL SQLite import complete", counts);
    return { success: true, counts };
  } catch (error) {
    db.exec("ROLLBACK");
    remoteDb.close();
    console.error("Failed to import BVL SQLite:", error);
    throw error;
  }
}

async function getBvlMeta(key) {
  if (!db) throw new Error("Database not initialized");

  let value = null;
  db.exec({
    sql: "SELECT value FROM bvl_meta WHERE key = ?",
    bind: [key],
    callback: (row) => {
      value = row[0];
    },
  });

  return value;
}

async function setBvlMeta(payload) {
  if (!db) throw new Error("Database not initialized");

  const { key, value } = payload;

  const stmt = db.prepare(
    "INSERT OR REPLACE INTO bvl_meta (key, value) VALUES (?, ?)"
  );
  stmt.bind([key, value]).step();
  stmt.finalize();

  return { success: true };
}

async function appendBvlSyncLog(payload) {
  if (!db) throw new Error("Database not initialized");

  const { synced_at, ok, message, payload_hash } = payload;

  const stmt = db.prepare(`
    INSERT INTO bvl_sync_log (synced_at, ok, message, payload_hash)
    VALUES (?, ?, ?, ?)
  `);
  stmt.bind([synced_at, ok, message, payload_hash]).step();
  stmt.finalize();

  return { success: true };
}

async function listBvlSyncLog(payload) {
  if (!db) throw new Error("Database not initialized");

  const limit = payload?.limit || 10;
  const logs = [];

  db.exec({
    sql: "SELECT id, synced_at, ok, message, payload_hash FROM bvl_sync_log ORDER BY id DESC LIMIT ?",
    bind: [limit],
    callback: (row) => {
      logs.push({
        id: row[0],
        synced_at: row[1],
        ok: row[2],
        message: row[3],
        payload_hash: row[4],
      });
    },
  });

  return logs;
}

async function queryZulassung(payload) {
  if (!db) throw new Error("Database not initialized");

  const { culture, pest, text, includeExpired } = payload || {};

  let sql = `
    SELECT DISTINCT
      m.kennr,
      m.name,
      m.formulierung,
      m.zul_ende,
      m.geringes_risiko,
      a.awg_id,
      a.status_json,
      a.zulassungsende
    FROM bvl_mittel m
    JOIN bvl_awg a ON m.kennr = a.kennr
  `;

  const conditions = [];
  const bindings = [];

  if (culture) {
    sql += ` JOIN bvl_awg_kultur ak ON a.awg_id = ak.awg_id `;
    conditions.push("ak.kultur = ?");
    bindings.push(culture);
  }

  if (pest) {
    sql += ` JOIN bvl_awg_schadorg aso ON a.awg_id = aso.awg_id `;
    conditions.push("aso.schadorg = ?");
    bindings.push(pest);
  }

  if (text) {
    const searchTerm = text.toLowerCase();
    const textPattern = `%${searchTerm}%`;
    conditions.push(`(
      LOWER(m.name) LIKE ? OR LOWER(m.kennr) LIKE ? OR
      EXISTS (
        SELECT 1
        FROM bvl_awg_kultur ak_filter
        LEFT JOIN bvl_lookup_kultur lk_filter ON lk_filter.code = ak_filter.kultur
        WHERE ak_filter.awg_id = a.awg_id
          AND (
            LOWER(ak_filter.kultur) LIKE ? OR
            LOWER(IFNULL(lk_filter.label, '')) LIKE ?
          )
      ) OR
      EXISTS (
        SELECT 1
        FROM bvl_awg_schadorg aso_filter
        LEFT JOIN bvl_lookup_schadorg ls_filter ON ls_filter.code = aso_filter.schadorg
        WHERE aso_filter.awg_id = a.awg_id
          AND (
            LOWER(aso_filter.schadorg) LIKE ? OR
            LOWER(IFNULL(ls_filter.label, '')) LIKE ?
          )
      )
    )`);
    bindings.push(
      textPattern,
      textPattern,
      textPattern,
      textPattern,
      textPattern,
      textPattern
    );
  }

  if (!includeExpired) {
    conditions.push("(m.zul_ende IS NULL OR m.zul_ende >= date('now'))");
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  sql += " ORDER BY m.name";

  const results = [];

  db.exec({
    sql,
    bind: bindings,
    callback: (row) => {
      results.push({
        kennr: row[0],
        name: row[1],
        formulierung: row[2],
        zul_ende: row[3],
        geringes_risiko: row[4],
        awg_id: row[5],
        status_json: row[6],
        zulassungsende: row[7],
      });
    },
  });

  // Enrich each result with detailed information
  for (const result of results) {
    // Get cultures
    result.kulturen = [];
    db.exec({
      sql: `
        SELECT ak.kultur, ak.ausgenommen, ak.sortier_nr, IFNULL(lk.label, ak.kultur) as label
        FROM bvl_awg_kultur ak
        LEFT JOIN bvl_lookup_kultur lk ON lk.code = ak.kultur
        WHERE ak.awg_id = ? 
        ORDER BY ak.sortier_nr
      `,
      bind: [result.awg_id],
      callback: (row) => {
        result.kulturen.push({
          kultur: row[0],
          ausgenommen: row[1],
          sortier_nr: row[2],
          label: row[3],
        });
      },
    });

    // Get schadorganismen
    result.schadorganismen = [];
    db.exec({
      sql: `
        SELECT aso.schadorg, aso.ausgenommen, aso.sortier_nr, IFNULL(ls.label, aso.schadorg) as label
        FROM bvl_awg_schadorg aso
        LEFT JOIN bvl_lookup_schadorg ls ON ls.code = aso.schadorg
        WHERE aso.awg_id = ? 
        ORDER BY aso.sortier_nr
      `,
      bind: [result.awg_id],
      callback: (row) => {
        result.schadorganismen.push({
          schadorg: row[0],
          ausgenommen: row[1],
          sortier_nr: row[2],
          label: row[3],
        });
      },
    });

    // Get aufwände
    result.aufwaende = [];
    db.exec({
      sql: `
        SELECT aufwand_bedingung, sortier_nr, mittel_menge, mittel_einheit,
               wasser_menge, wasser_einheit, payload_json
        FROM bvl_awg_aufwand 
        WHERE awg_id = ? 
        ORDER BY sortier_nr
      `,
      bind: [result.awg_id],
      callback: (row) => {
        result.aufwaende.push({
          aufwand_bedingung: row[0],
          sortier_nr: row[1],
          mittel_menge: row[2],
          mittel_einheit: row[3],
          wasser_menge: row[4],
          wasser_einheit: row[5],
          payload_json: row[6],
        });
      },
    });

    // Get wartezeiten
    result.wartezeiten = [];
    db.exec({
      sql: `
        SELECT w.awg_wartezeit_nr, w.kultur, w.sortier_nr, w.tage, 
               w.bemerkung_kode, w.anwendungsbereich, w.erlaeuterung, w.payload_json,
               IFNULL(lk.label, w.kultur) as kultur_label
        FROM bvl_awg_wartezeit w
        LEFT JOIN bvl_lookup_kultur lk ON lk.code = w.kultur
        WHERE w.awg_id = ? 
        ORDER BY w.sortier_nr
      `,
      bind: [result.awg_id],
      callback: (row) => {
        result.wartezeiten.push({
          awg_wartezeit_nr: row[0],
          kultur: row[1],
          sortier_nr: row[2],
          tage: row[3],
          bemerkung_kode: row[4],
          anwendungsbereich: row[5],
          erlaeuterung: row[6],
          payload_json: row[7],
          kultur_label: row[8],
        });
      },
    });

    // Get wirkstoffe (if table exists)
    result.wirkstoffe = [];
    let wirkstoffeTableExists = false;
    db.exec({
      sql: "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bvl_mittel_wirkstoffe'",
      callback: () => { wirkstoffeTableExists = true; },
    });
    if (wirkstoffeTableExists) {
      db.exec({
        sql: "SELECT * FROM bvl_mittel_wirkstoffe WHERE kennr = ?",
        bind: [result.kennr],
        callback: (row) => {
          // Store all columns dynamically
          const wirkstoff = {};
          const colNames = db.exec({
            sql: "PRAGMA table_info(bvl_mittel_wirkstoffe)",
            returnValue: "resultRows"
          });
          colNames.forEach((col, idx) => {
            wirkstoff[col[1]] = row[idx];
          });
          result.wirkstoffe.push(wirkstoff);
        },
      });
    }

    // Get gefahrhinweise (if table exists)
    result.gefahrhinweise = [];
    let gefahrTableExists = false;
    db.exec({
      sql: "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bvl_mittel_gefahrhinweise'",
      callback: () => { gefahrTableExists = true; },
    });
    if (gefahrTableExists) {
      db.exec({
        sql: "SELECT * FROM bvl_mittel_gefahrhinweise WHERE kennr = ?",
        bind: [result.kennr],
        callback: (row) => {
          const gefahr = {};
          const colNames = db.exec({
            sql: "PRAGMA table_info(bvl_mittel_gefahrhinweise)",
            returnValue: "resultRows"
          });
          colNames.forEach((col, idx) => {
            gefahr[col[1]] = row[idx];
          });
          result.gefahrhinweise.push(gefahr);
        },
      });
    }

    // Get vertrieb/hersteller (if table exists)
    result.vertrieb = [];
    let vertriebTableExists = false;
    db.exec({
      sql: "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bvl_mittel_vertrieb'",
      callback: () => { vertriebTableExists = true; },
    });
    if (vertriebTableExists) {
      db.exec({
        sql: "SELECT * FROM bvl_mittel_vertrieb WHERE kennr = ?",
        bind: [result.kennr],
        callback: (row) => {
          const vert = {};
          const colNames = db.exec({
            sql: "PRAGMA table_info(bvl_mittel_vertrieb)",
            returnValue: "resultRows"
          });
          colNames.forEach((col, idx) => {
            vert[col[1]] = row[idx];
          });
          result.vertrieb.push(vert);
        },
      });
    }

    // Check for extras/flags (bio, öko, etc.) if table exists
    let extrasTableExists = false;
    db.exec({
      sql: "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bvl_mittel_extras'",
      callback: () => { extrasTableExists = true; },
    });
    if (extrasTableExists) {
      db.exec({
        sql: "SELECT * FROM bvl_mittel_extras WHERE kennr = ?",
        bind: [result.kennr],
        callback: (row) => {
          result.extras = {};
          const colNames = db.exec({
            sql: "PRAGMA table_info(bvl_mittel_extras)",
            returnValue: "resultRows"
          });
          colNames.forEach((col, idx) => {
            result.extras[col[1]] = row[idx];
          });
        },
      });
    }
  }

  return results;
}

async function listBvlCultures(payload) {
  if (!db) throw new Error("Database not initialized");

  const withCount = payload?.withCount || false;
  const cultures = [];

  let sql;

  if (withCount) {
    sql = `
      SELECT ak.kultur, IFNULL(lk.label, ak.kultur) AS label, COUNT(*) AS count
      FROM bvl_awg_kultur ak
      LEFT JOIN bvl_lookup_kultur lk ON lk.code = ak.kultur
      WHERE ak.ausgenommen = 0
  GROUP BY ak.kultur, label
  ORDER BY label COLLATE NOCASE
    `;
  } else {
    sql = `
      SELECT DISTINCT ak.kultur, IFNULL(lk.label, ak.kultur) AS label
      FROM bvl_awg_kultur ak
      LEFT JOIN bvl_lookup_kultur lk ON lk.code = ak.kultur
  WHERE ak.ausgenommen = 0
  ORDER BY label COLLATE NOCASE
    `;
  }

  db.exec({
    sql,
    callback: (row) => {
      if (withCount) {
        cultures.push({ code: row[0], label: row[1], count: row[2] });
      } else {
        cultures.push({ code: row[0], label: row[1] });
      }
    },
  });

  return cultures;
}

async function listBvlSchadorg(payload) {
  if (!db) throw new Error("Database not initialized");

  const withCount = payload?.withCount || false;
  const schadorg = [];

  let sql;

  if (withCount) {
    sql = `
      SELECT aso.schadorg, IFNULL(ls.label, aso.schadorg) AS label, COUNT(*) AS count
      FROM bvl_awg_schadorg aso
      LEFT JOIN bvl_lookup_schadorg ls ON ls.code = aso.schadorg
      WHERE aso.ausgenommen = 0
  GROUP BY aso.schadorg, label
  ORDER BY label COLLATE NOCASE
    `;
  } else {
    sql = `
      SELECT DISTINCT aso.schadorg, IFNULL(ls.label, aso.schadorg) AS label
      FROM bvl_awg_schadorg aso
      LEFT JOIN bvl_lookup_schadorg ls ON ls.code = aso.schadorg
  WHERE aso.ausgenommen = 0
  ORDER BY label COLLATE NOCASE
    `;
  }

  db.exec({
    sql,
    callback: (row) => {
      if (withCount) {
        schadorg.push({ code: row[0], label: row[1], count: row[2] });
      } else {
        schadorg.push({ code: row[0], label: row[1] });
      }
    },
  });

  return schadorg;
}

async function diagnoseBvlSchema() {
  if (!db) throw new Error("Database not initialized");

  const schema = {};
  const tables = [
    "bvl_mittel",
    "bvl_awg",
    "bvl_awg_kultur",
    "bvl_awg_schadorg",
    "bvl_awg_aufwand",
    "bvl_awg_wartezeit",
    "bvl_meta",
    "bvl_sync_log",
    "bvl_lookup_kultur",
    "bvl_lookup_schadorg",
  ];

  for (const table of tables) {
    schema[table] = {
      columns: [],
      indices: [],
    };

    // Get column info
    db.exec({
      sql: `PRAGMA table_info(${table})`,
      callback: (row) => {
        schema[table].columns.push({
          cid: row[0],
          name: row[1],
          type: row[2],
          notnull: row[3],
          dflt_value: row[4],
          pk: row[5],
        });
      },
    });

    // Get indices
    db.exec({
      sql: `PRAGMA index_list(${table})`,
      callback: (row) => {
        schema[table].indices.push({
          seq: row[0],
          name: row[1],
          unique: row[2],
          origin: row[3],
          partial: row[4],
        });
      },
    });
  }

  const userVersion = db.selectValue("PRAGMA user_version");

  return {
    user_version: userVersion,
    tables: schema,
  };
}
