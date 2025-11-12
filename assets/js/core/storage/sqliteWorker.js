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
      case "listBvlMittel":
        result = await listBvlMittel(payload);
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

const tableColumnCache = new Map();

function resetTableColumnCache() {
  tableColumnCache.clear();
}

function getTableColumns(tableName) {
  if (!db) throw new Error("Database not initialized");
  if (tableColumnCache.has(tableName)) {
    return tableColumnCache.get(tableName);
  }

  const columns = [];
  db.exec({
    sql: `PRAGMA table_info(${tableName})`,
    callback: (row) => {
      columns.push(row[1]);
    },
  });

  tableColumnCache.set(tableName, columns);
  return columns;
}

function hasTableColumn(tableName, columnName) {
  const columns = getTableColumns(tableName);
  return columns.includes(columnName);
}

function safeJsonParse(value) {
  if (value === null || value === undefined) {
    return null;
  }

  try {
    return JSON.parse(typeof value === "string" ? value : String(value));
  } catch (error) {
    return null;
  }
}

function pickFirstNonEmpty(...values) {
  for (const value of values) {
    if (value === null || value === undefined) {
      continue;
    }

    if (typeof value === "string") {
      const trimmed = value.trim();
      if (trimmed === "") {
        continue;
      }
      return trimmed;
    }

    if (Array.isArray(value) && value.length === 0) {
      continue;
    }

    return value;
  }

  return null;
}

function toBooleanFlag(value) {
  if (value === null || value === undefined) {
    return 0;
  }

  if (typeof value === "number") {
    return value > 0 ? 1 : 0;
  }

  if (typeof value === "boolean") {
    return value ? 1 : 0;
  }

  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (!normalized) {
      return 0;
    }
    if (["1", "true", "ja", "yes", "y"].includes(normalized)) {
      return 1;
    }
    return 0;
  }

  return 0;
}

function hasTable(tableName) {
  if (!db) throw new Error("Database not initialized");
  let exists = false;
  db.exec({
    sql: "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ? LIMIT 1",
    bind: [tableName],
    callback: () => {
      exists = true;
    },
  });
  return exists;
}

function pickColumn(columns, ...candidates) {
  for (const candidate of candidates) {
    if (candidate && columns.includes(candidate)) {
      return candidate;
    }
  }
  return null;
}

function buildAwgKulturQueryConfig() {
  if (!hasTable("bvl_awg_kultur")) {
    return null;
  }

  const columns = getTableColumns("bvl_awg_kultur");
  if (!columns.length) {
    return null;
  }

  const codeCol = pickColumn(
    columns,
    "kultur",
    "kultur_kode",
    "kulturcode",
    "kultur_code"
  );
  if (!codeCol) {
    return null;
  }

  const labelCol = pickColumn(
    columns,
    "label",
    "kultur_label",
    "kulturtext",
    "kultur_text",
    "kultur_bez"
  );
  const excludeCol = pickColumn(
    columns,
    "ausgenommen",
    "ausnahme",
    "ausgeschl"
  );
  const sortCol = pickColumn(
    columns,
    "sortier_nr",
    "sortier_index",
    "sort",
    "sortierung"
  );

  const codeExpr = `ak.${codeCol}`;
  const trimmedCodeExpr = `TRIM(${codeExpr})`;
  const labelBase = labelCol ? `TRIM(ak.${labelCol})` : trimmedCodeExpr;
  const fallbackLabelExpr = `COALESCE(NULLIF(${labelBase}, ''), ${trimmedCodeExpr})`;
  const lookupAvailable = hasTable("bvl_lookup_kultur");
  const labelExpr = lookupAvailable
    ? `COALESCE(NULLIF(TRIM(lk.label), ''), ${fallbackLabelExpr})`
    : fallbackLabelExpr;
  const excludeExpr = excludeCol ? `COALESCE(ak.${excludeCol}, 0)` : "0";
  const sortExpr = sortCol ? `ak.${sortCol}` : "ak.rowid";
  const orderExpr = sortCol ? "sort_order" : "label";

  const joinClause = lookupAvailable
    ? `
    LEFT JOIN bvl_lookup_kultur lk
      ON TRIM(lk.code) = ${trimmedCodeExpr}
  `
    : "";

  const sql = `
    SELECT ${trimmedCodeExpr} AS code,
           ${labelExpr} AS label,
           ${excludeExpr} AS excluded,
           ${sortExpr} AS sort_order
    FROM bvl_awg_kultur ak
    ${joinClause}
    WHERE ak.awg_id = ?
    ORDER BY ${orderExpr}
  `;

  return { sql };
}

function buildAwgSchadorgQueryConfig() {
  if (!hasTable("bvl_awg_schadorg")) {
    return null;
  }

  const columns = getTableColumns("bvl_awg_schadorg");
  if (!columns.length) {
    return null;
  }

  const codeCol = pickColumn(
    columns,
    "schadorg",
    "schadorg_kode",
    "schadorgcode",
    "schadorganismus"
  );
  if (!codeCol) {
    return null;
  }

  const labelCol = pickColumn(
    columns,
    "label",
    "schadorg_label",
    "schadorg_text",
    "schadorganismus_text",
    "schadorg_bez"
  );
  const excludeCol = pickColumn(
    columns,
    "ausgenommen",
    "ausnahme",
    "ausgeschl"
  );
  const sortCol = pickColumn(
    columns,
    "sortier_nr",
    "sortier_index",
    "sort",
    "sortierung"
  );

  const codeExpr = `aso.${codeCol}`;
  const trimmedCodeExpr = `TRIM(${codeExpr})`;
  const labelBase = labelCol ? `TRIM(aso.${labelCol})` : trimmedCodeExpr;
  const fallbackLabelExpr = `COALESCE(NULLIF(${labelBase}, ''), ${trimmedCodeExpr})`;
  const lookupAvailable = hasTable("bvl_lookup_schadorg");
  const labelExpr = lookupAvailable
    ? `COALESCE(NULLIF(TRIM(ls.label), ''), ${fallbackLabelExpr})`
    : fallbackLabelExpr;
  const excludeExpr = excludeCol ? `COALESCE(aso.${excludeCol}, 0)` : "0";
  const sortExpr = sortCol ? `aso.${sortCol}` : "aso.rowid";
  const orderExpr = sortCol ? "sort_order" : "label";

  const joinClause = lookupAvailable
    ? `
    LEFT JOIN bvl_lookup_schadorg ls
      ON TRIM(ls.code) = ${trimmedCodeExpr}
  `
    : "";

  const sql = `
    SELECT ${trimmedCodeExpr} AS code,
           ${labelExpr} AS label,
           ${excludeExpr} AS excluded,
           ${sortExpr} AS sort_order
    FROM bvl_awg_schadorg aso
    ${joinClause}
    WHERE aso.awg_id = ?
    ORDER BY ${orderExpr}
  `;

  return { sql };
}

function buildAwgAufwandQueryConfig() {
  if (!hasTable("bvl_awg_aufwand")) {
    return null;
  }

  const columns = getTableColumns("bvl_awg_aufwand");
  if (!columns.length) {
    return null;
  }

  const conditionCol = pickColumn(
    columns,
    "aufwand_bedingung",
    "aufwand_bed",
    "bedingung",
    "aufwand_bedingung_text"
  );
  const sortCol = pickColumn(
    columns,
    "sortier_nr",
    "sortier_index",
    "sort",
    "sortierung"
  );

  const mittelValueCol = pickColumn(
    columns,
    "mittel_menge",
    "aufwand_menge",
    "aufwandmenge",
    "aufwand"
  );
  const mittelMinCol = pickColumn(
    columns,
    "mittel_menge_von",
    "aufwandmenge_min",
    "aufwandmenge_von"
  );
  const mittelMaxCol = pickColumn(
    columns,
    "mittel_menge_bis",
    "aufwandmenge_max",
    "aufwandmenge_bis"
  );
  const mittelUnitCol = pickColumn(
    columns,
    "mittel_einheit",
    "aufwandmenge_einheit",
    "aufwand_unit"
  );

  const wasserValueCol = pickColumn(columns, "wasser_menge", "wassermenge");
  const wasserMinCol = pickColumn(
    columns,
    "wasser_menge_von",
    "wassermenge_min"
  );
  const wasserMaxCol = pickColumn(
    columns,
    "wasser_menge_bis",
    "wassermenge_max"
  );
  const wasserUnitCol = pickColumn(
    columns,
    "wasser_einheit",
    "wassermenge_einheit"
  );

  const payloadCol = columns.includes("payload_json") ? "payload_json" : null;

  const conditionExpr = conditionCol
    ? `COALESCE(NULLIF(TRIM(a.${conditionCol}), ''), 'Standard')`
    : "'Standard'";
  const sortExpr = sortCol ? `a.${sortCol}` : "a.rowid";

  const sql = `
    SELECT
      ${conditionExpr} AS condition,
      ${sortExpr} AS sort_order,
      ${mittelValueCol ? `a.${mittelValueCol}` : "NULL"} AS mittel_value,
      ${mittelMinCol ? `a.${mittelMinCol}` : "NULL"} AS mittel_min,
      ${mittelMaxCol ? `a.${mittelMaxCol}` : "NULL"} AS mittel_max,
      ${mittelUnitCol ? `a.${mittelUnitCol}` : "NULL"} AS mittel_unit,
      ${wasserValueCol ? `a.${wasserValueCol}` : "NULL"} AS wasser_value,
      ${wasserMinCol ? `a.${wasserMinCol}` : "NULL"} AS wasser_min,
      ${wasserMaxCol ? `a.${wasserMaxCol}` : "NULL"} AS wasser_max,
      ${wasserUnitCol ? `a.${wasserUnitCol}` : "NULL"} AS wasser_unit,
      ${payloadCol ? `a.${payloadCol}` : "NULL"} AS payload_json
    FROM bvl_awg_aufwand a
    WHERE a.awg_id = ?
    ORDER BY ${sortCol ? "sort_order" : "a.rowid"}
  `;

  return { sql };
}

function buildAwgWartezeitQueryConfig() {
  if (!hasTable("bvl_awg_wartezeit")) {
    return null;
  }

  const columns = getTableColumns("bvl_awg_wartezeit");
  if (!columns.length) {
    return null;
  }

  const kulturCol = pickColumn(
    columns,
    "kultur",
    "kultur_kode",
    "kulturcode",
    "kultur_code"
  );
  if (!kulturCol) {
    return null;
  }

  const labelCol = pickColumn(
    columns,
    "kultur_label",
    "kultur_text",
    "kultur_bez",
    "kulturname"
  );
  const sortCol = pickColumn(
    columns,
    "sortier_nr",
    "sortier_index",
    "sort",
    "sortierung"
  );
  const tageCol = pickColumn(columns, "tage", "wartezeit_tage", "wartezeit");
  const bemerkungCol = pickColumn(
    columns,
    "bemerkung_kode",
    "bemerkung",
    "wartezeit_text",
    "hinweis"
  );
  const anwendungsbereichCol = pickColumn(
    columns,
    "anwendungsbereich",
    "bereich"
  );
  const erlaeuterungCol = pickColumn(
    columns,
    "erlaeuterung",
    "zusatztext",
    "hinweis_text"
  );
  const payloadCol = columns.includes("payload_json") ? "payload_json" : null;

  const codeExpr = `w.${kulturCol}`;
  const trimmedCodeExpr = `TRIM(${codeExpr})`;
  const labelBase = labelCol ? `TRIM(w.${labelCol})` : trimmedCodeExpr;
  const fallbackLabelExpr = `COALESCE(NULLIF(${labelBase}, ''), ${trimmedCodeExpr})`;
  const lookupAvailable = hasTable("bvl_lookup_kultur");
  const labelExpr = lookupAvailable
    ? `COALESCE(NULLIF(TRIM(lk.label), ''), ${fallbackLabelExpr})`
    : fallbackLabelExpr;
  const sortExpr = sortCol ? `w.${sortCol}` : "w.rowid";
  const tageExpr = tageCol ? `w.${tageCol}` : "NULL";
  const bemerkungExpr = bemerkungCol ? `w.${bemerkungCol}` : "NULL";
  const anwendungsbereichExpr = anwendungsbereichCol
    ? `w.${anwendungsbereichCol}`
    : "NULL";
  const erlaeuterungExpr = erlaeuterungCol ? `w.${erlaeuterungCol}` : "NULL";
  const joinClause = lookupAvailable
    ? `
    LEFT JOIN bvl_lookup_kultur lk
      ON TRIM(lk.code) = ${trimmedCodeExpr}
  `
    : "";

  const sql = `
    SELECT
      ${trimmedCodeExpr} AS kultur_code,
      ${labelExpr} AS kultur_label,
      ${sortExpr} AS sort_order,
      ${tageExpr} AS tage,
      ${bemerkungExpr} AS bemerkung,
      ${anwendungsbereichExpr} AS anwendungsbereich,
      ${erlaeuterungExpr} AS erlaeuterung,
      ${payloadCol ? `w.${payloadCol}` : "NULL"} AS payload_json
    FROM bvl_awg_wartezeit w
    ${joinClause}
    WHERE w.awg_id = ?
    ORDER BY ${sortCol ? "sort_order" : "w.rowid"}
  `;

  return { sql };
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

  if (currentVersion < 3) {
    console.log("Migrating database to version 3...");

    db.exec("BEGIN TRANSACTION");

    try {
      db.exec("PRAGMA user_version = 3;");
      db.exec("COMMIT");
      console.log("Database migrated to version 3 successfully");
    } catch (error) {
      db.exec("ROLLBACK");
      console.error("Migration to version 3 failed:", error);
      throw error;
    }
  }

  if (currentVersion < 4) {
    console.log("Migrating database to version 4...");

    db.exec("BEGIN TRANSACTION");

    try {
      db.exec(`
        DROP TABLE IF EXISTS bvl_mittel_extras;
        DROP INDEX IF EXISTS idx_mittel_extras_bio;
        DROP INDEX IF EXISTS idx_mittel_extras_oeko;
      `);

      db.exec("PRAGMA user_version = 4;");
      db.exec("COMMIT");
      console.log("Database migrated to version 4 successfully");
    } catch (error) {
      db.exec("ROLLBACK");
      console.error("Migration to version 4 failed:", error);
      throw error;
    }
  }

  resetTableColumnCache();
}

function hydrateBvlMittelFromPayload() {
  if (!db) throw new Error("Database not initialized");

  const columns = getTableColumns("bvl_mittel");
  const hasName = columns.includes("name");
  const hasMittelname = columns.includes("mittelname");
  const hasFormulierung = columns.includes("formulierung");
  const hasFormulierungArt = columns.includes("formulierung_art");
  const hasZulEnde = columns.includes("zul_ende");
  const hasZulassungsende = columns.includes("zulassungsende");
  const hasGeringesRisiko = columns.includes("geringes_risiko");

  const assignments = [];

  if (hasName) {
    assignments.push(`
      name = COALESCE(
        NULLIF(TRIM(name), ''),
        ${hasMittelname ? "NULLIF(TRIM(mittelname), '')," : ""}
        NULLIF(TRIM(json_extract(payload_json, '$.mittelname')), ''),
        NULLIF(TRIM(json_extract(payload_json, '$.mittel_name')), ''),
        NULLIF(TRIM(json_extract(payload_json, '$.mittelName')), ''),
        name
      )
    `);
  }

  if (!hasName && hasMittelname) {
    assignments.push(`
      mittelname = COALESCE(
        NULLIF(TRIM(mittelname), ''),
        NULLIF(TRIM(json_extract(payload_json, '$.mittelname')), ''),
        NULLIF(TRIM(json_extract(payload_json, '$.mittel_name')), ''),
        NULLIF(TRIM(json_extract(payload_json, '$.mittelName')), ''),
        mittelname
      )
    `);
  }

  if (hasFormulierung || hasFormulierungArt) {
    const targetColumn = hasFormulierung ? "formulierung" : "formulierung_art";
    assignments.push(`
      ${targetColumn} = COALESCE(
        NULLIF(TRIM(${targetColumn}), ''),
        NULLIF(TRIM(json_extract(payload_json, '$.formulierung')), ''),
        NULLIF(TRIM(json_extract(payload_json, '$.formulierung_art')), ''),
        ${targetColumn}
      )
    `);
  }

  if (hasZulEnde || hasZulassungsende) {
    const targetColumn = hasZulEnde ? "zul_ende" : "zulassungsende";
    assignments.push(`
      ${targetColumn} = COALESCE(
        NULLIF(TRIM(${targetColumn}), ''),
        NULLIF(TRIM(json_extract(payload_json, '$.zul_ende')), ''),
        NULLIF(TRIM(json_extract(payload_json, '$.zulassungsende')), ''),
        ${targetColumn}
      )
    `);
  }

  if (assignments.length) {
    db.exec(`
      UPDATE bvl_mittel
      SET ${assignments
        .map((assignment) => assignment.replace(/\s+/g, " ").trim())
        .join(", ")};
    `);
  }

  if (hasGeringesRisiko) {
    db.exec(`
      UPDATE bvl_mittel
      SET geringes_risiko = COALESCE(
        geringes_risiko,
        CASE
          WHEN json_valid(payload_json)
            THEN CASE
              WHEN CAST(json_extract(payload_json, '$.mittel_mit_geringem_risiko') AS INTEGER) = 1 THEN 1
              WHEN LOWER(json_extract(payload_json, '$.mittel_mit_geringem_risiko')) IN ('true', 'ja') THEN 1
              ELSE 0
            END
          ELSE 0
        END,
        0
      );
    `);
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
    resetTableColumnCache();
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

  console.log(
    `Found ${bvlTables.length} BVL tables in remote database:`,
    bvlTables
  );

  // Begin transaction on main database
  const wasForeignKeysEnabled =
    Number(db.selectValue("PRAGMA foreign_keys") || 0) === 1;
  db.exec("PRAGMA foreign_keys = OFF");
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
          console.warn(
            `Could not get CREATE statement for ${tableName}, skipping`
          );
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
        console.warn(
          `No common columns between remote and main ${tableName}, skipping`
        );
        continue;
      }

      const colList = commonColumns.join(", ");

      // Copy the remote database into a temporary in-memory instance
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

    hydrateBvlMittelFromPayload();

    // Update metadata from manifest
    if (manifest) {
      const manifestCounts = manifest.tables ? { ...manifest.tables } : {};

      const metaUpdates = {
        dataSource: `pflanzenschutz-db@${manifest.version}`,
        lastSyncIso: new Date().toISOString(),
        lastSyncHash: manifest.hash || manifest.version,
      };

      if (Object.keys(manifestCounts).length > 0) {
        metaUpdates.lastSyncCounts = JSON.stringify(manifestCounts);
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
          .bind([
            key,
            typeof value === "string" ? value : JSON.stringify(value),
          ])
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

    console.log("BVL SQLite import complete", counts);
    resetTableColumnCache();
    return { success: true, counts };
  } catch (error) {
    db.exec("ROLLBACK");
    console.error("Failed to import BVL SQLite:", error);
    throw error;
  } finally {
    try {
      if (remoteDb) {
        remoteDb.close();
      }
    } catch (closeError) {
      console.warn("Failed to close remote database after import", closeError);
    }

    db.exec(`PRAGMA foreign_keys = ${wasForeignKeysEnabled ? 1 : 0}`);
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

  const { culture, pest, text, includeExpired, mittel } = payload || {};

  const clean = (expr) => expr.replace(/\s+/g, " ").trim();

  const mittelColumns = getTableColumns("bvl_mittel");
  const awgColumns = getTableColumns("bvl_awg");
  const mittelHasPayload = mittelColumns.includes("payload_json");
  const awgHasPayload = awgColumns.includes("payload_json");
  const kulturConfig = buildAwgKulturQueryConfig();
  const schadorgConfig = buildAwgSchadorgQueryConfig();
  const aufwandConfig = buildAwgAufwandQueryConfig();
  const wartezeitConfig = buildAwgWartezeitQueryConfig();

  const nameExprParts = [];
  if (mittelColumns.includes("name")) {
    nameExprParts.push("NULLIF(TRIM(m.name), '')");
  }
  if (mittelColumns.includes("mittelname")) {
    nameExprParts.push("NULLIF(TRIM(m.mittelname), '')");
  }
  if (mittelColumns.includes("mittel_name")) {
    nameExprParts.push("NULLIF(TRIM(m.mittel_name), '')");
  }
  if (mittelHasPayload) {
    nameExprParts.push(
      "NULLIF(TRIM(json_extract(m.payload_json, '$.mittelname')), '')"
    );
    nameExprParts.push(
      "NULLIF(TRIM(json_extract(m.payload_json, '$.mittel_name')), '')"
    );
    nameExprParts.push(
      "NULLIF(TRIM(json_extract(m.payload_json, '$.mittelName')), '')"
    );
    nameExprParts.push(
      "NULLIF(TRIM(json_extract(m.payload_json, '$.name')), '')"
    );
  }
  const rawNameExpr = nameExprParts.length
    ? clean(`COALESCE(${nameExprParts.join(", ")}, m.kennr)`)
    : "m.kennr";
  const selectNameExpr = `${rawNameExpr} AS name`;

  const formulierungExprParts = [];
  if (mittelColumns.includes("formulierung")) {
    formulierungExprParts.push("NULLIF(TRIM(m.formulierung), '')");
  }
  if (mittelColumns.includes("formulierung_art")) {
    formulierungExprParts.push("NULLIF(TRIM(m.formulierung_art), '')");
  }
  if (mittelHasPayload) {
    formulierungExprParts.push(
      "NULLIF(TRIM(json_extract(m.payload_json, '$.formulierung')), '')"
    );
    formulierungExprParts.push(
      "NULLIF(TRIM(json_extract(m.payload_json, '$.formulierung_art')), '')"
    );
  }
  const rawFormulierungExpr = formulierungExprParts.length
    ? clean(`COALESCE(${formulierungExprParts.join(", ")})`)
    : "NULL";
  const selectFormulierungExpr = `${rawFormulierungExpr} AS formulierung`;

  const zulEndeExprParts = [];
  if (mittelColumns.includes("zul_ende")) {
    zulEndeExprParts.push("NULLIF(TRIM(m.zul_ende), '')");
  }
  if (mittelColumns.includes("zulassungsende")) {
    zulEndeExprParts.push("NULLIF(TRIM(m.zulassungsende), '')");
  }
  if (awgColumns.includes("zulassungsende")) {
    zulEndeExprParts.push("NULLIF(TRIM(a.zulassungsende), '')");
  }
  if (mittelHasPayload) {
    zulEndeExprParts.push(
      "NULLIF(TRIM(json_extract(m.payload_json, '$.zul_ende')), '')"
    );
    zulEndeExprParts.push(
      "NULLIF(TRIM(json_extract(m.payload_json, '$.zulassungsende')), '')"
    );
    zulEndeExprParts.push(
      "NULLIF(TRIM(json_extract(m.payload_json, '$.gueltig_bis')), '')"
    );
  }
  const rawZulEndeExpr = zulEndeExprParts.length
    ? clean(`COALESCE(${zulEndeExprParts.join(", ")})`)
    : null;
  const selectZulEndeExpr = rawZulEndeExpr
    ? `${rawZulEndeExpr} AS zul_ende`
    : "NULL AS zul_ende";

  const geringesParts = [];
  if (mittelColumns.includes("geringes_risiko")) {
    geringesParts.push("m.geringes_risiko");
  }
  if (mittelHasPayload) {
    geringesParts.push(`CASE
      WHEN json_valid(m.payload_json)
        THEN CASE
          WHEN CAST(json_extract(m.payload_json, '$.mittel_mit_geringem_risiko') AS INTEGER) = 1 THEN 1
          WHEN LOWER(json_extract(m.payload_json, '$.mittel_mit_geringem_risiko')) IN ('true', 'ja') THEN 1
          ELSE 0
        END
      ELSE 0
    END`);
  }
  const selectGeringesExpr = geringesParts.length
    ? clean(`COALESCE(${geringesParts.join(", ")}, 0) AS geringes_risiko`)
    : "0 AS geringes_risiko";

  const statusExpr = awgColumns.includes("status_json")
    ? "a.status_json"
    : awgColumns.includes("status")
      ? "json_object('status', a.status)"
      : awgColumns.includes("status_text")
        ? "json_object('status', a.status_text)"
        : "NULL";
  const awgPayloadExpr = awgHasPayload ? "a.payload_json" : "NULL";
  const mittelPayloadExpr = mittelHasPayload ? "m.payload_json" : "NULL";

  const selectFields = [
    "m.kennr",
    selectNameExpr,
    selectFormulierungExpr,
    selectZulEndeExpr,
    selectGeringesExpr,
    "a.awg_id",
    clean(`${statusExpr} AS status_payload`),
    clean(`${awgPayloadExpr} AS awg_payload`),
    clean(`${mittelPayloadExpr} AS mittel_payload`),
  ];

  const selectClause = selectFields.map(clean).join(",\n      ");

  let sql = `
    SELECT DISTINCT
      ${selectClause}
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

    const textColumnExprs = ["LOWER(m.kennr) LIKE ?"];
    if (mittelColumns.includes("name")) {
      textColumnExprs.push("LOWER(m.name) LIKE ?");
    }
    if (mittelColumns.includes("mittelname")) {
      textColumnExprs.push("LOWER(m.mittelname) LIKE ?");
    }
    if (mittelColumns.includes("mittel_name")) {
      textColumnExprs.push("LOWER(m.mittel_name) LIKE ?");
    }
    if (mittelHasPayload) {
      textColumnExprs.push(
        "LOWER(json_extract(m.payload_json, '$.mittelname')) LIKE ?"
      );
      textColumnExprs.push(
        "LOWER(json_extract(m.payload_json, '$.mittel_name')) LIKE ?"
      );
      textColumnExprs.push(
        "LOWER(json_extract(m.payload_json, '$.mittelName')) LIKE ?"
      );
    }

    const cultureFilterClause = `EXISTS (
        SELECT 1
        FROM bvl_awg_kultur ak_filter
        LEFT JOIN bvl_lookup_kultur lk_filter ON lk_filter.code = ak_filter.kultur
        WHERE ak_filter.awg_id = a.awg_id
          AND (
            LOWER(ak_filter.kultur) LIKE ? OR
            LOWER(IFNULL(lk_filter.label, '')) LIKE ?
          )
      )`;

    const pestFilterClause = `EXISTS (
        SELECT 1
        FROM bvl_awg_schadorg aso_filter
        LEFT JOIN bvl_lookup_schadorg ls_filter ON ls_filter.code = aso_filter.schadorg
        WHERE aso_filter.awg_id = a.awg_id
          AND (
            LOWER(aso_filter.schadorg) LIKE ? OR
            LOWER(IFNULL(ls_filter.label, '')) LIKE ?
          )
      )`;

    const combinedTextConditions = [
      ...textColumnExprs.map((expr) => `(${expr})`),
      cultureFilterClause,
      pestFilterClause,
    ];

    conditions.push(`(${combinedTextConditions.join(" OR ")})`);

    for (let i = 0; i < textColumnExprs.length; i += 1) {
      bindings.push(textPattern);
    }
    bindings.push(textPattern, textPattern); // culture
    bindings.push(textPattern, textPattern); // pest
  }

  if (mittel) {
    const normalized = String(mittel).trim().toLowerCase();
    const isExactKennr = /^[0-9a-z]+-[0-9a-z]+$/i.test(normalized);
    if (isExactKennr) {
      conditions.push("LOWER(m.kennr) = ?");
      bindings.push(normalized);
    } else {
      const mittelPattern = `%${normalized}%`;
      const mittelExprs = ["LOWER(m.kennr) LIKE ?"];
      if (mittelColumns.includes("name")) {
        mittelExprs.push("LOWER(m.name) LIKE ?");
      }
      if (mittelColumns.includes("mittelname")) {
        mittelExprs.push("LOWER(m.mittelname) LIKE ?");
      }
      if (mittelColumns.includes("mittel_name")) {
        mittelExprs.push("LOWER(m.mittel_name) LIKE ?");
      }
      if (mittelHasPayload) {
        mittelExprs.push(
          "LOWER(json_extract(m.payload_json, '$.mittelname')) LIKE ?"
        );
      }
      conditions.push(`(${mittelExprs.join(" OR ")})`);
      for (let i = 0; i < mittelExprs.length; i += 1) {
        bindings.push(mittelPattern);
      }
    }
  }

  if (!includeExpired) {
    if (rawZulEndeExpr) {
      conditions.push(
        `(${rawZulEndeExpr} IS NULL OR DATE(${rawZulEndeExpr}) >= DATE('now'))`
      );
    } else if (awgHasPayload) {
      const payloadExpiryCoalesce = clean(
        `COALESCE(
          json_extract(a.payload_json, '$.zulassungsende'),
          json_extract(a.payload_json, '$.gueltig_bis')
        )`
      );
      conditions.push(
        `(${payloadExpiryCoalesce} IS NULL OR DATE(${payloadExpiryCoalesce}) >= DATE('now'))`
      );
    }
  }

  if (conditions.length > 0) {
    sql += " WHERE " + conditions.join(" AND ");
  }

  sql += " ORDER BY name COLLATE NOCASE";

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
        status_payload: row[6],
        awg_payload: row[7],
        mittel_payload: row[8],
      });
    },
  });

  // Enrich each result with detailed information
  for (const result of results) {
    const mittelPayload = safeJsonParse(result.mittel_payload);
    const awgPayload = safeJsonParse(result.awg_payload);
    const statusPayload = safeJsonParse(result.status_payload);

    const resolvedName = pickFirstNonEmpty(
      result.name,
      mittelPayload?.mittelname,
      mittelPayload?.mittel_name,
      mittelPayload?.mittelName,
      mittelPayload?.name,
      result.kennr
    );
    result.name = resolvedName ? String(resolvedName) : String(result.kennr);

    const resolvedFormulierung = pickFirstNonEmpty(
      result.formulierung,
      mittelPayload?.formulierung,
      mittelPayload?.formulierung_art
    );
    result.formulierung =
      resolvedFormulierung === null || resolvedFormulierung === undefined
        ? null
        : String(resolvedFormulierung).trim() || null;

    const resolvedExpiry = pickFirstNonEmpty(
      result.zul_ende,
      awgPayload?.zulassungsende,
      awgPayload?.gueltig_bis,
      mittelPayload?.zul_ende,
      mittelPayload?.zulassungsende,
      mittelPayload?.gueltig_bis,
      mittelPayload?.gueltigBis
    );
    result.zul_ende =
      resolvedExpiry === null || resolvedExpiry === undefined
        ? null
        : String(resolvedExpiry);

    let geringesValue = toBooleanFlag(result.geringes_risiko);
    if (mittelPayload) {
      geringesValue = Math.max(
        geringesValue,
        toBooleanFlag(mittelPayload.mittel_mit_geringem_risiko)
      );
    }
    result.geringes_risiko = geringesValue === 1;

    let statusObject = statusPayload;
    if (!statusObject && typeof result.status_payload === "string") {
      const trimmedStatus = result.status_payload.trim();
      if (trimmedStatus) {
        statusObject = { status: trimmedStatus };
      }
    }

    if (!statusObject && awgPayload) {
      const statusText = pickFirstNonEmpty(
        awgPayload.status,
        awgPayload.status_text,
        awgPayload.status_bez,
        awgPayload.statustext
      );
      const gueltigBis = pickFirstNonEmpty(
        awgPayload.gueltig_bis,
        awgPayload.zulassungsende,
        awgPayload.gueltigBis
      );
      if (statusText || gueltigBis) {
        statusObject = {};
        if (statusText) {
          statusObject.status = statusText;
        }
        if (gueltigBis) {
          statusObject.gueltig_bis = gueltigBis;
        }
      }
    }

    result.status_json = statusObject ? JSON.stringify(statusObject) : null;

    delete result.status_payload;
    delete result.awg_payload;
    delete result.mittel_payload;

    // Get cultures
    result.kulturen = [];
    if (kulturConfig) {
      db.exec({
        sql: kulturConfig.sql,
        bind: [result.awg_id],
        callback: (row) => {
          result.kulturen.push({
            kultur: row[0],
            label: row[1],
            ausgenommen: toBooleanFlag(row[2]) === 1,
            sortier_nr: row[3],
          });
        },
      });
    }

    // Get schadorganismen
    result.schadorganismen = [];
    if (schadorgConfig) {
      db.exec({
        sql: schadorgConfig.sql,
        bind: [result.awg_id],
        callback: (row) => {
          result.schadorganismen.push({
            schadorg: row[0],
            label: row[1],
            ausgenommen: toBooleanFlag(row[2]) === 1,
            sortier_nr: row[3],
          });
        },
      });
    }

    // Get aufwände
    result.aufwaende = [];
    if (aufwandConfig) {
      db.exec({
        sql: aufwandConfig.sql,
        bind: [result.awg_id],
        callback: (row) => {
          result.aufwaende.push({
            aufwand_bedingung: row[0],
            sortier_nr: row[1],
            mittel_menge: row[2],
            mittel_menge_min: row[3],
            mittel_menge_max: row[4],
            mittel_einheit: row[5],
            wasser_menge: row[6],
            wasser_menge_min: row[7],
            wasser_menge_max: row[8],
            wasser_einheit: row[9],
            payload_json: row[10],
          });
        },
      });
    }

    // Get wartezeiten
    result.wartezeiten = [];
    if (wartezeitConfig) {
      db.exec({
        sql: wartezeitConfig.sql,
        bind: [result.awg_id],
        callback: (row) => {
          result.wartezeiten.push({
            kultur: row[0],
            kultur_label: row[1],
            sortier_nr: row[2],
            tage: row[3],
            bemerkung_kode: row[4],
            anwendungsbereich: row[5],
            erlaeuterung: row[6],
            payload_json: row[7],
          });
        },
      });
    }

    // Get wirkstoffe (if table exists)
    result.wirkstoffe = [];
    let wirkstoffeTableExists = false;
    db.exec({
      sql: "SELECT 1 FROM sqlite_master WHERE type='table' AND name='bvl_mittel_wirkstoffe'",
      callback: () => {
        wirkstoffeTableExists = true;
      },
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
            returnValue: "resultRows",
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
      callback: () => {
        gefahrTableExists = true;
      },
    });
    if (gefahrTableExists) {
      db.exec({
        sql: "SELECT * FROM bvl_mittel_gefahrhinweise WHERE kennr = ?",
        bind: [result.kennr],
        callback: (row) => {
          const gefahr = {};
          const colNames = db.exec({
            sql: "PRAGMA table_info(bvl_mittel_gefahrhinweise)",
            returnValue: "resultRows",
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
      callback: () => {
        vertriebTableExists = true;
      },
    });
    if (vertriebTableExists) {
      db.exec({
        sql: "SELECT * FROM bvl_mittel_vertrieb WHERE kennr = ?",
        bind: [result.kennr],
        callback: (row) => {
          const vert = {};
          const colNames = db.exec({
            sql: "PRAGMA table_info(bvl_mittel_vertrieb)",
            returnValue: "resultRows",
          });
          colNames.forEach((col, idx) => {
            vert[col[1]] = row[idx];
          });
          result.vertrieb.push(vert);
        },
      });
    }
  }

  return results;
}

async function listBvlMittel(payload) {
  if (!db) throw new Error("Database not initialized");

  let limit = Number(payload?.limit);
  if (Number.isNaN(limit) || limit <= 0) {
    limit = 500;
  }
  limit = Math.min(Math.max(Math.floor(limit), 1), 5000);

  const searchRaw = payload?.search ? String(payload.search).trim() : "";
  const hasSearch = searchRaw.length > 0;
  const searchNormalized = searchRaw.toLowerCase();

  const mittelColumns = getTableColumns("bvl_mittel");
  const hasPayload = mittelColumns.includes("payload_json");
  const hasNameColumn = mittelColumns.includes("name");
  const hasMittelnameColumn = mittelColumns.includes("mittelname");
  const hasMittelNameAltColumn = mittelColumns.includes("mittel_name");
  const hasFormColumn = mittelColumns.includes("formulierung");
  const hasFormArtColumn = mittelColumns.includes("formulierung_art");
  const hasZulEndeColumn = mittelColumns.includes("zul_ende");
  const hasZulassungsendeColumn = mittelColumns.includes("zulassungsende");

  const selectPieces = [
    "m.kennr",
    hasNameColumn
      ? "m.name AS name"
      : hasMittelnameColumn
        ? "m.mittelname AS name"
        : hasMittelNameAltColumn
          ? "m.mittel_name AS name"
          : "NULL AS name",
    hasFormColumn
      ? "m.formulierung AS formulierung"
      : hasFormArtColumn
        ? "m.formulierung_art AS formulierung"
        : "NULL AS formulierung",
    hasZulEndeColumn
      ? "m.zul_ende AS zul_ende"
      : hasZulassungsendeColumn
        ? "m.zulassungsende AS zul_ende"
        : "NULL AS zul_ende",
    hasPayload ? "m.payload_json AS payload_json" : "NULL AS payload_json",
  ];

  let sql = `
    SELECT ${selectPieces.join(", ")}
    FROM bvl_mittel m
  `;

  const bindings = [];

  if (hasSearch) {
    const pattern = `%${searchNormalized}%`;
    const searchExprs = ["LOWER(m.kennr) LIKE ?"];
    if (hasNameColumn) {
      searchExprs.push("LOWER(m.name) LIKE ?");
    }
    if (hasMittelnameColumn) {
      searchExprs.push("LOWER(m.mittelname) LIKE ?");
    }
    if (hasMittelNameAltColumn) {
      searchExprs.push("LOWER(m.mittel_name) LIKE ?");
    }
    if (hasPayload) {
      searchExprs.push(
        "LOWER(json_extract(m.payload_json, '$.mittelname')) LIKE ?"
      );
      searchExprs.push(
        "LOWER(json_extract(m.payload_json, '$.mittel_name')) LIKE ?"
      );
      searchExprs.push(
        "LOWER(json_extract(m.payload_json, '$.mittelName')) LIKE ?"
      );
    }

    sql += ` WHERE ${searchExprs.map((expr) => `(${expr})`).join(" OR ")}`;
    for (let i = 0; i < searchExprs.length; i += 1) {
      bindings.push(pattern);
    }
  }

  sql += " ORDER BY name COLLATE NOCASE";
  sql += ` LIMIT ${limit}`;

  const mittel = [];
  const execOptions = {
    sql,
    callback: (row) => {
      mittel.push({
        kennr: row[0],
        name: row[1],
        formulierung: row[2],
        zul_ende: row[3],
        payload_json: row[4],
      });
    },
  };

  if (bindings.length) {
    execOptions.bind = bindings;
  }

  db.exec(execOptions);

  for (const entry of mittel) {
    const payload = safeJsonParse(entry.payload_json);

    const resolvedName = pickFirstNonEmpty(
      entry.name,
      payload?.mittelname,
      payload?.mittel_name,
      payload?.mittelName,
      entry.kennr
    );
    entry.name = resolvedName ? String(resolvedName) : String(entry.kennr);

    const resolvedFormulierung = pickFirstNonEmpty(
      entry.formulierung,
      payload?.formulierung,
      payload?.formulierung_art
    );
    entry.formulierung =
      resolvedFormulierung === null || resolvedFormulierung === undefined
        ? null
        : String(resolvedFormulierung).trim() || null;

    const resolvedExpiry = pickFirstNonEmpty(
      entry.zul_ende,
      payload?.zul_ende,
      payload?.zulassungsende,
      payload?.gueltig_bis,
      payload?.gueltigBis
    );
    entry.zul_ende =
      resolvedExpiry === null || resolvedExpiry === undefined
        ? null
        : String(resolvedExpiry);

    delete entry.payload_json;
  }

  return mittel;
}

async function listBvlCultures(payload) {
  if (!db) throw new Error("Database not initialized");

  const withCount = payload?.withCount || false;
  const cultures = [];

  if (!hasTable("bvl_awg_kultur")) {
    return cultures;
  }

  const columns = getTableColumns("bvl_awg_kultur");
  const codeCol = pickColumn(
    columns,
    "kultur",
    "kultur_kode",
    "kulturcode",
    "kultur_code"
  );
  if (!codeCol) {
    return cultures;
  }
  const labelCol = pickColumn(
    columns,
    "label",
    "kultur_label",
    "kultur_text",
    "kultur_bez",
    "kulturname"
  );
  const excludeCol = pickColumn(
    columns,
    "ausgenommen",
    "ausnahme",
    "ausgeschl"
  );

  const lookupAvailable = hasTable("bvl_lookup_kultur");
  const codeExpr = `TRIM(ak.${codeCol})`;
  const labelBase = labelCol ? `TRIM(ak.${labelCol})` : codeExpr;
  const fallbackLabelExpr = `COALESCE(NULLIF(${labelBase}, ''), ${codeExpr})`;
  const labelExpr = lookupAvailable
    ? `COALESCE(NULLIF(TRIM(lk.label), ''), ${fallbackLabelExpr})`
    : fallbackLabelExpr;
  const whereClause = excludeCol
    ? `WHERE COALESCE(ak.${excludeCol}, 0) = 0`
    : "";
  const orderClause = "ORDER BY label COLLATE NOCASE";
  const joinClause = lookupAvailable
    ? `
    LEFT JOIN bvl_lookup_kultur lk
      ON TRIM(lk.code) = ${codeExpr}
  `
    : "";

  let sql;
  if (withCount) {
    sql = `
      SELECT ${codeExpr} AS code, ${labelExpr} AS label, COUNT(*) AS count
      FROM bvl_awg_kultur ak
      ${joinClause}
      ${whereClause}
      GROUP BY code, label
      ${orderClause}
    `;
  } else {
    sql = `
      SELECT DISTINCT ${codeExpr} AS code, ${labelExpr} AS label
      FROM bvl_awg_kultur ak
      ${joinClause}
      ${whereClause}
      ${orderClause}
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

  if (!hasTable("bvl_awg_schadorg")) {
    return schadorg;
  }

  const columns = getTableColumns("bvl_awg_schadorg");
  const codeCol = pickColumn(
    columns,
    "schadorg",
    "schadorg_kode",
    "schadorgcode",
    "schadorganismus"
  );
  if (!codeCol) {
    return schadorg;
  }
  const labelCol = pickColumn(
    columns,
    "label",
    "schadorg_label",
    "schadorg_text",
    "schadorganismus_text",
    "schadorg_bez"
  );
  const excludeCol = pickColumn(
    columns,
    "ausgenommen",
    "ausnahme",
    "ausgeschl"
  );

  const lookupAvailable = hasTable("bvl_lookup_schadorg");
  const codeExpr = `TRIM(aso.${codeCol})`;
  const labelBase = labelCol ? `TRIM(aso.${labelCol})` : codeExpr;
  const fallbackLabelExpr = `COALESCE(NULLIF(${labelBase}, ''), ${codeExpr})`;
  const labelExpr = lookupAvailable
    ? `COALESCE(NULLIF(TRIM(ls.label), ''), ${fallbackLabelExpr})`
    : fallbackLabelExpr;
  const whereClause = excludeCol
    ? `WHERE COALESCE(aso.${excludeCol}, 0) = 0`
    : "";
  const orderClause = "ORDER BY label COLLATE NOCASE";
  const joinClause = lookupAvailable
    ? `
    LEFT JOIN bvl_lookup_schadorg ls
      ON TRIM(ls.code) = ${codeExpr}
  `
    : "";

  let sql;
  if (withCount) {
    sql = `
      SELECT ${codeExpr} AS code, ${labelExpr} AS label, COUNT(*) AS count
      FROM bvl_awg_schadorg aso
      ${joinClause}
      ${whereClause}
      GROUP BY code, label
      ${orderClause}
    `;
  } else {
    sql = `
      SELECT DISTINCT ${codeExpr} AS code, ${labelExpr} AS label
      FROM bvl_awg_schadorg aso
      ${joinClause}
      ${whereClause}
      ${orderClause}
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
