/**
 * SQLite Web Worker
 * Handles all database operations in a separate thread to prevent UI blocking
 */

let sqlite3 = null;
let db = null;
let isInitialized = false;
let currentMode = 'memory';

// SQLite-WASM CDN URL
const SQLITE_WASM_CDN = 'https://cdn.jsdelivr.net/npm/@sqlite.org/sqlite-wasm@3.46.1-build1/sqlite-wasm/jswasm/';

// Message handler
self.onmessage = async function(event) {
  const { id, action, payload } = event.data;
  
  try {
    let result;
    
    switch (action) {
      case 'init':
        result = await initDatabase(payload);
        break;
      case 'importSnapshot':
        result = await importSnapshot(payload);
        break;
      case 'exportSnapshot':
        result = await exportSnapshot();
        break;
      case 'upsertMedium':
        result = await upsertMedium(payload);
        break;
      case 'deleteMedium':
        result = await deleteMedium(payload);
        break;
      case 'listMediums':
        result = await listMediums();
        break;
      case 'listHistory':
        result = await listHistory(payload);
        break;
      case 'getHistoryEntry':
        result = await getHistoryEntry(payload);
        break;
      case 'appendHistoryEntry':
        result = await appendHistoryEntry(payload);
        break;
      case 'deleteHistoryEntry':
        result = await deleteHistoryEntry(payload);
        break;
      case 'exportDB':
        result = await exportDB();
        break;
      case 'importDB':
        result = await importDB(payload);
        break;
      case 'importBvlDataset':
        result = await importBvlDataset(payload);
        break;
      case 'getBvlMeta':
        result = await getBvlMeta(payload);
        break;
      case 'setBvlMeta':
        result = await setBvlMeta(payload);
        break;
      case 'appendBvlSyncLog':
        result = await appendBvlSyncLog(payload);
        break;
      case 'queryZulassung':
        result = await queryZulassung(payload);
        break;
      case 'listBvlCultures':
        result = await listBvlCultures();
        break;
      case 'listBvlSchadorg':
        result = await listBvlSchadorg();
        break;
      default:
        throw new Error(`Unknown action: ${action}`);
    }
    
    self.postMessage({ id, ok: true, result });
  } catch (error) {
    self.postMessage({ 
      id, 
      ok: false, 
      error: error.message || String(error)
    });
  }
};

/**
 * Initialize SQLite database
 */
async function initDatabase(options = {}) {
  if (isInitialized) {
    return { success: true, message: 'Already initialized' };
  }
  
  try {
    // Load SQLite WASM module
    const sqlite3InitModule = await import(SQLITE_WASM_CDN + 'sqlite3.mjs').then(m => m.default);
    
    sqlite3 = await sqlite3InitModule({
      print: console.log,
      printErr: console.error,
      locateFile: (file) => SQLITE_WASM_CDN + file
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
      message: `Database initialized in ${mode} mode` 
    };
  } catch (error) {
    console.error('Failed to initialize database:', error);
    throw new Error(`Database initialization failed: ${error.message}`);
  }
}

/**
 * Detect best storage mode
 */
function detectMode() {
  if (typeof sqlite3?.opfs !== 'undefined') {
    return 'opfs';
  }
  return 'memory';
}

function createDatabaseInstance(mode = 'memory') {
  if (mode === 'opfs' && sqlite3?.opfs) {
    return new sqlite3.oo1.OpfsDb('/pflanzenschutz.sqlite');
  }
  return new sqlite3.oo1.DB();
}

function configureDatabase(targetDb = db) {
  if (!targetDb) {
    throw new Error('Database not initialized');
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
  if (!db) throw new Error('Database not initialized');
  
  // Check current version
  const currentVersion = db.selectValue('PRAGMA user_version') || 0;
  
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
    // Drop any existing bvl_* tables from incomplete migrations
    db.exec(`
      DROP TABLE IF EXISTS bvl_sync_log;
      DROP TABLE IF EXISTS bvl_awg_wartezeit;
      DROP TABLE IF EXISTS bvl_awg_aufwand;
      DROP TABLE IF EXISTS bvl_awg_schadorg;
      DROP TABLE IF EXISTS bvl_awg_kultur;
      DROP TABLE IF EXISTS bvl_awg;
      DROP TABLE IF EXISTS bvl_mittel;
      DROP TABLE IF EXISTS bvl_meta;
    `);
    
    // Create BVL schema
    const bvlSchemaSql = `
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
      
      CREATE INDEX idx_awg_kennr ON bvl_awg(kennr);
      CREATE INDEX idx_awg_kultur_kultur ON bvl_awg_kultur(kultur);
      CREATE INDEX idx_awg_schadorg_schadorg ON bvl_awg_schadorg(schadorg);
      CREATE INDEX idx_awg_aufwand_awg ON bvl_awg_aufwand(awg_id);
      CREATE INDEX idx_awg_wartezeit_awg ON bvl_awg_wartezeit(awg_id);
      
      PRAGMA user_version = 2;
    `;
    
    db.exec(bvlSchemaSql);
  }
}

/**
 * Import a complete snapshot from JSON format
 */
async function importSnapshot(snapshot) {
  if (!db) throw new Error('Database not initialized');
  
  db.exec('BEGIN TRANSACTION');
  
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
        measurementMethods: JSON.stringify(snapshot.meta.measurementMethods || [])
      };
      
      const stmt = db.prepare('INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)');
      for (const [key, value] of Object.entries(metaEntries)) {
        stmt.bind([key, typeof value === 'string' ? value : JSON.stringify(value)]).step();
        stmt.reset();
      }
      stmt.finalize();
      
      // Import measurement methods
      if (snapshot.meta.measurementMethods && Array.isArray(snapshot.meta.measurementMethods)) {
        const methodStmt = db.prepare(
          'INSERT OR REPLACE INTO measurement_methods (id, label, type, unit, requires, config) VALUES (?, ?, ?, ?, ?, ?)'
        );
        for (const method of snapshot.meta.measurementMethods) {
          methodStmt.bind([
            method.id,
            method.label,
            method.type,
            method.unit,
            JSON.stringify(method.requires || []),
            JSON.stringify(method.config || {})
          ]).step();
          methodStmt.reset();
        }
        methodStmt.finalize();
      }
    }
    
    // Import mediums
    if (snapshot.mediums && Array.isArray(snapshot.mediums)) {
      const mediumStmt = db.prepare(
        'INSERT OR REPLACE INTO mediums (id, name, unit, method_id, value) VALUES (?, ?, ?, ?, ?)'
      );
      for (const medium of snapshot.mediums) {
        mediumStmt.bind([
          medium.id,
          medium.name,
          medium.unit,
          medium.methodId || medium.method_id,
          medium.value
        ]).step();
        mediumStmt.reset();
      }
      mediumStmt.finalize();
    }
    
    // Import history
    if (snapshot.history && Array.isArray(snapshot.history)) {
      const historyStmt = db.prepare(
        'INSERT INTO history (created_at, header_json) VALUES (?, ?)'
      );
      const itemsStmt = db.prepare(
        'INSERT INTO history_items (history_id, medium_id, payload_json) VALUES (?, ?, ?)'
      );

      for (const entry of snapshot.history) {
        const header = entry.header ? { ...entry.header } : { ...entry };
        delete header.items;
        const createdAt = entry.savedAt
          || header.savedAt
          || header.createdAt
          || new Date().toISOString();
        if (!header.createdAt) {
          header.createdAt = createdAt;
        }

        historyStmt.bind([createdAt, JSON.stringify(header)]).step();
        const historyId = db.selectValue('SELECT last_insert_rowid()');
        historyStmt.reset();

        const items = entry.items && Array.isArray(entry.items) ? entry.items : [];
        for (const item of items) {
          itemsStmt.bind([
            historyId,
            item.mediumId || item.medium_id || '',
            JSON.stringify(item)
          ]).step();
          itemsStmt.reset();
        }
      }

      historyStmt.finalize();
      itemsStmt.finalize();
    }
    
    db.exec('COMMIT');
    return { success: true, message: 'Snapshot imported successfully' };
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}

/**
 * Export complete database as JSON snapshot
 */
async function exportSnapshot() {
  if (!db) throw new Error('Database not initialized');
  
  const snapshot = {
    meta: {
      version: 1,
      company: {},
      defaults: {},
      fieldLabels: {},
      measurementMethods: []
    },
    mediums: [],
    history: []
  };
  
  // Export meta
  db.exec({
    sql: 'SELECT key, value FROM meta',
    callback: (row) => {
      const key = row[0];
      const value = row[1];
      try {
        const parsed = JSON.parse(value);
        if (key === 'company') snapshot.meta.company = parsed;
        else if (key === 'defaults') snapshot.meta.defaults = parsed;
        else if (key === 'fieldLabels') snapshot.meta.fieldLabels = parsed;
        else if (key === 'version') snapshot.meta.version = parsed;
      } catch (e) {
        console.warn(`Failed to parse meta key ${key}:`, e);
      }
    }
  });
  
  // Export measurement methods
  db.exec({
    sql: 'SELECT id, label, type, unit, requires, config FROM measurement_methods',
    callback: (row) => {
      snapshot.meta.measurementMethods.push({
        id: row[0],
        label: row[1],
        type: row[2],
        unit: row[3],
        requires: JSON.parse(row[4] || '[]'),
        config: JSON.parse(row[5] || '{}')
      });
    }
  });
  
  // Export mediums
  db.exec({
    sql: 'SELECT id, name, unit, method_id, value FROM mediums',
    callback: (row) => {
      snapshot.mediums.push({
        id: row[0],
        name: row[1],
        unit: row[2],
        methodId: row[3],
        value: row[4]
      });
    }
  });
  
  // Export history
  const historyMap = new Map();
  db.exec({
    sql: 'SELECT id, created_at, header_json FROM history ORDER BY created_at DESC',
    callback: (row) => {
      historyMap.set(row[0], {
        header: JSON.parse(row[2] || '{}'),
        items: []
      });
    }
  });
  
  // Export history items
  db.exec({
    sql: 'SELECT history_id, medium_id, payload_json FROM history_items',
    callback: (row) => {
      const historyId = row[0];
      if (historyMap.has(historyId)) {
        historyMap.get(historyId).items.push(JSON.parse(row[2]));
      }
    }
  });
  
  snapshot.history = Array.from(historyMap.values()).map(entry => ({
    ...entry.header,
    items: entry.items
  }));
  
  return snapshot;
}

/**
 * CRUD operations for mediums
 */
async function upsertMedium(medium) {
  if (!db) throw new Error('Database not initialized');
  
  const stmt = db.prepare(
    'INSERT OR REPLACE INTO mediums (id, name, unit, method_id, value) VALUES (?, ?, ?, ?, ?)'
  );
  stmt.bind([
    medium.id,
    medium.name,
    medium.unit,
    medium.methodId || medium.method_id,
    medium.value
  ]).step();
  stmt.finalize();
  
  return { success: true, id: medium.id };
}

async function deleteMedium(id) {
  if (!db) throw new Error('Database not initialized');
  
  const stmt = db.prepare('DELETE FROM mediums WHERE id = ?');
  stmt.bind([id]).step();
  stmt.finalize();
  
  return { success: true };
}

async function listMediums() {
  if (!db) throw new Error('Database not initialized');
  
  const mediums = [];
  db.exec({
    sql: 'SELECT id, name, unit, method_id, value FROM mediums',
    callback: (row) => {
      mediums.push({
        id: row[0],
        name: row[1],
        unit: row[2],
        methodId: row[3],
        value: row[4]
      });
    }
  });
  
  return mediums;
}

/**
 * History operations with paging
 */
async function listHistory({ page = 1, pageSize = 50 } = {}) {
  if (!db) throw new Error('Database not initialized');
  
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
      const header = JSON.parse(row[2] || '{}');
      history.push({
        id: row[0],
        ...header
      });
    }
  });
  
  const totalCount = db.selectValue('SELECT COUNT(*) FROM history') || 0;
  
  return {
    items: history,
    page,
    pageSize,
    totalCount,
    totalPages: Math.ceil(totalCount / pageSize)
  };
}

async function getHistoryEntry(id) {
  if (!db) throw new Error('Database not initialized');
  
  let entry = null;
  
  db.exec({
    sql: 'SELECT id, created_at, header_json FROM history WHERE id = ?',
    bind: [id],
    callback: (row) => {
      const header = JSON.parse(row[2] || '{}');
      entry = {
        id: row[0],
        ...header,
        items: []
      };
    }
  });
  
  if (!entry) {
    throw new Error('History entry not found');
  }
  
  db.exec({
    sql: 'SELECT medium_id, payload_json FROM history_items WHERE history_id = ?',
    bind: [id],
    callback: (row) => {
      entry.items.push(JSON.parse(row[1]));
    }
  });
  
  return entry;
}

async function appendHistoryEntry(entry) {
  if (!db) throw new Error('Database not initialized');
  
  db.exec('BEGIN TRANSACTION');
  
  try {
    const header = entry.header ? { ...entry.header } : { ...entry };
    delete header.items;
    const createdAt = entry.savedAt
      || header.savedAt
      || header.createdAt
      || new Date().toISOString();
    if (!header.createdAt) {
      header.createdAt = createdAt;
    }

    const stmt = db.prepare('INSERT INTO history (created_at, header_json) VALUES (?, ?)');
    stmt.bind([createdAt, JSON.stringify(header)]).step();
    const historyId = db.selectValue('SELECT last_insert_rowid()');
    stmt.finalize();
    
    const items = entry.items && Array.isArray(entry.items) ? entry.items : [];
    if (items.length) {
      const itemStmt = db.prepare(
        'INSERT INTO history_items (history_id, medium_id, payload_json) VALUES (?, ?, ?)'
      );
      for (const item of items) {
        itemStmt.bind([
          historyId,
          item.mediumId || item.medium_id || '',
          JSON.stringify(item)
        ]).step();
        itemStmt.reset();
      }
      itemStmt.finalize();
    }
    
    db.exec('COMMIT');
    return { success: true, id: historyId };
  } catch (error) {
    db.exec('ROLLBACK');
    throw error;
  }
}

async function deleteHistoryEntry(id) {
  if (!db) throw new Error('Database not initialized');
  
  // CASCADE will handle history_items deletion
  const stmt = db.prepare('DELETE FROM history WHERE id = ?');
  stmt.bind([id]).step();
  stmt.finalize();
  
  return { success: true };
}

/**
 * Export database as binary SQLite file
 */
async function exportDB() {
  if (!db) throw new Error('Database not initialized');
  
  const exported = sqlite3.capi.sqlite3_js_db_export(db.pointer);
  return { data: Array.from(exported) };
}

/**
 * Import database from binary SQLite file
 */
async function importDB(data) {
  if (!db) throw new Error('Database not initialized');
  const bytes = data instanceof Uint8Array ? data : new Uint8Array(data);

  if (currentMode === 'opfs' && sqlite3?.oo1?.OpfsDb && sqlite3?.opfs) {
    // Import directly into OPFS-backed database
    db.close();
    await sqlite3.oo1.OpfsDb.importDb('/pflanzenschutz.sqlite', bytes);
  db = createDatabaseInstance('opfs');
    configureDatabase();
    currentMode = 'opfs';
    isInitialized = true;
    return { success: true, mode: 'opfs' };
  }

  // In-memory fallback using sqlite3_deserialize
  db.close();
  const newDb = new sqlite3.oo1.DB();
  const scope = sqlite3.wasm.scopedAllocPush();
  try {
    const pData = sqlite3.wasm.allocFromTypedArray(bytes);
    const pSchema = sqlite3.wasm.allocCString('main');
    const flags = (sqlite3.capi.SQLITE_DESERIALIZE_FREEONCLOSE || 0)
      | (sqlite3.capi.SQLITE_DESERIALIZE_RESIZEABLE || 0);
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
      throw new Error(`sqlite3_deserialize failed: ${sqlite3.capi.sqlite3_js_rc_str(rc) || rc}`);
    }
  } finally {
    sqlite3.wasm.scopedAllocPop(scope);
  }

  db = newDb;
  configureDatabase();
  currentMode = 'memory';
  isInitialized = true;
  return { success: true, mode: 'memory' };
}

/**
 * Import BVL dataset into database
 * Replaces all BVL data with new dataset in a transaction
 */
async function importBvlDataset(dataset) {
  if (!db) throw new Error('Database not initialized');
  
  db.exec('BEGIN TRANSACTION');
  
  try {
    // Clear existing BVL data
    db.exec(`
      DELETE FROM bvl_awg_wartezeit;
      DELETE FROM bvl_awg_aufwand;
      DELETE FROM bvl_awg_schadorg;
      DELETE FROM bvl_awg_kultur;
      DELETE FROM bvl_awg;
      DELETE FROM bvl_mittel;
    `);
    
    // Insert mittel
    if (dataset.mittel && dataset.mittel.length > 0) {
      const stmt = db.prepare(`
        INSERT INTO bvl_mittel (kennr, name, formulierung, zul_erstmalig, zul_ende, geringes_risiko, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?)
      `);
      for (const item of dataset.mittel) {
        stmt.bind([
          item.kennr,
          item.name,
          item.formulierung,
          item.zul_erstmalig,
          item.zul_ende,
          item.geringes_risiko,
          item.payload_json
        ]).step();
        stmt.reset();
      }
      stmt.finalize();
    }
    
    // Insert awg
    if (dataset.awg && dataset.awg.length > 0) {
      const stmt = db.prepare(`
        INSERT INTO bvl_awg (awg_id, kennr, status_json, zulassungsende)
        VALUES (?, ?, ?, ?)
      `);
      for (const item of dataset.awg) {
        stmt.bind([
          item.awg_id,
          item.kennr,
          item.status_json,
          item.zulassungsende
        ]).step();
        stmt.reset();
      }
      stmt.finalize();
    }
    
    // Insert awg_kultur
    if (dataset.awg_kultur && dataset.awg_kultur.length > 0) {
      const stmt = db.prepare(`
        INSERT INTO bvl_awg_kultur (awg_id, kultur, ausgenommen, sortier_nr)
        VALUES (?, ?, ?, ?)
      `);
      for (const item of dataset.awg_kultur) {
        stmt.bind([
          item.awg_id,
          item.kultur,
          item.ausgenommen,
          item.sortier_nr
        ]).step();
        stmt.reset();
      }
      stmt.finalize();
    }
    
    // Insert awg_schadorg
    if (dataset.awg_schadorg && dataset.awg_schadorg.length > 0) {
      const stmt = db.prepare(`
        INSERT INTO bvl_awg_schadorg (awg_id, schadorg, ausgenommen, sortier_nr)
        VALUES (?, ?, ?, ?)
      `);
      for (const item of dataset.awg_schadorg) {
        stmt.bind([
          item.awg_id,
          item.schadorg,
          item.ausgenommen,
          item.sortier_nr
        ]).step();
        stmt.reset();
      }
      stmt.finalize();
    }
    
    // Insert awg_aufwand
    if (dataset.awg_aufwand && dataset.awg_aufwand.length > 0) {
      const stmt = db.prepare(`
        INSERT INTO bvl_awg_aufwand (awg_id, aufwand_bedingung, sortier_nr, mittel_menge, mittel_einheit, wasser_menge, wasser_einheit, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `);
      for (const item of dataset.awg_aufwand) {
        stmt.bind([
          item.awg_id,
          item.aufwand_bedingung,
          item.sortier_nr,
          item.mittel_menge,
          item.mittel_einheit,
          item.wasser_menge,
          item.wasser_einheit,
          item.payload_json
        ]).step();
        stmt.reset();
      }
      stmt.finalize();
    }
    
    // Insert awg_wartezeit
    if (dataset.awg_wartezeit && dataset.awg_wartezeit.length > 0) {
      const stmt = db.prepare(`
        INSERT INTO bvl_awg_wartezeit (awg_wartezeit_nr, awg_id, kultur, sortier_nr, tage, bemerkung_kode, anwendungsbereich, erlaeuterung, payload_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `);
      for (const item of dataset.awg_wartezeit) {
        stmt.bind([
          item.awg_wartezeit_nr,
          item.awg_id,
          item.kultur,
          item.sortier_nr,
          item.tage,
          item.bemerkung_kode,
          item.anwendungsbereich,
          item.erlaeuterung,
          item.payload_json
        ]).step();
        stmt.reset();
      }
      stmt.finalize();
    }
    
    db.exec('COMMIT');
    return { success: true };
  } catch (err) {
    db.exec('ROLLBACK');
    throw err;
  }
}

/**
 * Get BVL metadata value
 */
async function getBvlMeta(key) {
  if (!db) throw new Error('Database not initialized');
  
  const stmt = db.prepare('SELECT value FROM bvl_meta WHERE key = ?');
  stmt.bind([key]);
  
  let value = null;
  if (stmt.step()) {
    value = stmt.get(0);
  }
  
  stmt.finalize();
  return value;
}

/**
 * Set BVL metadata value
 */
async function setBvlMeta(payload) {
  if (!db) throw new Error('Database not initialized');
  
  const { key, value } = payload;
  
  const stmt = db.prepare('INSERT OR REPLACE INTO bvl_meta (key, value) VALUES (?, ?)');
  stmt.bind([key, value]).step();
  stmt.finalize();
  
  return { success: true };
}

/**
 * Append entry to BVL sync log
 */
async function appendBvlSyncLog(entry) {
  if (!db) throw new Error('Database not initialized');
  
  const stmt = db.prepare(`
    INSERT INTO bvl_sync_log (synced_at, ok, message, payload_hash)
    VALUES (?, ?, ?, ?)
  `);
  
  stmt.bind([
    entry.synced_at,
    entry.ok,
    entry.message,
    entry.payload_hash
  ]).step();
  
  stmt.finalize();
  return { success: true };
}

/**
 * Query Zulassung data with filters
 */
async function queryZulassung(params) {
  if (!db) throw new Error('Database not initialized');
  
  const { culture, pest, text, includeExpired } = params;
  
  let sql = `
    SELECT DISTINCT
      m.kennr,
      m.name,
      m.formulierung,
      m.zul_ende,
      a.awg_id,
      a.status_json,
      a.zulassungsende
    FROM bvl_mittel m
    INNER JOIN bvl_awg a ON a.kennr = m.kennr
  `;
  
  const conditions = [];
  const bindings = [];
  
  if (culture) {
    sql += `
      INNER JOIN bvl_awg_kultur k ON k.awg_id = a.awg_id
    `;
    conditions.push('k.kultur = ?');
    bindings.push(culture);
  }
  
  if (pest) {
    sql += `
      INNER JOIN bvl_awg_schadorg s ON s.awg_id = a.awg_id
    `;
    conditions.push('s.schadorg = ?');
    bindings.push(pest);
  }
  
  if (!includeExpired) {
    conditions.push('(a.zulassungsende IS NULL OR a.zulassungsende >= date("now"))');
  }
  
  if (text) {
    conditions.push('(m.name LIKE ? OR m.kennr LIKE ?)');
    const searchText = `%${text}%`;
    bindings.push(searchText, searchText);
  }
  
  if (conditions.length > 0) {
    sql += ' WHERE ' + conditions.join(' AND ');
  }
  
  sql += ' ORDER BY m.name LIMIT 100';
  
  const stmt = db.prepare(sql);
  stmt.bind(bindings);
  
  const results = [];
  while (stmt.step()) {
    const row = stmt.get({});
    
    // Get aufwand for this awg_id
    const aufwandStmt = db.prepare(`
      SELECT * FROM bvl_awg_aufwand WHERE awg_id = ? ORDER BY sortier_nr
    `);
    aufwandStmt.bind([row.awg_id]);
    const aufwand = [];
    while (aufwandStmt.step()) {
      aufwand.push(aufwandStmt.get({}));
    }
    aufwandStmt.finalize();
    
    // Get wartezeit for this awg_id
    const wartezeitStmt = db.prepare(`
      SELECT * FROM bvl_awg_wartezeit WHERE awg_id = ? ORDER BY sortier_nr
    `);
    wartezeitStmt.bind([row.awg_id]);
    const wartezeit = [];
    while (wartezeitStmt.step()) {
      wartezeit.push(wartezeitStmt.get({}));
    }
    wartezeitStmt.finalize();
    
    results.push({
      ...row,
      aufwand,
      wartezeit
    });
  }
  
  stmt.finalize();
  return results;
}

/**
 * List all BVL cultures (distinct, not ausgenommen)
 */
async function listBvlCultures() {
  if (!db) throw new Error('Database not initialized');
  
  const stmt = db.prepare(`
    SELECT DISTINCT kultur FROM bvl_awg_kultur
    WHERE ausgenommen = 0
    ORDER BY kultur
  `);
  
  const cultures = [];
  while (stmt.step()) {
    cultures.push(stmt.get(0));
  }
  
  stmt.finalize();
  return cultures;
}

/**
 * List all BVL Schadorganismen (distinct, not ausgenommen)
 */
async function listBvlSchadorg() {
  if (!db) throw new Error('Database not initialized');
  
  const stmt = db.prepare(`
    SELECT DISTINCT schadorg FROM bvl_awg_schadorg
    WHERE ausgenommen = 0
    ORDER BY schadorg
  `);
  
  const pests = [];
  while (stmt.step()) {
    pests.push(stmt.get(0));
  }
  
  stmt.finalize();
  return pests;
}
