# SQLite-WASM Migration Blueprint

This document is a detailed execution plan for introducing a full SQLite-WASM storage layer into the existing Pflanzenschutzliste web application. It is structured so that a coding agent can pick individual tasks, implement them, and validate the work step by step.

## 1. Zielsetzung & Rahmen
- Ersetze die bisherige JSON-basierte Persistenz durch eine SQLite-Datenbank im Browser, primär via OPFS (Origin Private File System).
- Halte den bisherigen UX-Flow (Datenbank erstellen/öffnen/speichern) aufrecht; erweitere ihn um einen SQLite-Export/Import.
- Stelle sicher, dass die App auch bei sehr großen Datenmengen performant bleibt (Lazy Loading, Worker, vorbereitete Statements).
- Bewahre Kompatibilität: Fallback auf bestehende JSON-Treiber, falls SQLite-WASM/OPFS nicht verfügbar ist.

## 2. Architekturüberblick
- **Neuer Storage-Treiber `sqlite`** (in `assets/js/core/storage/sqlite.js`).
- **Web Worker** (`assets/js/core/storage/sqliteWorker.js`) lädt und betreibt SQLite-WASM, kapselt alle DB-Operationen und verhindert UI-Blocking.
- **WASM Assets** (z. B. `assets/vendor/sqlite/sqlite3.wasm`, `sqlite3.js`, `sqlite3.wasm.wasmfs.data` falls benötigt) lokal bundlen.
- **Message Bridge** zwischen Hauptthread und Worker (Request/Response mit IDs, Fehlerbehandlung, Cancel-Handling).
- **Data Access Layer (DAL)** in Worker kapselt SQL, verwaltet Schema, Migrationen, Paging.
- **State Layer** im Hauptthread bleibt die Single Source of Truth für die UI, holt Daten via DAL/Worker und schreibt aktualisierte Slices wie bisher.

## 3. Schema & Migration
1. Definiere SQL-Schema in `assets/js/core/storage/schema.sql` (neue Datei):
   - `meta (key TEXT PRIMARY KEY, value TEXT)` für Settings wie Version, company JSON, defaults JSON, labels JSON.
   - `measurement_methods (id TEXT PRIMARY KEY, label TEXT, type TEXT, unit TEXT, requires TEXT, config TEXT)`.
   - `mediums (id TEXT PRIMARY KEY, name TEXT, unit TEXT, method_id TEXT, value REAL, FOREIGN KEY(method_id) REFERENCES measurement_methods(id))`.
   - `history (id INTEGER PRIMARY KEY AUTOINCREMENT, created_at TEXT, header_json TEXT)`.
   - `history_items (id INTEGER PRIMARY KEY AUTOINCREMENT, history_id INTEGER REFERENCES history(id) ON DELETE CASCADE, medium_id TEXT, payload_json TEXT)`.
   - Optional: `indices` auf `history(created_at DESC)` und `history_items(history_id)`.
2. Implementiere Migration-Manager im Worker (z. B. `applyMigrations(db)`):
   - Nutze `PRAGMA user_version` zur Schema-Versionierung.
   - Migration 1: Erstelle o. g. Tabellen + Defaultdaten.
   - Migration 2+: Reserviert für zukünftige Anpassungen.
3. JSON → SQLite Import:
   - Parse `defaults.json` beim Erststart (wenn DB leer) und schreibe in Tabellen via Transaktion.
   - Für User-Import (bestehende JSON-Datei) ähnlicher Pfad.
4. SQLite → JSON Export:
   - Baue Funktion, die alle Tabellen in das existierende `getDatabaseSnapshot()`-Format zurückführt.

## 4. Worker-Implementierung
- **Dateien:**
  - `assets/js/core/storage/sqliteWorker.js` (Worker Script, per ES Module laden).
  - Optional Hilfsdateien (`dal.js`, `migrations.js`, `queries.js`).
- **Initialisierung:**
  - `sqlite3InitModule` laden (`importScripts` oder `await import` in Worker context).
  - `const sqlite3 = await sqlite3InitModule({ locateFile: file => 'assets/vendor/sqlite/' + file });`
  - OPFS-Handle öffnen (`navigator.storage.getDirectory()`), DB-Datei `pflanzenschutz.sqlite` erzeugen, `sqlite3.oo1.DB` mit `{ filename: 'file:pflanzenschutz-prod?mode=rw', vfs: 'opfs', create: true }` initialisieren.
  - Konfiguriere `PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA temp_store=MEMORY; PRAGMA cache_size=-20000;`.
- **Message Handling:**
  - Request-Form: `{ id, action, payload }`.
  - Response-Form: `{ id, ok, result }` oder `{ id, ok: false, error }`.
  - Aktionen: `init`, `importSnapshot`, `exportSnapshot`, `upsertMedium`, `deleteMedium`, `listMediums`, `listHistory`, `getHistoryEntry`, `appendHistoryEntry`, `deleteHistoryEntry`, usw.
- **Prepared Statements & Transaktionen:**
  - Halte Statement-Pools für häufige Queries (z. B. Medium CRUD, History Paging).
  - Nutze Transaktionen für Batch-Operationen und Imports.

## 5. Hauptthread-Treiber (`sqlite.js`)
- API kompatibel zu `fileSystem`/`fallback` Treibern: `isSupported`, `create`, `open`, `save`, `getContext`, `reset`.
- Aufgaben:
  1. Worker instanziieren (`new Worker(new URL('./sqliteWorker.js', import.meta.url), { type: 'module' })`).
  2. Init-Sequenz: `await worker.call('init', { mode: detectMode() })`.
  3. `create(initialData)`: DB neu erstellen, Schema anwenden, Snapshot importieren, optional FileHandle sichern.
  4. `open()`: Existierende SQLite-Datei (via File Picker) lesen, Worker-DB aus `ArrayBuffer` initialisieren (`DB.createFrom`) oder OPFS-File mit Hochladen ersetzen.
  5. `save(data)`: Aktualisierte Snapshots per `importSnapshot` (UPSERT) einspielen.
  6. `exportFile()`: Worker `exportSnapshot`-Result in eine Datei schreiben (`db.export()` → `Uint8Array`).
  7. `destroy()`: Worker terminieren, Ressourcen freigeben.
- Ergänze `storage/index.js`:
  - `DRIVERS.sqlite = sqliteDriver`.
  - Passe `detectPreferredDriver` an (`if (sqliteDriver.isSupported()) return 'sqlite';`).
  - Erweitere State `app.storageDriver` um Wert `'sqlite'`.

## 6. UI & State Anpassungen
- **Startup Feature** (`assets/js/features/startup/index.js`):
  - Neue Buttons für „SQLite-Datenbank erstellen“ / „SQLite-Datei verbinden“.
  - Fallback-Hinweise, wenn Browser kein WASM/OPFS kann.
  - Wizards aktualisieren, um `.sqlite` statt `.json` vorzuschlagen.
- **Settings Feature** (`assets/js/features/settings/index.js`):
  - CRUD-Aktionen via Worker-Calls implementieren (nicht mehr nur State-Arrays).
  - Nach erfolgreicher Operation: UI-State synchronisieren (`services.state.updateSlice` mit Worker-Daten).
- **History Feature** (`assets/js/features/history/index.js`):
  - Paging/Lazy Loading bei großen Datenmengen (`loadHistoryPage(pageSize, cursor)` via Worker).
  - Detail-Ansicht lädt Items on-demand (`getHistoryEntry(id)`).
- **Calculation Feature** (`assets/js/features/calculation/index.js`):
  - `save`-Button schreibt History-Eintrag über Worker, erhält ID zurück, aktualisiert UI-State minimal.
  - Optionale Optimierung: Medium/Method-Lookups direkt aus SQLite (z. B. Worker `listMediums` bei Start).
- **State Layer (`state.js`)**: State bleibt primär als UI-Cache; achte darauf, keine vollständigen Kopien großer Tabellen mehr zu halten (z. B. History nur aktuelle Seite).
- **Labels/Config**: Persistiere Label-Änderungen in SQLite (`meta`-Tabelle). Beim Start Snapshot ziehen und State initialisieren.

## 7. JSON-Kompatibilität
- Biete weiterhin Import/Export von JSON-Snapshots als Brücke:
  - Worker-Funktion `exportJsonSnapshot` generiert JSON aus DB.
  - `createInitialDatabase` nutzt Worker statt `defaults.json` direkt.
  - Datei-Uploads (`.json`) importieren via Worker-Transaktion.

## 8. Tests & Qualitätssicherung
- Implementiere automatisierte Browser-Tests (Playwright) oder Integrationstests, die Worker-Funktionalität validieren (z. B. Import, CRUD, Export).
- Führe Lasttests lokal aus: generiere mehrere tausend History-Einträge, prüfe Ladezeiten & Speicherverbrauch.
- Prüfe Browser-Kompatibilität: Chrome/Edge (OPFS), Firefox (Fallback, ggf. Behind-Flag Hinweis), Safari (WASM Support, kein OPFS → fallback auf JSON).
- Validierung nach jedem Schritt (`console`-Warnungen, Fehler-Handling).
- Dokumentiere alle neuen NPM/Build-Schritte (falls bundler nötig wird, z. B. Rollup/Vite für Worker + WASM). Aktuell ES Module → Worker via `new URL`.

## 9. Schritt-für-Schritt-Umsetzung (empfohlene Reihenfolge)
1. **Vorbereitung**
   - SQLite-WASM Assets hinzufügen.
   - Worker Grundgerüst + Init-Test (simple Query) erstellen.
2. **Schema & Migrationen**
   - `schema.sql`, Migration-Manager, Import `defaults.json`.
3. **Worker-DAL API**
   - CRUD-Methoden implementieren (Mediums, MeasurementMethods, History Paging, Meta).
4. **Hauptthread-Treiber**
   - `sqlite.js` implementieren, an `storage/index.js` anbinden.
5. **Startup-Flow**
   - UI aktualisieren, SQLite-Optionen integrieren, Fallback-Hinweise.
6. **Settings/Calculation/History anpassen**
   - UI-Events → Worker Calls, State-Sync.
   - Paging & Lazy Loading für History.
7. **Import/Export**
   - JSON + SQLite Export/Import implementieren.
   - Download/Upload via File Picker.
8. **Fallback & Feature Detection**
   - `isSupported` Checks, fallback to JSON driver, UI informiert Nutzer.
9. **Optimierung & Tests**
   - Performance-Tuning (Pragma, Statement-Reuse).
   - Integrationstests, große Testdatensätze, Regressionen prüfen.
10. **Dokumentation**
    - README aktualisieren (neue Features, Browser-Anforderungen, bekannte Einschränkungen).

## 10. Offene Fragen (für Product Owner / Stakeholder)
- Welche Mindest-Browser sollen unterstützt werden? (OPFS/SharedArrayBuffer).
- Soll History dauerhaft im UI-State cachen oder nur aktuelle Seite? (Memory-Verbrauch).
- Benötigt der Export wahlweise JSON und SQLite parallel? (für Datenaustausch mit bestehenden Nutzern).
- Ist eine Verschlüsselung / Passwortschutz vorgesehen? (SQLite unterstützt, aber zusätzliche Arbeit).

## 11. Erfolgskriterien
- SQLite-WASM läuft stabil in Chromium-basierten Browsern mit OPFS, inklusive Persistenz über Reloads.
- App reagiert flüssig bei >10k History-Einträgen (Paging, on-demand Laden).
- Import/Export (JSON & SQLite) funktioniert ohne Datenverlust.
- Fallback auf JSON-Storage funktioniert, wenn SQLite-WASM nicht verfügbar.
- Tests decken neue Persistenzlogik ab; keine Regressionen in bestehender UI.

---

Mit diesem Plan kann ein Coding Agent die Migration schrittweise durchführen und nach jedem Milestone testen. Änderungen sollten in kleinen, reviewbaren Pull Requests umgesetzt werden (z. B. pro Abschnitt aus Kapitel 9).git 