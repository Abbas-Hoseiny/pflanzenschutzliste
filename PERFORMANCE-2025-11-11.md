# Performance-Audit (Stand: 11.11.2025)

## Executive Summary

- Der aktuelle Astro-Build liefert nur ~14 KB HTML und ~8 KB CSS aus, bettet aber die kompletten TypeScript-Quellen als `data:`-URI ein. Dadurch wird kein echtes Bundling durchgeführt, Caching ist ausgeschlossen und der Browser erhält syntaktisch ungültigen Code.
- Alle Feature-Module (Berechnung, Historie, Zulassung etc.) werden synchron initialisiert. Der SQLite-WASM-Treiber (1–1,5 MB über CDN) und BVL-Sync-Logik blockieren den kritischen Pfad sowie die Laufzeit.
- Das State-System benachrichtigt bei jeder Mutation sämtliche Listener; umfangreiche DOM-Re-Renders werden ohne Diffing ausgelöst.
- Rendering großer Ergebnislisten (insbesondere Zulassung) erfolgt via `innerHTML` und string-basiertem Template, ohne Virtualisierung oder Chunking.
- Die BVL-Synchronisation iteriert sequenziell über alle Datensätze, führt Worker-Kommunikation pro Batch aus und lädt/manipuliert JSON im Haupt-Thread.

## Build-/Asset-Snapshot (`npm run build`)

| Asset                        | Quelle                                  | Größe (unkomprimiert)             | Anmerkungen                                                                           |
| ---------------------------- | --------------------------------------- | --------------------------------- | ------------------------------------------------------------------------------------- |
| `index.html`                 | Astro Output                            | 13,9 KB                           | Enthält drei `data:`-URIs mit unverarbeitetem TypeScript.                             |
| `inline-0.js`                | `Shell`-Client                          | 2,4 KB                            | TS mit Generics; keine Transpilation, keine Minifizierung.                            |
| `inline-1.js`                | `indexClient`                           | 3,3 KB                            | Importiert sämtliche Feature-Module synchron.                                         |
| `inline-2.js`                | `main.ts`                               | 0,5 KB                            | Importiert `./core/bootstrap` relativ, was aus einer `data:`-URI nicht auflösbar ist. |
| `_astro/index.CHqRlHk4.css`  | kombinierte Styles                      | 8,3 KB                            | Wird korrekt ausgeliefert, jedoch ohne Critical-CSS-Optimierung.                      |
| `_astro/hoisted.w45Pi1Xg.js` | Bootstrap-Bundle Loader                 | 86 B                              | Referenziert externes CDN.                                                            |
| Externe CDNs                 | Bootstrap, Bootstrap Icons, SQLite WASM | ~300 KB (CSS/JS) + ~1,2 MB (WASM) | Kein Self-Hosting, keine SRI, kein Caching unter Kontrolle.                           |

**Kernerkenntnis:** Durch die `?url`-Strategie in den `.astro`-Dateien wird Astro/Vite komplett umgangen. Es entstehen keine rollup-basierten Bundles, keine Tree-Shakes und keine Lazy-Chunks. Der ausgelieferte Code ist in modernen Browsern nicht lauffähig und verhindert jedes Performance-Tuning downstream.

## Priorisierte Findings

### 🔴 P0 – Build-Pipeline bricht Bundling

- Ursache: `script type="module" src={mainScript}` mit `main.ts?url`, analog in `Shell.astro` und `index.astro`.
- Auswirkungen: Keine Transpilation (TS-Syntax in Produktion), keine Hash-Dateien, kein HTTP-Caching, kein Code-Splitting, relative Imports ins Leere → Applikation fällt nach dem Build sofort aus.
- Empfehlung: `?url` entfernen, stattdessen `import` innerhalb der `.astro`-Dateien (`client:load`/`client:idle`) oder `<script>` mit `await import('/@/scripts/...')`. Alternativ: Astro-Komponenten mit `client:only` / `client:load` für jeweilige Features.

### 🔴 P0 – Monolithische Feature-Bootstrap-Last

- `src/scripts/pages/indexClient.ts` initialisiert _alle_ Feature-Module ohne Lazy-Grenzen.
- `manualChunks`-Konfiguration in `astro.config.mjs` greift nicht (falscher Pfad + Bundling-Breakage).
- Folgen: Hohe TTI, auch wenn Nutzer nur ein Feature benötigt.
- Empfehlung: Feature-Gate via `import()` + `IntersectionObserver`, Shell-Events erst bei Bedarf registrieren; manuelles Chunking auf funktionierende IDs umstellen.

### 🔴 P0 – SQLite-WASM von CDN

- Worker lädt zur Laufzeit `sqlite3.mjs` + `sqlite3.wasm` (~1,3 MB) direkt vom CDN.
- Kein Vorab-Download, keine Integritätsprüfung, kein Asset-Caching, hoher Hauptthread-Block bei erstem Query.
- Empfehlung: Paket lokal bundeln (z. B. über `vite.staticCopy`), `initWorker()` erst nach Nutzeraktion starten, Worker warmhalten, WASM via `Response`-Streaming einlesen.

### 🟡 P1 – BVL-Sync blockiert Main Thread

- `syncBvlData` lädt alle Collections seriell, transformiert JSON synchron und schreibt per Worker in großen Batches.
- Jeder Fortschritt löst `updateSlice('zulassung', ...)` aus → alle Listener reagieren. UI friert bei großen Daten (5–10 s) ein.
- Empfehlung: Fetch parallelisieren (Promise.all mit Timeout-Bremse), Transform in Worker verlagern, Worker-Nachrichten bündeln, Fortschritt-Updates auf `requestAnimationFrame` throttlen.

### 🟡 P1 – Zulassung-Rendering via `innerHTML`

- Komplettes UI wird bei jedem Render neu als String gebaut (`section.innerHTML = ...`).
- 1000 Treffer erzeugen >1 MB HTML, blockieren 1–1,5 s.
- Empfehlung: Virtualisierung (bestehende `initVirtualList` adaptieren), komponentenweises Patchen (DocumentFragment + diff), UI in Astro/Islands portieren.

### 🟡 P1 – State/Events ohne Selektivität

- `patchState` und `updateSlice` iterieren über alle Listener (O(n)).
- Listener aktualisieren DOM ungefiltert (z. B. Shell-Buttons, Labels) → unnötige Layout-Thrashes.
- Empfehlung: Selektive Subscription (`subscribeSlice('app', cb)`), `structuredClone` zur Change-Detection vermeiden, Microtask-Debounce.

### 🟢 P2 – Animiertes Starfield

- Läuft dauerhaft mit `requestAnimationFrame`, 150 Sterne, keine Pause bei Background Tabs.
- Arbeitet canvas-intern, aber verursacht CPU-Last auf Low-End-Geräten.
- Empfehlung: `document.visibilityState` prüfen, Animation bei Inaktivität pausieren.

### 🟢 P2 – Duplicate Legacy Assets

- Ordner `assets/js` und `assets/css` (Legacy) bleiben im Repo, aber nicht im Build.
- Risiko: Veraltete Dateien könnten versehentlich ausgeliefert oder gepflegt werden.
- Empfehlung: Entfernen oder klar als Legacy kennzeichnen.

## Quick Wins (0–2 Wochen)

1. **Astro-Skripte korrigieren:** `?url` entfernen, `client:load` einsetzen, `manualChunks` reparieren.
2. **Bundling validieren:** `npm run build` → prüfen, dass `dist/_astro/*.js` mit Hashes entstehen; Lighthouse-Run via `astro preview` + `lighthouse-ci`.
3. **SQLite-WASM lazy laden:** Worker erst beim Öffnen/Import initialisieren, `loadDefaultsConfig` davor beenden.
4. **Zulassung throttlen:** Anzeige auf max. 50 Treffer begrenzen, `requestAnimationFrame`-Loop für Fortschritt nutzen.

## Mittelfristige Maßnahmen (1–2 Monate)

- **Feature-Splitting:** `initZulassung`, `initHistory`, `initReporting` per dynamischem Import hinter Tabs, `prefetch` beim Hover.
- **State-Refactor:** Selektive Listener, Option auf Signals/Store (z. B. `nanostores` oder `zustand`).
- **Worker-Pipeline:** Daten-Transformation und Hashing in den SQLite-Worker verschieben, Bulk-Inserts statt JSON-Schleifen.
- **Self-Hosting & SRI:** Bootstrap/Icons und SQLite-Assets lokal mit `integrity`-Attribut; CDN nur als Fallback.
- **Starfield optional machen:** Toggle in Settings, reduzierte FPS bei `prefers-reduced-motion`.

## Langfristige Maßnahmen (>2 Monate)

- **Astro Islands / Partial Hydration:** UI-Module (z. B. Berechnung, Zulassung) als Astro-Komponenten mit `client:visible` zur Minimierung der Initiallast.
- **Persistente Datenbank:** IndexedDB (Dexie) als Primärspeicher evaluieren; SQLite nur für Export/Import.
- **Performance-Monitoring:** RUM-Metriken (TTFB, FCP, TTI) via `web-vitals` erfassen, Worker-Latenz und Sync-Dauer loggen.
- **Automatisierte Audits:** GitHub Action mit `@astrojs/check`, `@lhci/cli`, Performance-Budget (z. B. <200 KB JS nach Gzip).

## Mess-Setup & weitere Schritte

- `npm run build && du -sh dist/_astro` als Basis-Kennzahl, danach Gzip/Brotli messen (`du -sh dist/_astro/*.js`).
- Chrome-Lighthouse im Mobile-Profile (Slow 4G + Moto G4) laufen lassen, sobald Bundling wieder funktioniert.
- Worker-Ladezeit messen: `performance.mark` um `initWorker` + `import(sqlite3.mjs)` legen, Ergebnisse im Devtools-Performance-Panel dokumentieren.
- BVL-Sync: `console.time('sync')` + Log mit Datensatzanzahl, Hash, Storage-Latenz; Ergebnisse in Telemetrie (z. B. Supabase Edge Logging) halten.

## Offene Fragen

- Welche Feature-Sektionen sind im MVP zwingend? → Priorisierung für Lazy-Loading.
- Muss SQLite offline-fähig bleiben oder reicht IndexedDB? → Architekturentscheidung.
- Dürfen externe CDNs entfallen (Compliance/DSGVO)? → Falls ja, Self-Hosting ASAP.

## Empfohlene Testfälle

- `npm run preview` + Browser-Konsole prüfen, ob gebundelte Module laufen.
- Manuelle Tests: Erstes Öffnen ohne Datenbank, BVL-Sync (WLAN vs. 3G Simulator), Suche mit 1000+ Treffern, Historie-Liste (Virtualisierung).
- Regression: Druckansicht (`window.print`), bevor/after `beforeprint`-Handler.

---

_Erstellt von GitHub Copilot am 11.11.2025. Für Rückfragen gerne melden._
