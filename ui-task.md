# UI Task: Manifest-basierte BVL-Daten mit Bio-Gruppierung darstellen

## Kontext

- Das Frontend (`assets/js`) synchronisiert künftig ausschließlich die per Manifest ausgelieferte SQLite-Datenbank aus dem `pflanzenschutz-db`-Repo (vgl. `db-task.md`).
- Die UI muss sämtliche Inhalte aus den reduzierten Tabellen anzeigen, Bio-Produkte prominent kennzeichnen und zusätzliche Komfortfunktionen (Filter, Fortschritt, Auto-Update-Prüfung) bieten.
- Zielgruppe sind praxisnahe Anwender:innen: klare Badges, verständliche Tooltips, moderne Icons/Farben (Bootstrap 5 / Bootstrap Icons), aussagekräftige Fehlermeldungen.

## Ziele

1. **Manifest-First UX:** Sync-Schritt zeigt Manifest-Version, Build-Stand, Datenquelle und reagiert korrekt auf "no change".
2. **Bio-Gruppierung & Filter:** Nutzer:innen können Bio-Produkte gezielt filtern und erkennen sie in Ergebnislisten sofort.
3. **Erweiterte Detailkarten:** Alle in der DB verfügbaren Zusatzfelder (Gefahren, Wirkstoffe, Vertrieb, Abpackung, Auflagen, Partner, Piktogramme) erscheinen kontextsensitiv.
4. **Moderne UI-Elemente:** Fortschrittsanzeigen, Badges, Icons, Farben und Layout folgen konsistenten Designregeln, bleiben aber leichtgewichtig.
5. **Debug & Auto-Update:** Entwickler:innen bzw. Power-User bekommen gute Einsicht in Manifest, Sync-Logs, Schema und Auto-Update-Checks.

## Muss-Anpassungen

### 1. Sync-Oberfläche (`assets/js/features/zulassung/index.js`, `renderStatusSection`, `renderSyncSection`)

- Manifest-Metadaten anzeigen: `version`, `build.finished_at`, `dataSource`, `apiStand`, `lastSyncHash`. Bei fehlender Info Platzhalter, aber Layout nicht zerbrechen.
- Fortschrittsanzeige über mehrere Stufen: `manifest`, `download`, `decompress`, `import`, `verify`. Jede Stufe mit eindeutigem Icon (`bi-cloud-download`, `bi-archive`, `bi-cpu`, `bi-check2`).
- Fortschrittsbalken (Bootstrap `.progress`) farblich differenziert (z. B. `bg-info` für Download, `bg-success` für Abschluss) und mit Tooltip (`title`) für Detailtext.
- Fehlerzustände prominent (Alert mit `bi-exclamation-triangle-fill`), Link "Debug anzeigen" öffnet Debug-Sektion.
- Buttons disabled bei `busy`, Text `Synchronisiere…`. Bei Erfolg kurzer Toast/Alert: "Daten aktualisiert" mit Manifest-Version.

### 2. Bio-Hinweise (vorerst deaktiviert)

- Bio-spezifische Filter und Badges wurden entfernt, bis die Datenbasis stabil gepflegt wird.
- Hinweise aus Manifest/Enrichment dürfen weiterhin im Freitext erscheinen, beeinflussen aber keine UI-Elemente.

### 3. Detailkarten erweitern

- **Gefahrenhinweise:** Neben H-Code auch Text anzeigen (`<span>` oder Tooltip). Falls Piktogramme verfügbar (aus `bvl_mittel_gefahrhinweise` oder eigenem Table), Icon oder Bild ergänzen (SVGs in `assets/img/ghs/`).
- **Sicherheitshinweise/P-Sätze:** Wenn Tabellen geliefert werden, analog rendern (Badge `bg-warning-subtle`).
- **Wirkstoffe:** Menge + Einheit mit `Intl.NumberFormat`, Option zur Anzeige eines Tooltips mit vollem JSON (Debug).
- **Vertrieb:** Name, Website-Link (Icon `bi-box-arrow-up-right`), optional Land/Ort.
- **Abpackung:** Tabelle oder Liste mit Menge + Einheit.
- **Auflagen & Partner:** Accordion oder collapsible Listen, nur wenn Daten vorhanden.
- Alle Abschnitte mit `bi-` Icons in Überschriften (z. B. `bi-activity` für Aufwände, `bi-umbrella` für Schadorganismen).

### 4. Moderne Interaktion

- Suchergebnisse paginieren oder "Laden mehr" (mind. Soft-Limit 100 Treffer) um Performance zu wahren.
- Sticky Filterleiste auf Desktop, responsive Verhalten auf Mobilgeräten (Bootstrap Grid).
- Badges/Labels mit ausreichend Kontrast, Farbpalette: Success (Bio), Info (Kulturen), Secondary (Standard), Warning (Fristen), Danger (Ausnahmen).
- Keyboard-Accessibility: Enter löst Suche aus, Tab Reihenfolge beachten.

### 5. Auto-Update & Debug

- Nach App-Start automatischer Manifest-Check im Hintergrund: Wenn neue Version → Hinweisbanner "Neue Daten verfügbar" + Button zum Starten.
- Debug-Sektion erweitern um:
  - Manifest-JSON (collapsible `<details>`)
  - Letzten Auto-Update-Check (Zeit, Ergebnis)
  - `diagnoseBvlSchema` Ausgabe mit Scrollbereich
  - Download-/Import-Metriken (Bytes, Dauer)
- Logs strukturieren (Badge pro Level: Info=primary, Warn=warning, Error=danger) und lange Texte umbrechen.

## Code-Fokus (Module)

| Modul                                    | Aufgabe                                                                                                       |
| ---------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| `assets/js/core/bvlSync.js`              | Progress/Log-Events verfeinern, Auto-Update-Check via `fetchManifest` ohne Import, `services.events` Trigger. |
| `assets/js/core/bvlDataset.js`           | Zusätzliche Progress-Callbacks (`decompress`), Dateimengen (MB) berechnen, Feature-Flag für Auto-Check.       |
| `assets/js/core/storage/sqliteWorker.js` | SQL-Filter für Bio, Aggregation von Counts, optionale Views (`bvl_mittel_extras`).                            |
| `assets/js/features/zulassung/index.js`  | UI-Rendering, Filter-Handling, Badge/Icon-Design, Debug-Panel.                                                |
| `assets/css/components.css`              | Neue Badges, Fortschrittsfarben, Responsive Styles, Icon-Abstände.                                            |

## Qualitätssicherung

1. **Manuelle Tests** (Chrome, Firefox, Safari, iPadOS):
   - Initialer Sync nach Cache-Reset, Fortschrittsphasen korrekt?
   - Auto-Update-Banner erscheint bei neuer Manifest-Version (simulierte URL).
   - Bio-spezifische Filter sind deaktiviert; vorhandene Texte dürfen weiterhin erscheinen.
   - Gefahrenabschnitte/Icons erscheinen nur, wenn Daten da.
   - Fehlerfälle (Offline, 404 Manifest) zeigen klare Meldungen.
2. **Unit/Integration** (optional, aber empfohlen):
   - Utility-Funktionen für Progress-Mapping, Bio-Filter.
   - Snapshot-Tests für Renderfunktionen (z. B. mit Vitest/jsdom).
3. **Accessibility**: Quick-Check mit Lighthouse/axe (Kontrast, ARIA für Collapse/Accordion).

## Abnahme-Kriterien

- Sync nutzt ausschließlich Manifestpfad und visualisiert jeden Schritt.
- Bio-Gruppierung und Flags werden nicht mehr aktiv verwendet; keine UI-Abhängigkeiten mehr.
- Ergebnisliste zeigt neue Icons/Farben, ohne Layout-Brüche bei fehlenden Daten.
- Debug-Panel informiert über Manifest, Schema, Logs.
- Auto-Update meldet verfügbare Versionen ohne kompletten Download.
- Code kommentiert nur dort, wo Logik nicht offensichtlich ist; alle neuen Strings deutschsprachig.

Bitte diese Aufgaben sequenziell umsetzen, regelmäßig testen und bei offenen Fragen das Product-Team einbeziehen.
