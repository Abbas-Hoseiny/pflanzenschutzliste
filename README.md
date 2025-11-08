# Bio-Pflanzenschutz – All in One

Statische Web-Anwendung zur Verwaltung, Berechnung und Dokumentation von Pflanzenschutzmitteln. Die gesamte Logik läuft im Browser – inklusive einer **SQLite-WASM** Datenbank im Web Worker für große Datenmengen. Für Browser ohne OPFS stehen weiterhin JSON-basierte Speicher-Treiber zur Verfügung.

## Highlights

- Moderne Single-Page-App (ES-Module, kein Build-Step nötig)
- Persistent gespeicherte Berechnungen, History und Stammdaten
- SQLite-WASM mit OPFS-Unterstützung, Fallback auf JSON-Dateien oder LocalStorage
- **BVL Zulassungs-Datenbank** – Integration der offiziellen Pflanzenschutzmittel-Zulassungsdaten
- Sofortdruck (PDF) und Export/Import von Datenbank-Dateien
- Nutzerfreundliche Features wie `beforeunload`-Hinweis bei aktiver Datenbankverbindung

## Projektstruktur

- `index.html` – Host-Dokument, bindet Bootstrap & App Entry-Point
- `assets/css/` – Basistheme, Layout und Komponenten-Styles
- `assets/config/` – Seed-Daten (`defaults.json`) und JSON-Schema (`schema.json`)
- `assets/js/main.js` – Einstiegspunkt, bootstrapped die App
- `assets/js/core/` – State-Management, Events, Storage, SQLite-Worker, Konfiguration
- `assets/js/core/storage/` – Treiber für SQLite-WASM, File System Access API, LocalStorage
- `assets/js/features/` – Feature-Module (Startup, Shell, Calculation, History, Zulassung, Settings, Reporting, Starfield)

## BVL Zulassungs-Datenbank

Die Anwendung integriert die offizielle BVL (Bundesamt für Verbraucherschutz und Lebensmittelsicherheit) API für Pflanzenschutzmittel-Zulassungen. Im Tab **"Zulassung"** können Nutzer:

- Nach zugelassenen Pflanzenschutzmitteln suchen
- Filter nach Kultur und Schadorganismus anwenden
- Detailinformationen wie Aufwandmenge, Wartezeit und Zulassungsende abrufen
- Daten manuell über den "Daten aktualisieren"-Button synchronisieren

### Erstmalige Nutzung

1. Öffnen Sie den Tab **"Zulassung"** nach dem Verbinden einer Datenbank
2. Klicken Sie auf **"Daten aktualisieren"**, um die BVL-Daten zu laden
3. Der Vorgang kann einige Minuten dauern (ca. 6 API-Endpunkte werden geladen)
4. Nach dem Laden stehen die Suchfunktionen zur Verfügung

### Aktualisierung

- Die Daten werden lokal in der SQLite-Datenbank gespeichert
- Ein erneutes Klicken auf "Daten aktualisieren" lädt nur neue Änderungen
- Bei unverändertem Datenstand wird "Keine Aktualisierung erforderlich" angezeigt
- Die letzte Aktualisierung wird im UI angezeigt

### Datenquellen

Die Daten stammen von der offiziellen BVL OpenAPI: `https://psm-api.bvl.bund.de/ords/psm/api-v1/`

Folgende Endpunkte werden verwendet:
- `mittel` – Stammdaten der Pflanzenschutzmittel
- `awg` – Anwendungen (Mittel × Kultur × Schadorganismus)
- `awg_kultur` – Kulturen zu Anwendungen
- `awg_schadorg` – Schadorganismen zu Anwendungen
- `awg_aufwand` – Aufwand- und Wassermengen
- `awg_wartezeit` – Wartezeiten

Weitere Details zur API finden sich in `API.md`.


## Storage-Treiber & Browser-Support

| Treiber | Format | Persistenz | Voraussetzungen | Empfohlen für |
| --- | --- | --- | --- | --- |
| SQLite-WASM | `.sqlite`/`.db` | OPFS (Chromium) oder In-Memory | Chromium ≥108, HTTPS/localhost, WebAssembly | Produktive Nutzung, große Historien |
| File System Access | `.json` | Lokale Datei via Picker | Chromium ≥86, HTTPS/localhost | Kleine/mittlere Datenmengen, manuelles Speichern |
| LocalStorage | `.json` | Browser-Storage (max. ~10 MB) | Alle modernen Browser | Demo/Test ohne Dateizugriff |

> Firefox & Safari unterstützen aktuell kein OPFS. In diesen Browsern läuft SQLite im Speicher und Änderungen sollten zusätzlich als JSON oder SQLite-Datei exportiert werden.

## Arbeiten mit SQLite-WASM

1. **Neue Datenbank erstellen** – Dateien werden im OPFS abgelegt (Chromium-basiert) oder optional direkt als `.sqlite` exportiert.
2. **Bestehende Datei verbinden** – sowohl JSON- als auch SQLite-Dateien werden erkannt und importiert.
3. **Änderungen speichern** – History-Änderungen (Speichern/Löschen) schreiben direkt in die aktive Datenbank.
4. **Seite verlassen** – Nutzer erhalten einen Hinweis, dass die Verbindung getrennt wird, solange eine DB aktiv ist.

## Datenmodell

### JSON-Snapshot (Export/Import)

```json
{
  "meta": {
    "version": 1,
    "company": { "name": "", "logoUrl": "", "contactEmail": "" },
    "defaults": { "waterPerKisteL": 5, "kistenProAr": 300 },
    "measurementMethods": [ { "id": "perKiste", "label": "pro Kiste" } ],
    "fieldLabels": { }
  },
  "mediums": [ { "id": "water", "name": "Wasser", "methodId": "perKiste", "value": 0.0166 } ],
  "history": [ { "ersteller": "…", "datum": "…", "items": [ … ] } ]
}
```

`assets/config/schema.json` beschreibt das komplette JSON-Schema.

### SQLite-Schema

| Tabelle | Inhalt |
| --- | --- |
| `meta` | Schlüssel/Werte für Company, Defaults, Labels, Version |
| `measurement_methods` | Messmethoden inkl. Konfiguration (JSON in Spalten) |
| `mediums` | Mittel mit Referenz auf Messmethode |
| `history` | Header eines Historien-Eintrags (JSON) |
| `history_items` | Detailwerte pro Mittel |

SQL-Definition: `assets/js/core/storage/schema.sql`.

## Entwicklung & Setup

```bash
# Repository klonen
git clone https://github.com/Abbas-Hoseiny/pflanzenschutzliste.git
cd pflanzenschutzliste

# Statischer Dev-Server (Beispiel)
python3 -m http.server

# Browser öffnen
open http://localhost:8000
```

Empfohlener Browser für Entwicklung: Chrome oder Edge (ermöglicht SQLite + OPFS). Beim ersten Start können die bereitgestellten Defaults geladen oder direkt eine neue Datenbank erstellt werden.

Deployment auf GitHub Pages funktioniert out-of-the-box, da die App aus statischen Assets besteht. Die Anwendung läuft dauerhaft öffentlich unter [https://abbas-hoseiny.github.io/pflanzenschutzliste/](https://abbas-hoseiny.github.io/pflanzenschutzliste/) und kann ohne Entwicklerkenntnisse direkt im Browser genutzt werden.

## Tests & Qualität

Automatisierte Tests sind aktuell nicht hinterlegt. Für Releases sollten diese manuellen Szenarien geprüft werden:

1. Neue SQLite-Datenbank erstellen, Berechnung durchführen, History speichern, Seite neu laden und Eintrag prüfen.
2. Historie-Eintrag löschen, Seite neu laden und sicherstellen, dass der Eintrag entfernt bleibt.
3. JSON-Datei importieren und anschließend als SQLite exportieren (und umgekehrt).
4. Browser ohne OPFS (Firefox/Safari) – prüfen, dass Fallback funktioniert und der Nutzer bei Bedarf Dateien exportieren kann.

## Lizenz

[MIT License](LICENSE) © 2025 Pflanzenschutzliste
