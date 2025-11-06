# Bio-Pflanzenschutz – All in One

Statische Web-Anwendung zur Verwaltung und Berechnung von Pflanzenschutzmitteln. Die App läuft komplett im Browser und nutzt wahlweise die File System Access API oder einen lokalen Fallback, um JSON-Datenbanken zu lesen und zu speichern. Die Oberfläche wurde in modulare Features aufgeteilt und lässt sich über GitHub Pages direkt ausliefern.

## Architekturüberblick

- **index.html** – minimaler Host, bindet Bootstrap sowie die gebündelten Styles und lädt `assets/js/main.js` als ES-Modul.
- **assets/css/** – thematische Stylesheets (`base.css`, `layout.css`, `components.css`) mit CSS-Variablen für das Branding.
- **assets/config/** – Seed- und Schema-Dateien (`defaults.json`, `schema.json`) liefern Startdaten und Validierungsgrundlage.
- **assets/js/core/** – Infrastrukturmodule (State-Management, EventBus, Storage-Abstraktion, Bootstrap, Config-Handling, Database-Snapshots).
- **assets/js/features/** – unabhängige Feature-Pakete (Startup, Shell, Calculation, History, Settings, Reporting, Starfield).

## Lokale Entwicklung

1. Repository klonen oder Codespace öffnen.
2. Projekt über einen lokalen Webserver ausliefern, z. B.:
   ```bash
   python3 -m http.server
   ```
3. Anschließend `http://localhost:8000` im Browser öffnen.
4. Beim ersten Start eine neue JSON-Datenbank erstellen oder die Defaults laden.

> **Hinweis:** File System Access API funktioniert nur in Chromium-basierten Browsern über HTTPS oder `http://localhost`. In anderen Browsern fällt die App automatisch auf den LocalStorage-Fallback zurück.

## Deployment auf GitHub Pages

1. Stelle sicher, dass die statischen Assets committet sind.
2. Aktiviere GitHub Pages für den `master`- (oder `main`-) Branch über die Repository-Einstellungen.
3. Die Anwendung ist anschließend unter `https://<user>.github.io/pflanzenschutzliste/` erreichbar.

## Datenstruktur

```json
{
  "meta": {
    "version": 1,
    "company": { "name": "", "logoUrl": "", "contactEmail": "", "address": "", "accentColor": "" },
    "defaults": { "waterPerKisteL": 5, "kistenProAr": 300 },
    "measurementMethods": [ { "id": "perKiste", ... }, ... ]
  },
  "mediums": [ { "id": "water", "name": "Wasser", ... } ],
  "history": [ { "header": {...}, "items": [...] } ]
}
```

`assets/config/schema.json` enthält das vollständige JSON-Schema.

## Tests

Aktuell keine automatisierten Tests. Sichtprüfungen erfolgen manuell im Browser.

## Lizenz

Noch nicht festgelegt – bitte ergänzen.
