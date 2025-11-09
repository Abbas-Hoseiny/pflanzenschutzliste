# Task: Migration zu Astro 4 mit GitHub Pages Build

## Kontext

Lies zuerst die folgenden Analysedokumente:
- **ARCHITEKTUR.md** - Vollständige Ist-Architektur
- **PERFORMANCE.md** - Performance-Probleme und Metriken
- **ASTRO-MIGRATION.md** - Migrations-Strategie und Vorteile

**Projektziel:** Migration der bestehenden Vanilla-JS Single-Page-App zu Astro 4 für:
- 86% weniger JavaScript zum Client
- 64% schnellere Load Time  
- Moderne Developer Experience (TypeScript, HMR)
- Automatische Optimierungen (Code-Splitting, Minification)
- GitHub Pages Deployment mit GitHub Actions

**Wichtig:** Der bestehende Code ist **funktional und produktiv**. Ziel ist eine **nicht-invasive Migration**, die die Logik weitgehend beibehält und primär die Build-Pipeline und Komponentenstruktur modernisiert.

## Ziele der Migration

### Primäre Ziele (Must-Have)

1. **Astro-Setup mit GitHub Pages**
   - Astro 4.x Projekt initialisieren
   - GitHub Actions Workflow für automatisches Deployment
   - Build-Output kompatibel mit GitHub Pages (`site: abbas-hoseiny.github.io`, `base: /pflanzenschutzliste`)

2. **Bestehenden Code portieren (ohne große Änderungen)**
   - Layouts: `index.html` → Astro-Layout-Komponente
   - Features: JavaScript-Module → Astro-Komponenten + Client-Scripts
   - Styles: CSS-Dateien übernehmen (mit Scoping)
   - Core-Module: State, EventBus, Storage bleiben weitgehend unverändert

3. **Performance-Optimierung**
   - Code-Splitting: Vendor, Core, Features getrennt
   - Lazy-Loading: Große Komponenten (Zulassung) mit `client:visible`
   - Critical CSS: Above-the-fold inline
   - Asset-Optimierung: Minification, Tree-Shaking

4. **Beibehaltung aller Features**
   - SQLite-WASM Worker funktioniert weiter
   - BVL-Sync mit Manifest-Download
   - Berechnungs- und Historie-Features
   - OPFS-Persistenz (Browser-abhängig)
   - File System Access API
   - PDF-Export

### Sekundäre Ziele (Nice-to-Have)

5. **TypeScript-Migration**
   - Core-Module schrittweise zu `.ts` konvertieren
   - Interfaces für State, Events, API-Responses
   - Strikte Typen für bessere DX

6. **Komponenten-Aufteilung**
   - Große Module (z.B. `zulassung/index.js` - 1.187 Zeilen) in kleinere Komponenten aufteilen
   - Shared-Components für wiederverwendbare UI-Elemente
   - Scoped Styles pro Komponente

7. **Testing-Setup**
   - Vitest für Unit-Tests (State, Utils)
   - Playwright für E2E-Tests (kritische User-Flows)
   - CI-Integration

## Projektstruktur (Ziel)

```
pflanzenschutzliste/
├── .github/
│   └── workflows/
│       └── deploy.yml              # GitHub Actions Deployment
├── src/
│   ├── layouts/
│   │   └── BaseLayout.astro        # Haupt-Layout (ersetzt index.html)
│   ├── pages/
│   │   └── index.astro             # Single-Page Entry
│   ├── components/
│   │   ├── Shell.astro             # Navigation/Header
│   │   ├── Calculation.astro       # Berechnungs-Feature
│   │   ├── History.astro           # Historie-Feature
│   │   ├── Settings.astro          # Einstellungs-Feature
│   │   ├── Reporting.astro         # Reporting-Feature
│   │   ├── Zulassung/              # Zulassungs-Feature (aufgeteilt)
│   │   │   ├── index.astro         # Haupt-Komponente
│   │   │   ├── FilterBar.astro     # Filter-UI
│   │   │   ├── ResultList.astro    # Ergebnis-Liste
│   │   │   └── DetailCard.astro    # Detail-Ansicht
│   │   └── shared/
│   │       ├── Card.astro
│   │       ├── MediumTable.astro
│   │       └── LoadingSpinner.astro
│   ├── scripts/                    # Client-seitige Logik
│   │   ├── core/
│   │   │   ├── state.ts            # State Management (TypeScript)
│   │   │   ├── eventBus.ts         # Event-System
│   │   │   ├── storage/            # Storage-Treiber
│   │   │   │   ├── sqlite.ts
│   │   │   │   ├── sqliteWorker.js # Web Worker (bleibt JS)
│   │   │   │   ├── fileSystem.ts
│   │   │   │   ├── fallback.ts
│   │   │   │   └── index.ts
│   │   │   ├── bvlSync.ts
│   │   │   ├── bvlDataset.ts
│   │   │   ├── bvlClient.ts
│   │   │   ├── database.ts
│   │   │   ├── config.ts
│   │   │   ├── labels.ts
│   │   │   ├── print.ts
│   │   │   ├── utils.ts
│   │   │   └── virtualList.ts
│   │   └── features/               # Feature-Logik (ohne UI)
│   │       ├── calculation/
│   │       │   ├── logic.ts
│   │       │   └── types.ts
│   │       ├── history/
│   │       ├── settings/
│   │       ├── reporting/
│   │       └── zulassung/
│   ├── styles/
│   │   ├── base.css
│   │   ├── layout.css
│   │   └── components.css
│   └── config/
│       ├── defaults.json
│       └── schema.json
├── public/
│   └── workers/                    # Statische Worker-Dateien
│       └── sqliteWorker.js         # Falls nötig als static asset
├── astro.config.mjs
├── tsconfig.json
├── package.json
└── README.md                       # Update mit neuen Build-Anweisungen
```

## Implementierungs-Schritte

### Phase 1: Setup (Tag 1-2) - PRIORITÄT 1

#### 1.1 Astro-Projekt initialisieren

```bash
# Neues Astro-Projekt im selben Repo
npm create astro@latest . -- --template minimal --typescript strict

# Dependencies installieren
npm install

# Dev-Server testen
npm run dev
```

#### 1.2 Astro-Konfiguration für GitHub Pages

```javascript
// astro.config.mjs
import { defineConfig } from 'astro/config';

export default defineConfig({
  site: 'https://abbas-hoseiny.github.io',
  base: '/pflanzenschutzliste',
  output: 'static',
  build: {
    inlineStylesheets: 'auto',
    assets: '_astro'
  },
  vite: {
    build: {
      rollupOptions: {
        output: {
          manualChunks: {
            'sqlite': ['./src/scripts/core/storage/sqlite'],
            'bvl': ['./src/scripts/core/bvlSync'],
            'vendor': ['./src/scripts/core/bootstrap']
          }
        }
      }
    }
  }
});
```

#### 1.3 GitHub Actions Workflow

```yaml
# .github/workflows/deploy.yml
name: Deploy to GitHub Pages

on:
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: "pages"
  cancel-in-progress: false

jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4
      
      - name: Setup Node
        uses: actions/setup-node@v4
        with:
          node-version: 20
          cache: npm
      
      - name: Install dependencies
        run: npm ci
      
      - name: Build with Astro
        run: npm run build
      
      - name: Upload artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: ./dist

  deploy:
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4
```

#### 1.4 TypeScript-Konfiguration

```json
// tsconfig.json
{
  "extends": "astro/tsconfigs/strict",
  "compilerOptions": {
    "target": "ES2022",
    "module": "ESNext",
    "moduleResolution": "bundler",
    "lib": ["ES2022", "DOM", "DOM.Iterable"],
    "jsx": "react-jsx",
    "jsxImportSource": "astro",
    "allowJs": true,
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "forceConsistentCasingInFileNames": true,
    "resolveJsonModule": true,
    "isolatedModules": true,
    "baseUrl": ".",
    "paths": {
      "@/*": ["src/*"],
      "@scripts/*": ["src/scripts/*"],
      "@components/*": ["src/components/*"],
      "@styles/*": ["src/styles/*"]
    }
  },
  "include": ["src/**/*"],
  "exclude": ["node_modules", "dist"]
}
```

**Acceptance Criteria:**
- ✅ `npm run dev` startet Dev-Server
- ✅ `npm run build` generiert `dist/` Ordner
- ✅ GitHub Actions Workflow ist konfiguriert
- ✅ TypeScript-Konfiguration ist valide

### Phase 2: Layout & Shell (Tag 3-5) - PRIORITÄT 1

#### 2.1 BaseLayout erstellen

```astro
---
// src/layouts/BaseLayout.astro
import '../styles/base.css';
import '../styles/layout.css';
import '../styles/components.css';

interface Props {
  title?: string;
}

const { title = 'Pflanzenschutz' } = Astro.props;
---

<!DOCTYPE html>
<html lang="de" data-bs-theme="dark">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{title}</title>
  
  <!-- Bootstrap CDN (wie bisher) -->
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" />
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.0/font/bootstrap-icons.css" />
</head>
<body>
  <canvas id="starCanvas"></canvas>
  <noscript>
    <div class="noscript-warning">
      Diese Anwendung benötigt JavaScript.
    </div>
  </noscript>
  
  <div id="app-root" class="app-root">
    <slot />
  </div>
  
  <!-- Bootstrap JS -->
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
  
  <!-- Starfield Animation -->
  <script>
    import { initStarfield } from '../scripts/features/starfield';
    initStarfield();
  </script>
</body>
</html>
```

#### 2.2 Shell-Komponente (Navigation)

```astro
---
// src/components/Shell.astro
---

<div data-region="shell">
  <nav class="navbar navbar-dark bg-dark navbar-expand-lg">
    <div class="container-fluid">
      <a class="navbar-brand" href="#">
        <i class="bi bi-leaf-fill"></i> Pflanzenschutz
      </a>
      
      <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
        <span class="navbar-toggler-icon"></span>
      </button>
      
      <div class="collapse navbar-collapse" id="navbarNav">
        <ul class="navbar-nav ms-auto">
          <li class="nav-item">
            <button class="btn btn-outline-light nav-btn" data-section="calc">
              <i class="bi bi-calculator"></i> Berechnung
            </button>
          </li>
          <li class="nav-item">
            <button class="btn btn-outline-light nav-btn" data-section="history">
              <i class="bi bi-clock-history"></i> Historie
            </button>
          </li>
          <li class="nav-item">
            <button class="btn btn-outline-light nav-btn" data-section="zulassung">
              <i class="bi bi-file-text"></i> Zulassung
            </button>
          </li>
          <li class="nav-item">
            <button class="btn btn-outline-light nav-btn" data-section="settings">
              <i class="bi bi-gear"></i> Einstellungen
            </button>
          </li>
          <li class="nav-item">
            <button class="btn btn-outline-light nav-btn" data-section="reporting">
              <i class="bi bi-printer"></i> Berichte
            </button>
          </li>
        </ul>
      </div>
    </div>
  </nav>
</div>

<footer data-region="footer" class="footer">
  <div class="container-fluid">
    <span class="text-muted">© 2025 Pflanzenschutzliste</span>
  </div>
</footer>

<script>
  import { emit } from '@scripts/core/eventBus';
  
  document.addEventListener('DOMContentLoaded', () => {
    const buttons = document.querySelectorAll('.nav-btn');
    
    buttons.forEach(button => {
      button.addEventListener('click', (e) => {
        const section = (e.currentTarget as HTMLElement).dataset.section;
        
        // Active-State setzen
        buttons.forEach(btn => btn.classList.remove('active'));
        button.classList.add('active');
        
        // Event emittieren
        emit('app:sectionChanged', section);
      });
    });
    
    // Initial: Berechnung aktiv
    buttons[0]?.classList.add('active');
  });
</script>

<style>
  .navbar {
    position: sticky;
    top: 0;
    z-index: 1000;
    box-shadow: 0 2px 10px rgba(0, 0, 0, 0.3);
  }
  
  .nav-btn {
    margin: 0 0.25rem;
  }
  
  .nav-btn.active {
    background-color: var(--color-accent);
    border-color: var(--color-accent);
  }
  
  .footer {
    position: fixed;
    bottom: 0;
    width: 100%;
    padding: 1rem;
    background-color: var(--color-navbar);
    box-shadow: 0 -2px 10px rgba(0, 0, 0, 0.2);
  }
</style>
```

#### 2.3 CSS-Dateien migrieren

```bash
# Einfach kopieren, da Astro CSS-Import unterstützt
cp assets/css/*.css src/styles/
```

**Acceptance Criteria:**
- ✅ Layout rendert korrekt
- ✅ Navigation funktioniert (Events)
- ✅ Starfield-Animation läuft
- ✅ CSS-Styles werden angewendet

### Phase 3: Core-Module (Tag 6-10) - PRIORITÄT 1

#### 3.1 State Management zu TypeScript

**Wichtig:** Bestehende Logik **nicht ändern**, nur zu TypeScript konvertieren!

```typescript
// src/scripts/core/state.ts
import { getDefaultFieldLabels } from './labels';

export interface AppState {
  app: {
    ready: boolean;
    version: string | null;
    hasFileAccess: boolean;
    hasDatabase: boolean;
    activeSection: 'calc' | 'history' | 'zulassung' | 'settings' | 'reporting';
    storageDriver: 'memory' | 'sqlite' | 'filesystem' | 'localstorage';
  };
  company: {
    name: string;
    headline: string;
    logoUrl: string;
    contactEmail: string;
    address: string;
    accentColor: string;
  };
  defaults: {
    waterPerKisteL: number;
    kistenProAr: number;
    form: {
      creator: string;
      location: string;
      crop: string;
      quantity: string;
    };
  };
  measurementMethods: any[]; // TODO: Typisieren
  mediums: any[];
  history: any[];
  fieldLabels: Record<string, string>;
  calcContext: any | null;
  zulassung: {
    filters: {
      culture: string | null;
      pest: string | null;
      text: string;
      includeExpired: boolean;
      bioOnly: boolean;
    };
    results: any[];
    lastSync: string | null;
    lastResultCounts: any | null;
    dataSource: string | null;
    apiStand: string | null;
    manifestVersion: string | null;
    lastSyncHash: string | null;
    busy: boolean;
    progress: {
      step: string | null;
      percent: number;
      message: string;
    };
    error: string | null;
    logs: any[];
    debug: {
      schema: any | null;
      lastSyncLog: any[];
      manifest: any | null;
      lastAutoUpdateCheck: any | null;
    };
    lookups: {
      cultures: any[];
      pests: any[];
    };
    autoUpdateAvailable: boolean;
    autoUpdateVersion: string | null;
  };
  ui: {
    notifications: any[];
  };
}

type StateListener = (state: AppState, prevState: AppState) => void;

const listeners = new Set<StateListener>();

let state: AppState = {
  // ... (bestehende Initial-State-Struktur kopieren)
};

export function getState(): AppState {
  return state;
}

export function getSlice<K extends keyof AppState>(key: K): AppState[K] {
  return state[key];
}

export function subscribeState(listener: StateListener): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

function notify(prevState: AppState): void {
  for (const listener of listeners) {
    try {
      listener(state, prevState);
    } catch (err) {
      console.error('state listener error', err);
    }
  }
}

export function patchState(patch: Partial<AppState>): AppState {
  const prevState = state;
  state = { ...state, ...patch };
  notify(prevState);
  return state;
}

export function updateSlice<K extends keyof AppState>(
  sliceKey: K,
  updater: ((current: AppState[K], state: AppState) => AppState[K]) | AppState[K]
): AppState {
  const currentSlice = state[sliceKey];
  const nextSlice = typeof updater === 'function' ? updater(currentSlice, state) : updater;
  
  if (nextSlice === currentSlice) {
    return state;
  }
  
  return patchState({ [sliceKey]: nextSlice } as Partial<AppState>);
}

// ... resetState() etc.
```

#### 3.2 EventBus zu TypeScript

```typescript
// src/scripts/core/eventBus.ts
type EventHandler<T = any> = (payload: T) => void;

const listeners = new Map<string, Set<EventHandler>>();

export function emit<T = any>(eventName: string, payload?: T): void {
  const handlers = listeners.get(eventName);
  if (!handlers) return;
  
  for (const handler of handlers) {
    try {
      handler(payload);
    } catch (err) {
      console.error(`Event handler error for "${eventName}"`, err);
    }
  }
}

export function subscribe<T = any>(
  eventName: string,
  handler: EventHandler<T>
): () => void {
  if (!listeners.has(eventName)) {
    listeners.set(eventName, new Set());
  }
  
  listeners.get(eventName)!.add(handler);
  
  return () => {
    listeners.get(eventName)?.delete(handler);
  };
}

export function unsubscribe<T = any>(
  eventName: string,
  handler: EventHandler<T>
): void {
  listeners.get(eventName)?.delete(handler);
}
```

#### 3.3 Weitere Core-Module

**Reihenfolge:**
1. `labels.ts` (einfach)
2. `utils.ts` (einfach)
3. `config.ts` (mittel)
4. `database.ts` (mittel)
5. `storage/index.ts` (mittel)
6. `storage/fallback.ts` (mittel)
7. `storage/fileSystem.ts` (mittel)
8. `storage/sqlite.ts` (komplex)
9. `storage/sqliteWorker.js` → **BLEIBT JavaScript** (Web Worker)
10. `bvlClient.ts` (komplex)
11. `bvlDataset.ts` (komplex)
12. `bvlSync.ts` (komplex)
13. `print.ts` (mittel)
14. `virtualList.ts` (mittel)

**Wichtig:**
- Bestehende Logik **nicht ändern**
- Nur Typen hinzufügen
- Fehlerbehandlung beibehalten
- Tests schreiben (falls Zeit)

**Acceptance Criteria:**
- ✅ Alle Core-Module sind TypeScript
- ✅ `sqliteWorker.js` funktioniert weiterhin
- ✅ Keine Breaking Changes
- ✅ TypeScript-Compiler hat keine Fehler

### Phase 4: Feature-Komponenten (Tag 11-21) - PRIORITÄT 2

#### 4.1 Calculation-Feature

```astro
---
// src/components/Calculation.astro
import { getState } from '@scripts/core/state';

const state = getState();
const { defaults, fieldLabels } = state;
---

<div class="calculation-section" data-section="calc" style="display: none;">
  <div class="container-fluid py-4">
    <div class="card card-dark">
      <div class="card-header">
        <h2>
          <i class="bi bi-calculator"></i>
          {fieldLabels.calculation || 'Berechnung'}
        </h2>
      </div>
      <div class="card-body">
        <form id="calculation-form">
          <!-- Formular-Felder aus bestehendem Code übernehmen -->
          <div class="row">
            <div class="col-md-6 mb-3">
              <label for="creator" class="form-label">
                {fieldLabels.creator || 'Ersteller'}
              </label>
              <input
                type="text"
                class="form-control"
                id="creator"
                name="creator"
                value={defaults.form.creator}
              />
            </div>
            <!-- ... weitere Felder -->
          </div>
          
          <button type="submit" class="btn btn-primary">
            <i class="bi bi-play-fill"></i> Berechnen
          </button>
        </form>
        
        <div id="calculation-results" style="display: none;">
          <!-- Ergebnis-Anzeige -->
        </div>
      </div>
    </div>
  </div>
</div>

<script>
  // Client-seitige Logik aus features/calculation/index.js übernehmen
  import { getState, subscribeState } from '@scripts/core/state';
  import { performCalculation } from '@scripts/features/calculation/logic';
  
  const form = document.getElementById('calculation-form') as HTMLFormElement;
  const resultsDiv = document.getElementById('calculation-results');
  
  // Section-Visibility Management
  subscribeState((state) => {
    const section = document.querySelector('[data-section="calc"]') as HTMLElement;
    if (state.app.activeSection === 'calc') {
      section.style.display = 'block';
    } else {
      section.style.display = 'none';
    }
  });
  
  // Form-Submit
  form?.addEventListener('submit', async (e) => {
    e.preventDefault();
    const formData = new FormData(form);
    
    try {
      const result = await performCalculation(formData);
      displayResults(result);
    } catch (error) {
      console.error('Calculation error', error);
      alert('Fehler bei der Berechnung');
    }
  });
  
  function displayResults(result: any) {
    // Rendering-Logik aus bestehendem Code
    if (resultsDiv) {
      resultsDiv.style.display = 'block';
      resultsDiv.innerHTML = /* ... */;
    }
  }
</script>

<style>
  .calculation-section {
    max-width: 1400px;
    margin: 0 auto;
  }
</style>
```

```typescript
// src/scripts/features/calculation/logic.ts
// Reine Berechnungs-Logik (keine UI)
export async function performCalculation(formData: FormData): Promise<any> {
  // Logik aus features/calculation/index.js extrahieren
  // ...
}
```

**Analog für:**
- `History.astro` (Tag 13-14)
- `Settings.astro` (Tag 15)
- `Reporting.astro` (Tag 16-17)

#### 4.2 Zulassung-Feature (Komplex, Tag 18-21)

**Aufteilung in mehrere Komponenten:**

```astro
---
// src/components/Zulassung/index.astro
import FilterBar from './FilterBar.astro';
import ResultList from './ResultList.astro';
import SyncSection from './SyncSection.astro';
---

<div class="zulassung-section" data-section="zulassung" style="display: none;">
  <div class="container-fluid py-4">
    <FilterBar />
    <SyncSection />
    <ResultList />
  </div>
</div>

<script>
  import { subscribeState } from '@scripts/core/state';
  
  subscribeState((state) => {
    const section = document.querySelector('[data-section="zulassung"]') as HTMLElement;
    section.style.display = state.app.activeSection === 'zulassung' ? 'block' : 'none';
  });
</script>
```

```astro
---
// src/components/Zulassung/FilterBar.astro
---

<div class="card card-dark mb-3">
  <div class="card-body">
    <div class="row g-3">
      <div class="col-md-3">
        <label class="form-label">Kultur</label>
        <select class="form-select" id="filter-culture">
          <option value="">Alle</option>
          <!-- Dynamisch gefüllt -->
        </select>
      </div>
      <div class="col-md-3">
        <label class="form-label">Schadorganismus</label>
        <select class="form-select" id="filter-pest">
          <option value="">Alle</option>
        </select>
      </div>
      <div class="col-md-3">
        <label class="form-label">Suchtext</label>
        <input type="text" class="form-control" id="filter-text" />
      </div>
      <div class="col-md-3">
        <div class="form-check mt-4">
          <input class="form-check-input" type="checkbox" id="filter-bio" />
          <label class="form-check-label" for="filter-bio">
            <i class="bi bi-leaf-fill text-success"></i> Nur Bio/Öko
          </label>
        </div>
      </div>
    </div>
    <div class="mt-3">
      <button class="btn btn-primary" id="btn-search">
        <i class="bi bi-search"></i> Suchen
      </button>
    </div>
  </div>
</div>

<script>
  import { updateSlice } from '@scripts/core/state';
  import { emit } from '@scripts/core/eventBus';
  
  // Filter-Logik aus zulassung/index.js
  document.getElementById('btn-search')?.addEventListener('click', () => {
    const filters = {
      culture: (document.getElementById('filter-culture') as HTMLSelectElement).value || null,
      pest: (document.getElementById('filter-pest') as HTMLSelectElement).value || null,
      text: (document.getElementById('filter-text') as HTMLInputElement).value,
      bioOnly: (document.getElementById('filter-bio') as HTMLInputElement).checked
    };
    
    updateSlice('zulassung', (z) => ({ ...z, filters }));
    emit('zulassung:search', filters);
  });
</script>
```

**Weitere Sub-Komponenten:**
- `SyncSection.astro` - BVL-Sync UI
- `ResultList.astro` - Ergebnis-Liste mit Virtual Scrolling
- `DetailCard.astro` - Detail-Ansicht

**Acceptance Criteria:**
- ✅ Alle Features funktionieren wie vorher
- ✅ UI ist identisch (oder besser)
- ✅ Keine Funktionalitäts-Regression
- ✅ Code ist in kleinere Komponenten aufgeteilt

### Phase 5: Optimierungen (Tag 22-25) - PRIORITÄT 2

#### 5.1 Code-Splitting konfigurieren

```javascript
// astro.config.mjs (erweitert)
export default defineConfig({
  // ...
  vite: {
    build: {
      rollupOptions: {
        output: {
          manualChunks(id) {
            // Vendor-Code
            if (id.includes('node_modules')) {
              return 'vendor';
            }
            
            // Storage-Layer (lazy)
            if (id.includes('storage/sqlite')) {
              return 'sqlite';
            }
            
            // BVL-Features (lazy)
            if (id.includes('bvlSync') || id.includes('bvlDataset')) {
              return 'bvl';
            }
            
            // Features (lazy)
            if (id.includes('features/zulassung')) {
              return 'zulassung';
            }
            if (id.includes('features/calculation')) {
              return 'calculation';
            }
            if (id.includes('features/history')) {
              return 'history';
            }
          }
        }
      }
    }
  }
});
```

#### 5.2 Client-Directives für Lazy-Hydration

```astro
---
// src/pages/index.astro
import BaseLayout from '@/layouts/BaseLayout.astro';
import Shell from '@/components/Shell.astro';
import Calculation from '@/components/Calculation.astro';
import History from '@/components/History.astro';
import Zulassung from '@/components/Zulassung/index.astro';
import Settings from '@/components/Settings.astro';
import Reporting from '@/components/Reporting.astro';
---

<BaseLayout>
  <Shell client:load />
  
  <main data-region="main">
    <!-- Critical: Sofort laden -->
    <Calculation client:load />
    
    <!-- Important: Bei Idle laden -->
    <History client:idle />
    <Settings client:idle />
    
    <!-- Heavy: Bei Sichtbarkeit laden -->
    <Zulassung client:visible />
    <Reporting client:visible />
  </main>
</BaseLayout>
```

#### 5.3 Bundle-Analyse

```bash
# Bundle-Größe analysieren
npm run build
npx vite-bundle-visualizer dist/stats.html
```

**Ziel:**
- Initial Bundle: < 50 KB (JS)
- Zulassung-Chunk: < 40 KB
- Sqlite-Chunk: < 50 KB
- Gesamt (alle geladen): < 200 KB

**Acceptance Criteria:**
- ✅ Code-Splitting funktioniert
- ✅ Lazy-Loading reduziert Initial-Load
- ✅ Bundle-Größen im Zielbereich

### Phase 6: Testing & Deployment (Tag 26-30) - PRIORITÄT 3

#### 6.1 Unit-Tests (Vitest)

```typescript
// src/scripts/core/state.test.ts
import { describe, it, expect, beforeEach } from 'vitest';
import { getState, patchState, subscribeState } from './state';

describe('State Management', () => {
  it('should return initial state', () => {
    const state = getState();
    expect(state.app.ready).toBe(false);
  });
  
  it('should update state', () => {
    patchState({ app: { ...getState().app, ready: true } });
    expect(getState().app.ready).toBe(true);
  });
  
  it('should notify subscribers', () => {
    let notified = false;
    subscribeState(() => { notified = true; });
    patchState({ app: { ready: true } });
    expect(notified).toBe(true);
  });
});
```

**Test-Abdeckung:**
- State Management
- EventBus
- Utils (formatAmount, escapeHtml, etc.)
- Calculation-Logic

#### 6.2 E2E-Tests (Playwright)

```typescript
// tests/e2e/basic.spec.ts
import { test, expect } from '@playwright/test';

test('should load homepage', async ({ page }) => {
  await page.goto('/');
  await expect(page.locator('h1')).toBeVisible();
});

test('should perform calculation', async ({ page }) => {
  await page.goto('/');
  
  // Navigation
  await page.click('[data-section="calc"]');
  
  // Formular ausfüllen
  await page.fill('#creator', 'Test User');
  await page.fill('#location', 'Test Farm');
  
  // Berechnen
  await page.click('button[type="submit"]');
  
  // Ergebnis prüfen
  await expect(page.locator('#calculation-results')).toBeVisible();
});

test('should switch sections', async ({ page }) => {
  await page.goto('/');
  
  await page.click('[data-section="history"]');
  await expect(page.locator('[data-section="history"]')).toBeVisible();
  
  await page.click('[data-section="zulassung"]');
  await expect(page.locator('[data-section="zulassung"]')).toBeVisible();
});
```

#### 6.3 CI-Integration

```yaml
# .github/workflows/deploy.yml (erweitert)
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-node@v4
      - run: npm ci
      
      # Tests ausführen
      - run: npm run test
      
      # E2E-Tests
      - run: npx playwright install --with-deps
      - run: npm run test:e2e
      
      # Build
      - run: npm run build
      
      # Upload
      - uses: actions/upload-pages-artifact@v3
        with:
          path: ./dist
```

**Acceptance Criteria:**
- ✅ Unit-Tests laufen und passen
- ✅ E2E-Tests validieren kritische Flows
- ✅ CI-Pipeline ist grün

### Phase 7: Dokumentation (Final)

#### 7.1 README.md aktualisieren

```markdown
# Bio-Pflanzenschutz

Moderne Web-Anwendung zur Verwaltung von Pflanzenschutzmitteln, gebaut mit Astro 4.

## Features

- ✅ Berechnungen mit persistierter Historie
- ✅ BVL-Zulassungsdaten mit Auto-Sync
- ✅ SQLite-WASM mit OPFS-Support
- ✅ PDF-Export
- ✅ Offline-fähig

## Entwicklung

```bash
# Installation
npm install

# Dev-Server
npm run dev

# Build
npm run build

# Preview
npm run preview

# Tests
npm run test
npm run test:e2e
```

## Deployment

Automatisches Deployment zu GitHub Pages via GitHub Actions bei Push zu `main`.

## Technologie-Stack

- **Framework:** Astro 4.x
- **Sprache:** TypeScript
- **UI:** Bootstrap 5
- **Datenbank:** SQLite-WASM
- **Hosting:** GitHub Pages

## Performance

- **Initial Load:** < 2s (4G)
- **Time to Interactive:** < 3s
- **Bundle-Größe:** ~200 KB (all features)
- **Lighthouse Score:** 95+

## Lizenz

MIT License
```

#### 7.2 MIGRATION.md erstellen

Dokumentiere die durchgeführten Änderungen für zukünftige Entwickler.

**Acceptance Criteria:**
- ✅ README.md ist aktuell
- ✅ Migrations-Dokumentation vorhanden

## Qualitätssicherung

### Manuelle Tests (vor Produktionsfreigabe)

1. **Funktionale Tests:**
   - ✅ Neue Berechnung durchführen
   - ✅ Historie laden und anzeigen
   - ✅ BVL-Daten synchronisieren
   - ✅ Nach Zulassungen suchen
   - ✅ PDF exportieren
   - ✅ Einstellungen ändern
   - ✅ Datenbank exportieren/importieren

2. **Browser-Tests:**
   - ✅ Chrome (Desktop)
   - ✅ Firefox (Desktop)
   - ✅ Safari (Desktop)
   - ✅ Chrome (Android)
   - ✅ Safari (iOS)

3. **Performance-Tests:**
   - ✅ Lighthouse Audit (Score > 90)
   - ✅ Bundle-Größe (< 200 KB gesamt)
   - ✅ Time to Interactive (< 3s)

4. **Regressions-Tests:**
   - ✅ Alle Features aus alter Version funktionieren
   - ✅ Keine Breaking Changes

## Acceptance Criteria (Gesamt)

### Muss (P0)
- ✅ Astro-Projekt ist aufgesetzt
- ✅ GitHub Actions Deployment funktioniert
- ✅ Alle Core-Module sind TypeScript
- ✅ Alle Features sind portiert und funktional
- ✅ Keine Funktionalitäts-Regression
- ✅ Code-Splitting ist implementiert
- ✅ Performance-Ziele erreicht (< 200 KB, < 3s TTI)

### Soll (P1)
- ✅ Unit-Tests für Core-Module
- ✅ E2E-Tests für kritische Flows
- ✅ Bundle-Analyse durchgeführt
- ✅ Dokumentation aktualisiert

### Kann (P2)
- ⬜ Komponenten-Library (Storybook)
- ⬜ Progressive Web App (Service Worker)
- ⬜ Weitere Optimierungen (Image-Optimization, etc.)

## Notizen & Best Practices

### Do's
- ✅ Bestehende Logik beibehalten (nicht re-implementieren)
- ✅ TypeScript-Typen inkrementell hinzufügen
- ✅ Kleine, fokussierte Commits
- ✅ Tests vor Refactorings
- ✅ Performance-Budget einhalten

### Don'ts
- ❌ Keine großen Refactorings während Migration
- ❌ Keine neuen Features während Migration
- ❌ Keine Breaking Changes für Nutzer
- ❌ Keine ungetesteten Änderungen in Production

### Tipps
- Web Worker (`sqliteWorker.js`) als static asset behandeln
- State-Management bleibt client-side (kein SSR)
- Bootstrap über CDN beibehalten (erst später optimieren)
- Schrittweise migrieren (Feature für Feature)
- Regelmäßig testen (nach jedem Feature)

## Eskalation

Bei Problemen oder Fragen:
1. Analysedokumente nochmal lesen (ARCHITEKTUR.md, PERFORMANCE.md)
2. Astro-Dokumentation konsultieren (docs.astro.build)
3. Existing Code als Referenz nutzen
4. Im Zweifel: Bestehende Lösung beibehalten

## Erfolgs-Metriken

### Vor Migration
- Bundle: 360 KB JS
- Load Time: 4-15s (je nach Netzwerk)
- Lighthouse: 72/100

### Nach Migration (Ziel)
- Bundle: 50 KB initial + 150 KB on-demand
- Load Time: 1-3s
- Lighthouse: 95+/100

**Verbesserung:** -86% JavaScript, -64% Load Time

Viel Erfolg bei der Migration! 🚀
