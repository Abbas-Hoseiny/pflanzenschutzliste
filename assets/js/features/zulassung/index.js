/**
 * Zulassung Feature - BVL Approval Database
 * Manages filtering and display of approved plant protection products
 */

import { syncBvlData } from '../../core/bvlSync.js';
import * as sqliteDriver from '../../core/storage/sqlite.js';

let container = null;
let services = null;
let currentRequest = null;

export function initZulassung(regionEl, serviceRefs) {
  container = regionEl;
  services = serviceRefs;
  
  render();
  
  // Subscribe to state changes
  services.state.subscribe((nextState, prevState) => {
    const activeChanged = nextState.app.activeSection !== prevState.app.activeSection;
    const zulassungChanged = nextState.zulassung !== prevState.zulassung;
    const dbChanged = nextState.app.hasDatabase !== prevState.app.hasDatabase;
    
    if (activeChanged || zulassungChanged || dbChanged) {
      render();
    }
  });
  
  // Load lookups when database is connected
  services.events.subscribe('database:connected', async () => {
    await loadLookups();
    await loadLastSync();
  });
}

function render() {
  const state = services.state.getState();
  
  if (state.app.activeSection !== 'zulassung') {
    container.classList.add('d-none');
    return;
  }
  
  container.classList.remove('d-none');
  
  if (!state.app.hasDatabase) {
    container.innerHTML = '<div class="alert alert-info">Bitte verbinden Sie zuerst eine Datenbank.</div>';
    return;
  }
  
  // Check if SQLite is supported
  const isSqliteSupported = sqliteDriver.isSupported();
  
  const { filters, results, busy, error, lastSync, lookups } = state.zulassung;
  
  container.innerHTML = `
    <div class="container">
      <div class="row mb-4">
        <div class="col">
          <h2>Zulassungsdatenbank</h2>
          <p class="text-muted">
            Suchen Sie nach zugelassenen Pflanzenschutzmitteln nach Kultur und Schadorganismus.
          </p>
        </div>
      </div>
      
      <div class="card mb-4">
        <div class="card-body">
          <div class="row g-3 mb-3">
            <div class="col-md-3">
              <label class="form-label">Kultur</label>
              <select class="form-select" id="filter-culture" ${busy ? 'disabled' : ''}>
                <option value="">-- Alle --</option>
                ${lookups.cultures.map(c => 
                  `<option value="${c}" ${filters.culture === c ? 'selected' : ''}>${c}</option>`
                ).join('')}
              </select>
            </div>
            
            <div class="col-md-3">
              <label class="form-label">Schadorganismus</label>
              <select class="form-select" id="filter-pest" ${busy ? 'disabled' : ''}>
                <option value="">-- Alle --</option>
                ${lookups.pests.map(p => 
                  `<option value="${p}" ${filters.pest === p ? 'selected' : ''}>${p}</option>`
                ).join('')}
              </select>
            </div>
            
            <div class="col-md-3">
              <label class="form-label">Freitext</label>
              <input type="text" class="form-control" id="filter-text" 
                     value="${filters.text}" placeholder="Mittelname oder Kennr."
                     ${busy ? 'disabled' : ''}>
            </div>
            
            <div class="col-md-3 d-flex align-items-end">
              <div class="form-check">
                <input class="form-check-input" type="checkbox" id="filter-expired"
                       ${filters.includeExpired ? 'checked' : ''} ${busy ? 'disabled' : ''}>
                <label class="form-check-label" for="filter-expired">
                  Abgelaufene einbeziehen
                </label>
              </div>
            </div>
          </div>
          
          <div class="d-flex gap-2">
            <button class="btn btn-primary" id="btn-search" ${busy ? 'disabled' : ''}>
              ${busy ? '<span class="spinner-border spinner-border-sm me-1"></span>' : ''}
              Suchen
            </button>
            
            <button class="btn btn-outline-secondary" id="btn-update" ${busy || !isSqliteSupported ? 'disabled' : ''}>
              ${busy ? '<span class="spinner-border spinner-border-sm me-1"></span>' : ''}
              Daten aktualisieren
            </button>
            
            ${!isSqliteSupported ? '<span class="text-warning ms-2 small">Online-Update erfordert SQLite-WASM Unterstützung</span>' : ''}
            
            ${lastSync ? `<span class="text-muted ms-auto align-self-center small">
              Letzte Aktualisierung: ${new Date(lastSync).toLocaleString('de-DE')}
            </span>` : ''}
          </div>
          
          ${error ? `<div class="alert alert-danger mt-3 mb-0">${error}</div>` : ''}
        </div>
      </div>
      
      <div id="results-container">
        ${renderResults(results, busy)}
      </div>
    </div>
  `;
  
  attachEventHandlers();
}

function renderResults(results, busy) {
  if (busy) {
    return '<div class="text-center py-5"><div class="spinner-border"></div></div>';
  }
  
  if (!results || results.length === 0) {
    return `
      <div class="alert alert-info">
        Keine Ergebnisse gefunden. Verwenden Sie die Filter oben, um nach Mitteln zu suchen,
        oder klicken Sie auf "Daten aktualisieren", um die BVL-Daten zu laden.
      </div>
    `;
  }
  
  return `
    <div class="card">
      <div class="card-body">
        <h5 class="card-title">${results.length} Ergebnis${results.length !== 1 ? 'se' : ''}</h5>
        <div class="table-responsive">
          <table class="table table-hover">
            <thead>
              <tr>
                <th>Kennr.</th>
                <th>Mittelname</th>
                <th>Formulierung</th>
                <th>Zulassungsende</th>
                <th>Aufwand</th>
                <th>Wartezeit</th>
              </tr>
            </thead>
            <tbody>
              ${results.map(r => `
                <tr>
                  <td><code>${r.kennr}</code></td>
                  <td>${r.name}</td>
                  <td>${r.formulierung || '-'}</td>
                  <td>${r.zul_ende || '-'}</td>
                  <td>
                    ${r.aufwand && r.aufwand.length > 0 
                      ? r.aufwand.map(a => 
                          `${a.mittel_menge} ${a.mittel_einheit || ''}${a.wasser_menge ? ' / ' + a.wasser_menge + ' ' + (a.wasser_einheit || '') : ''}`
                        ).join('<br>')
                      : '-'
                    }
                  </td>
                  <td>
                    ${r.wartezeit && r.wartezeit.length > 0
                      ? r.wartezeit.map(w => 
                          `${w.kultur || 'Alle'}: ${w.tage} Tage${w.bemerkung_kode ? ' (' + w.bemerkung_kode + ')' : ''}`
                        ).join('<br>')
                      : '-'
                    }
                  </td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  `;
}

function attachEventHandlers() {
  const searchBtn = container.querySelector('#btn-search');
  const updateBtn = container.querySelector('#btn-update');
  const cultureSelect = container.querySelector('#filter-culture');
  const pestSelect = container.querySelector('#filter-pest');
  const textInput = container.querySelector('#filter-text');
  const expiredCheckbox = container.querySelector('#filter-expired');
  
  if (searchBtn) {
    searchBtn.addEventListener('click', handleSearch);
  }
  
  if (updateBtn) {
    updateBtn.addEventListener('click', handleUpdate);
  }
  
  if (cultureSelect) {
    cultureSelect.addEventListener('change', (e) => {
      services.state.updateSlice('zulassung', z => ({
        ...z,
        filters: { ...z.filters, culture: e.target.value || null }
      }));
    });
  }
  
  if (pestSelect) {
    pestSelect.addEventListener('change', (e) => {
      services.state.updateSlice('zulassung', z => ({
        ...z,
        filters: { ...z.filters, pest: e.target.value || null }
      }));
    });
  }
  
  if (textInput) {
    textInput.addEventListener('input', (e) => {
      services.state.updateSlice('zulassung', z => ({
        ...z,
        filters: { ...z.filters, text: e.target.value }
      }));
    });
  }
  
  if (expiredCheckbox) {
    expiredCheckbox.addEventListener('change', (e) => {
      services.state.updateSlice('zulassung', z => ({
        ...z,
        filters: { ...z.filters, includeExpired: e.target.checked }
      }));
    });
  }
}

async function handleSearch() {
  const state = services.state.getState();
  const { filters } = state.zulassung;
  
  // Cancel previous request
  if (currentRequest) {
    currentRequest.abort();
  }
  
  services.state.updateSlice('zulassung', z => ({
    ...z,
    busy: true,
    error: null
  }));
  
  try {
    const results = await sqliteDriver.queryZulassung(filters);
    
    services.state.updateSlice('zulassung', z => ({
      ...z,
      results,
      busy: false
    }));
  } catch (err) {
    services.state.updateSlice('zulassung', z => ({
      ...z,
      error: `Suche fehlgeschlagen: ${err.message}`,
      busy: false
    }));
  }
}

async function handleUpdate() {
  services.state.updateSlice('zulassung', z => ({
    ...z,
    busy: true,
    error: null
  }));
  
  try {
    // Check if online
    if (!navigator.onLine) {
      throw new Error('Keine Internetverbindung verfügbar');
    }
    
    const result = await syncBvlData({
      sqliteDriver,
      onProgress: (endpoint, current, total) => {
        console.log(`Syncing ${endpoint} (${current}/${total})`);
      }
    });
    
    if (result.status === 'updated') {
      // Reload lookups
      await loadLookups();
      
      const lastSync = await sqliteDriver.getBvlMeta('lastSyncIso');
      
      services.state.updateSlice('zulassung', z => ({
        ...z,
        busy: false,
        lastSync,
        error: null
      }));
      
      alert(result.message);
    } else if (result.status === 'no-change') {
      const lastSync = await sqliteDriver.getBvlMeta('lastSyncIso');
      
      services.state.updateSlice('zulassung', z => ({
        ...z,
        busy: false,
        lastSync,
        error: null
      }));
      
      alert(result.message);
    } else {
      throw new Error(result.error || 'Unbekannter Fehler');
    }
  } catch (err) {
    let errorMessage = err.message;
    
    // Differentiate error types
    if (err.message.includes('timeout') || err.message.includes('Timeout')) {
      errorMessage = 'Zeitüberschreitung: Die Anfrage hat zu lange gedauert. Bitte versuchen Sie es später erneut.';
    } else if (err.message.includes('Network') || err.message.includes('Failed to fetch')) {
      errorMessage = 'Netzwerkfehler: Bitte überprüfen Sie Ihre Internetverbindung.';
    } else if (err.message.includes('HTTP')) {
      errorMessage = `API-Fehler: ${err.message}`;
    }
    
    services.state.updateSlice('zulassung', z => ({
      ...z,
      error: `Aktualisierung fehlgeschlagen: ${errorMessage}`,
      busy: false
    }));
  }
}

async function loadLookups() {
  try {
    const [cultures, pests] = await Promise.all([
      sqliteDriver.listBvlCultures(),
      sqliteDriver.listBvlSchadorg()
    ]);
    
    services.state.updateSlice('zulassung', z => ({
      ...z,
      lookups: { cultures, pests }
    }));
  } catch (err) {
    console.error('Failed to load lookups:', err);
  }
}

async function loadLastSync() {
  try {
    const lastSync = await sqliteDriver.getBvlMeta('lastSyncIso');
    
    if (lastSync) {
      services.state.updateSlice('zulassung', z => ({
        ...z,
        lastSync
      }));
    }
  } catch (err) {
    console.error('Failed to load lastSync:', err);
  }
}
