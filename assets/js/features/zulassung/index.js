/**
 * Zulassung Feature Module
 * Provides UI for searching BVL plant protection product approvals
 */

import { syncBvlData } from '../../core/bvlSync.js';
import * as sqlite from '../../core/storage/sqlite.js';

let initialized = false;
let container = null;
let services = null;

/**
 * Initialize the Zulassung feature
 */
export function initZulassung(region, svc) {
  if (initialized) {
    return;
  }
  
  container = region;
  services = svc;
  
  // Subscribe to state changes
  services.state.subscribe((state) => {
    if (state.app.activeSection === 'zulassung') {
      renderZulassungUI(state);
    }
  });
  
  // Subscribe to database connection events
  services.events.subscribe('database:connected', async () => {
    try {
      const lastSync = await sqlite.getBvlMeta('lastSyncIso');
      services.state.updateSlice('zulassung', z => ({
        ...z,
        lastSync
      }));
      
      // Load lookups
      await loadLookups();
    } catch (error) {
      console.error('Failed to load BVL metadata:', error);
    }
  });
  
  initialized = true;
}

/**
 * Load lookup lists (cultures and pests)
 */
async function loadLookups() {
  try {
    const [cultures, pests] = await Promise.all([
      sqlite.listBvlCultures(),
      sqlite.listBvlSchadorg()
    ]);
    
    services.state.updateSlice('zulassung', z => ({
      ...z,
      lookups: {
        cultures: cultures || [],
        pests: pests || []
      }
    }));
  } catch (error) {
    console.error('Failed to load lookups:', error);
  }
}

/**
 * Render the Zulassung UI
 */
function renderZulassungUI(state) {
  if (!container) return;
  
  const { zulassung, app } = state;
  
  // Only render if section is active and database is connected
  if (app.activeSection !== 'zulassung') {
    container.classList.add('d-none');
    return;
  }
  
  container.classList.remove('d-none');
  
  const hasData = zulassung.lookups.cultures.length > 0 || zulassung.lookups.pests.length > 0;
  
  container.innerHTML = `
    <div class="container py-4">
      <div class="row">
        <div class="col-12">
          <h2 class="mb-4">Zulassungs-Datenbank</h2>
          
          ${hasData ? renderSearchSection(zulassung) : renderEmptyState(zulassung)}
          
          ${zulassung.results.length > 0 ? renderResults(zulassung) : ''}
        </div>
      </div>
    </div>
  `;
  
  attachEventListeners();
}

/**
 * Render empty state when no data is available
 */
function renderEmptyState(zulassung) {
  return `
    <div class="card">
      <div class="card-body text-center py-5">
        <h5 class="card-title">Keine BVL-Daten vorhanden</h5>
        <p class="card-text text-muted mb-4">
          Bitte laden Sie zuerst die Zulassungsdaten von der BVL-API.
        </p>
        <button 
          id="btn-update-data" 
          class="btn btn-primary"
          ${zulassung.busy ? 'disabled' : ''}
        >
          ${zulassung.busy ? 'Lädt...' : 'Daten aktualisieren'}
        </button>
        ${zulassung.lastSync ? `
          <p class="text-muted small mt-3">
            Letzte Aktualisierung: ${formatDate(zulassung.lastSync)}
          </p>
        ` : ''}
        ${zulassung.error ? `
          <div class="alert alert-danger mt-3" role="alert">
            ${zulassung.error}
          </div>
        ` : ''}
        ${zulassung.busy ? `
          <div class="progress mt-3" style="height: 2rem;">
            <div 
              id="sync-progress" 
              class="progress-bar progress-bar-striped progress-bar-animated" 
              role="progressbar" 
              style="width: 0%"
            >
              <span id="sync-message">Initialisiere...</span>
            </div>
          </div>
        ` : ''}
      </div>
    </div>
  `;
}

/**
 * Render search section
 */
function renderSearchSection(zulassung) {
  const { filters, lookups, busy, error, lastSync } = zulassung;
  
  return `
    <div class="card mb-4">
      <div class="card-header d-flex justify-content-between align-items-center">
        <h5 class="mb-0">Suche nach zugelassenen Mitteln</h5>
        <button 
          id="btn-update-data" 
          class="btn btn-sm btn-outline-primary"
          ${busy ? 'disabled' : ''}
        >
          ${busy ? 'Lädt...' : 'Daten aktualisieren'}
        </button>
      </div>
      <div class="card-body">
        ${lastSync ? `
          <div class="alert alert-info alert-dismissible fade show" role="alert">
            <small>Letzte Aktualisierung: ${formatDate(lastSync)}</small>
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
          </div>
        ` : ''}
        
        ${error ? `
          <div class="alert alert-danger alert-dismissible fade show" role="alert">
            ${error}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
          </div>
        ` : ''}
        
        ${busy ? `
          <div class="progress mb-3" style="height: 2rem;">
            <div 
              id="sync-progress" 
              class="progress-bar progress-bar-striped progress-bar-animated" 
              role="progressbar" 
              style="width: 0%"
            >
              <span id="sync-message">Initialisiere...</span>
            </div>
          </div>
        ` : ''}
        
        <form id="search-form">
          <div class="row g-3">
            <div class="col-md-4">
              <label for="filter-culture" class="form-label">Kultur</label>
              <select id="filter-culture" class="form-select">
                <option value="">Alle Kulturen</option>
                ${lookups.cultures.map(c => `
                  <option value="${escapeHtml(c.kultur)}" ${filters.culture === c.kultur ? 'selected' : ''}>
                    ${escapeHtml(c.kultur)} (${c.count})
                  </option>
                `).join('')}
              </select>
            </div>
            
            <div class="col-md-4">
              <label for="filter-pest" class="form-label">Schadorganismus</label>
              <select id="filter-pest" class="form-select">
                <option value="">Alle Schadorganismen</option>
                ${lookups.pests.map(p => `
                  <option value="${escapeHtml(p.schadorg)}" ${filters.pest === p.schadorg ? 'selected' : ''}>
                    ${escapeHtml(p.schadorg)} (${p.count})
                  </option>
                `).join('')}
              </select>
            </div>
            
            <div class="col-md-4">
              <label for="filter-text" class="form-label">Mittelname / Kennnummer</label>
              <input 
                type="text" 
                id="filter-text" 
                class="form-control" 
                placeholder="Suche..."
                value="${escapeHtml(filters.text)}"
              />
            </div>
            
            <div class="col-12">
              <div class="form-check">
                <input 
                  type="checkbox" 
                  class="form-check-input" 
                  id="filter-expired"
                  ${filters.includeExpired ? 'checked' : ''}
                />
                <label class="form-check-label" for="filter-expired">
                  Abgelaufene Zulassungen einbeziehen
                </label>
              </div>
            </div>
            
            <div class="col-12">
              <button type="submit" class="btn btn-primary" ${busy ? 'disabled' : ''}>
                Suchen
              </button>
              <button type="button" id="btn-reset" class="btn btn-secondary ms-2" ${busy ? 'disabled' : ''}>
                Zurücksetzen
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  `;
}

/**
 * Render search results
 */
function renderResults(zulassung) {
  const { results } = zulassung;
  
  if (results.length === 0) {
    return `
      <div class="alert alert-info" role="alert">
        Keine Ergebnisse gefunden. Bitte passen Sie die Suchkriterien an.
      </div>
    `;
  }
  
  return `
    <div class="card">
      <div class="card-header">
        <h5 class="mb-0">Ergebnisse (${results.length})</h5>
      </div>
      <div class="card-body">
        <div class="table-responsive">
          <table class="table table-striped table-hover">
            <thead>
              <tr>
                <th>Kennnummer</th>
                <th>Mittelname</th>
                <th>Formulierung</th>
                <th>Kulturen</th>
                <th>Schadorganismen</th>
                <th>Aufwand</th>
                <th>Wartezeit</th>
                <th>Zul. Ende</th>
              </tr>
            </thead>
            <tbody>
              ${results.map(r => `
                <tr>
                  <td><code>${escapeHtml(r.kennr)}</code></td>
                  <td>${escapeHtml(r.mittelname)}</td>
                  <td>${escapeHtml(r.formulierung || '-')}</td>
                  <td>
                    ${r.kulturen ? `<span class="text-success">${escapeHtml(r.kulturen)}</span>` : '-'}
                    ${r.kulturen_ausgenommen ? `<br><small class="text-danger">Ausgenommen: ${escapeHtml(r.kulturen_ausgenommen)}</small>` : ''}
                  </td>
                  <td>
                    ${r.schadorg ? `<span class="text-success">${escapeHtml(r.schadorg)}</span>` : '-'}
                    ${r.schadorg_ausgenommen ? `<br><small class="text-danger">Ausgenommen: ${escapeHtml(r.schadorg_ausgenommen)}</small>` : ''}
                  </td>
                  <td>
                    ${r.mittel_menge && r.mittel_einheit ? 
                      `${r.mittel_menge} ${escapeHtml(r.mittel_einheit)}` : '-'
                    }
                    ${r.wasser_menge && r.wasser_einheit ? 
                      `<br><small>${r.wasser_menge} ${escapeHtml(r.wasser_einheit)} Wasser</small>` : ''
                    }
                  </td>
                  <td>${r.tage ? `${r.tage} Tage` : 'keine Angabe'}</td>
                  <td>${r.zul_ende ? formatDate(r.zul_ende) : '-'}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  `;
}

/**
 * Attach event listeners
 */
function attachEventListeners() {
  // Update data button
  const btnUpdate = document.getElementById('btn-update-data');
  if (btnUpdate) {
    btnUpdate.addEventListener('click', handleUpdateData);
  }
  
  // Search form
  const searchForm = document.getElementById('search-form');
  if (searchForm) {
    searchForm.addEventListener('submit', handleSearch);
  }
  
  // Reset button
  const btnReset = document.getElementById('btn-reset');
  if (btnReset) {
    btnReset.addEventListener('click', handleReset);
  }
}

/**
 * Handle update data button click
 */
async function handleUpdateData(event) {
  event.preventDefault();
  
  // Set busy state
  services.state.updateSlice('zulassung', z => ({
    ...z,
    busy: true,
    error: null
  }));
  
  try {
    const result = await syncBvlData({
      onProgress: (current, total, message) => {
        const progressBar = document.getElementById('sync-progress');
        const progressMessage = document.getElementById('sync-message');
        if (progressBar && progressMessage) {
          const percent = Math.round((current / total) * 100);
          progressBar.style.width = `${percent}%`;
          progressMessage.textContent = message;
        }
      }
    });
    
    // Update state with result
    services.state.updateSlice('zulassung', z => ({
      ...z,
      busy: false,
      lastSync: new Date().toISOString(),
      error: result.status === 'failed' ? result.message : null
    }));
    
    // Reload lookups if successful
    if (result.status === 'updated') {
      await loadLookups();
    }
    
    // Show notification
    if (result.status === 'updated' || result.status === 'no-change') {
      showNotification(result.message, 'success');
    } else {
      showNotification(result.message, 'danger');
    }
    
  } catch (error) {
    console.error('Update failed:', error);
    services.state.updateSlice('zulassung', z => ({
      ...z,
      busy: false,
      error: error.message
    }));
    showNotification(`Fehler: ${error.message}`, 'danger');
  }
}

/**
 * Handle search form submission
 */
async function handleSearch(event) {
  event.preventDefault();
  
  const culture = document.getElementById('filter-culture')?.value || null;
  const pest = document.getElementById('filter-pest')?.value || null;
  const text = document.getElementById('filter-text')?.value || '';
  const includeExpired = document.getElementById('filter-expired')?.checked || false;
  
  // Update filters in state
  services.state.updateSlice('zulassung', z => ({
    ...z,
    filters: { culture, pest, text, includeExpired }
  }));
  
  // Perform search
  try {
    const results = await sqlite.queryZulassung({
      culture: culture || null,
      pest: pest || null,
      text: text || null,
      includeExpired,
      limit: 100,
      offset: 0
    });
    
    services.state.updateSlice('zulassung', z => ({
      ...z,
      results,
      error: null
    }));
    
  } catch (error) {
    console.error('Search failed:', error);
    services.state.updateSlice('zulassung', z => ({
      ...z,
      error: error.message,
      results: []
    }));
  }
}

/**
 * Handle reset button click
 */
function handleReset(event) {
  event.preventDefault();
  
  services.state.updateSlice('zulassung', z => ({
    ...z,
    filters: { culture: null, pest: null, text: '', includeExpired: false },
    results: []
  }));
}

/**
 * Show notification
 */
function showNotification(message, type = 'info') {
  services.events.emit('notification:show', { message, type });
}

/**
 * Format ISO date to readable format
 */
function formatDate(isoString) {
  if (!isoString) return '-';
  const date = new Date(isoString);
  return date.toLocaleDateString('de-DE', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });
}

/**
 * Escape HTML to prevent XSS
 */
function escapeHtml(unsafe) {
  if (unsafe === null || unsafe === undefined) return '';
  return String(unsafe)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}
