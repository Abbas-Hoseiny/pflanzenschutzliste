import { getState } from '../../core/state.js';
import { printHtml } from '../../core/print.js';
import { saveDatabase, getActiveDriverKey } from '../../core/storage/index.js';
import { getDatabaseSnapshot } from '../../core/database.js';
import { buildMediumTableHTML, buildMediumSummaryLines } from '../shared/mediumTable.js';
import { escapeHtml } from '../../core/utils.js';
import { initVirtualList } from '../../core/virtualList.js';
import { renderCalculationSnapshot } from '../shared/calculationSnapshot.js';
import { printEntriesChunked } from '../shared/printing.js';

let initialized = false;
const selectedIndexes = new Set();
let virtualListInstance = null;

// Feature flag for virtual scrolling
const USE_VIRTUAL_SCROLLING = true;
const INITIAL_LOAD_LIMIT = 200;

async function persistHistoryChanges() {
  const driverKey = getActiveDriverKey();
  if (!driverKey || driverKey === 'memory') {
    return;
  }
  try {
    const snapshot = getDatabaseSnapshot();
    await saveDatabase(snapshot);
  } catch (err) {
    console.error('Fehler beim Persistieren der Historie', err);
    alert('Historie konnte nicht dauerhaft gespeichert werden. Bitte erneut versuchen.');
  }
}

function createSection() {
  const section = document.createElement('section');
  section.className = 'section-container d-none';
  section.dataset.section = 'history';
  section.innerHTML = `
    <div class="section-inner">
      <h2 class="text-center mb-4">Historie – Frühere Einträge</h2>
      <div class="card card-dark">
        <div class="card-header d-flex flex-column flex-lg-row justify-content-between align-items-lg-center gap-3 no-print">
          <div class="small text-muted" data-role="selection-info">Keine Einträge ausgewählt.</div>
          <button class="btn btn-outline-light btn-sm" data-action="print-selected" disabled>Ausgewählte drucken</button>
        </div>
        <div class="card-body">
          <div data-role="history-list" class="history-list"></div>
        </div>
      </div>
      <div class="card card-dark mt-4 d-none" id="history-detail">
        <div class="card-header bg-info text-white">
          <h5 class="mb-0">Details</h5>
        </div>
        <div class="card-body" id="history-detail-body"></div>
      </div>
    </div>
  `;
  return section;
}

/**
 * Incremental update for single card selection
 */
function updateCardSelection(listContainer, index, selected) {
  const card = listContainer.querySelector(`.calc-snapshot-card[data-index="${index}"]`);
  if (!card) return;
  
  card.classList.toggle('calc-snapshot-card--selected', selected);
  const checkbox = card.querySelector('[data-action="toggle-select"]');
  if (checkbox) {
    checkbox.checked = selected;
  }
}

/**
 * Initialize or update virtual list
 */
function renderCardsList(state, listContainer, labels) {
  const entries = state.history || [];
  const resolvedLabels = labels || getState().fieldLabels;

  // Clean up any stale selected indexes
  for (const idx of Array.from(selectedIndexes)) {
    if (!entries[idx]) {
      selectedIndexes.delete(idx);
    }
  }

  if (USE_VIRTUAL_SCROLLING && entries.length > INITIAL_LOAD_LIMIT) {
    // Use virtual scrolling for large lists
    if (!virtualListInstance) {
      virtualListInstance = initVirtualList(listContainer, {
        itemCount: entries.length,
        estimatedItemHeight: 250,
        overscan: 6,
        renderItem: (node, index) => {
          const entry = entries[index];
          const selected = selectedIndexes.has(index);
          node.innerHTML = renderCalculationSnapshot(entry, resolvedLabels, {
            showActions: true,
            includeCheckbox: true,
            index,
            selected
          });
        }
      });
    } else {
      // Update existing virtual list
      virtualListInstance.updateItemCount(entries.length);
    }
  } else {
    // Regular rendering for smaller lists or fallback
    // Destroy virtual list if it exists
    if (virtualListInstance) {
      virtualListInstance.destroy();
      virtualListInstance = null;
    }

    // Render all or initial batch
    const limit = Math.min(entries.length, INITIAL_LOAD_LIMIT);
    const fragment = document.createDocumentFragment();
    
    for (let i = 0; i < limit; i++) {
      const entry = entries[i];
      const selected = selectedIndexes.has(i);
      const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
        showActions: true,
        includeCheckbox: true,
        index: i,
        selected
      });
      
      const wrapper = document.createElement('div');
      wrapper.innerHTML = cardHtml;
      fragment.appendChild(wrapper.firstElementChild);
    }

    listContainer.innerHTML = '';
    listContainer.appendChild(fragment);

    // Add "Load More" button if needed
    if (entries.length > limit) {
      const loadMoreBtn = document.createElement('button');
      loadMoreBtn.className = 'btn btn-secondary w-100 mt-3';
      loadMoreBtn.textContent = `Mehr laden (${entries.length - limit} weitere)`;
      loadMoreBtn.dataset.action = 'load-more';
      loadMoreBtn.dataset.currentLimit = String(limit);
      listContainer.appendChild(loadMoreBtn);
    }
  }
}

function renderTable(state, section, labels) {
  const listContainer = section.querySelector('[data-role="history-list"]');
  if (listContainer) {
    renderCardsList(state, listContainer, labels);
  }
}

function renderDetail(entry, section, index = null, labels) {
  const detailCard = section.querySelector('#history-detail');
  const detailBody = section.querySelector('#history-detail-body');
  if (!entry) {
    detailCard.classList.add('d-none');
    detailBody.innerHTML = '';
    delete detailCard.dataset.index;
    return;
  }
  detailCard.dataset.index = index !== null ? String(index) : '';
  const resolvedLabels = labels || getState().fieldLabels;
  const tableLabels = resolvedLabels.history.tableColumns;
  const detailLabels = resolvedLabels.history.detail;
  const snapshotTable = buildMediumTableHTML(entry.items, resolvedLabels, 'detail');
  detailBody.innerHTML = `
    <p>
      <strong>${escapeHtml(tableLabels.date)}:</strong> ${escapeHtml(entry.datum || entry.date || '')}<br />
      <strong>${escapeHtml(detailLabels.creator)}:</strong> ${escapeHtml(entry.ersteller || '')}<br />
      <strong>${escapeHtml(detailLabels.location)}:</strong> ${escapeHtml(entry.standort || '')}<br />
      <strong>${escapeHtml(detailLabels.crop)}:</strong> ${escapeHtml(entry.kultur || '')}<br />
      <strong>${escapeHtml(detailLabels.quantity)}:</strong> ${escapeHtml(entry.kisten != null ? String(entry.kisten) : '')}
    </p>
    <div class="table-responsive">
      ${snapshotTable}
    </div>
    <button class="btn btn-outline-secondary no-print" data-action="detail-print">Drucken / PDF</button>
  `;
  detailCard.classList.remove('d-none');
}

const HISTORY_SUMMARY_STYLES = `
  .history-summary {
    margin-top: 1.5rem;
  }
  .history-summary table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.95rem;
  }
  .history-summary th,
  .history-summary td {
    border: 1px solid #555;
    padding: 6px 8px;
    vertical-align: top;
  }
  .history-summary th {
    background: #f2f2f2;
  }
  .history-summary td div + div {
    margin-top: 0.25rem;
  }
  .history-detail h2 {
    margin-top: 1.5rem;
  }
  .history-detail table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.95rem;
  }
  .history-detail th,
  .history-detail td {
    border: 1px solid #555;
    padding: 6px 8px;
    text-align: left;
  }
  .nowrap {
    white-space: nowrap;
  }
`;

function buildCompanyHeader(company = {}) {
  const hasContent = Boolean(
    company.name || company.headline || company.address || company.contactEmail
  );
  if (!hasContent) {
    return '';
  }
  return `
    <div class="print-meta">
      ${company.name ? `<h1>${escapeHtml(company.name)}</h1>` : ''}
      ${company.headline ? `<p>${escapeHtml(company.headline)}</p>` : ''}
      ${company.address ? `<p>${escapeHtml(company.address).replace(/\n/g, '<br />')}</p>` : ''}
      ${company.contactEmail ? `<p>${escapeHtml(company.contactEmail)}</p>` : ''}
    </div>
  `;
}

/**
 * Print selected entries using chunked printing
 */
async function printSummary(entries, labels) {
  if (!entries.length) {
    alert('Keine Einträge zum Drucken ausgewählt.');
    return;
  }
  
  const company = getState().company || {};
  const headerHtml = buildCompanyHeader(company);
  
  try {
    await printEntriesChunked(entries, labels, {
      title: 'Historie – Übersicht',
      headerHtml,
      chunkSize: 50
    });
  } catch (err) {
    console.error('Printing failed', err);
    alert('Fehler beim Drucken. Bitte erneut versuchen.');
  }
}

/**
 * Print single entry detail
 */
function printDetail(entry, labels) {
  if (!entry) {
    alert('Kein Eintrag zum Drucken vorhanden.');
    return;
  }
  const company = getState().company || {};
  const resolvedLabels = labels || getState().fieldLabels;
  const detailLabels = resolvedLabels.history.detail;
  const snapshotTable = buildMediumTableHTML(entry.items, resolvedLabels, 'detail', { classes: 'history-detail-table' });
  
  const content = `${buildCompanyHeader(company)}
    <section class="history-detail">
      <h2>${escapeHtml(detailLabels.title)} – ${escapeHtml(entry.datum || entry.date || '')}</h2>
      <p>
        <strong>${escapeHtml(detailLabels.creator)}:</strong> ${escapeHtml(entry.ersteller || '')}<br />
        <strong>${escapeHtml(detailLabels.location)}:</strong> ${escapeHtml(entry.standort || '')}<br />
        <strong>${escapeHtml(detailLabels.crop)}:</strong> ${escapeHtml(entry.kultur || '')}<br />
        <strong>${escapeHtml(detailLabels.quantity)}:</strong> ${escapeHtml(entry.kisten != null ? String(entry.kisten) : '')}
      </p>
      ${snapshotTable}
    </section>
  `;
  
  printHtml({
    title: `Historie – ${entry.datum || entry.date || ''}`,
    styles: HISTORY_SUMMARY_STYLES,
    content
  });
}

function updateSelectionUI(section) {
  const info = section.querySelector('[data-role="selection-info"]');
  const printButton = section.querySelector('[data-action="print-selected"]');
  if (info) {
    info.textContent = selectedIndexes.size
      ? `${selectedIndexes.size} Eintrag(e) ausgewählt.`
      : 'Keine Einträge ausgewählt.';
  }
  if (printButton) {
    printButton.disabled = !selectedIndexes.size;
  }
}

export function initHistory(container, services) {
  if (!container || initialized) {
    return;
  }
  const section = createSection();
  container.appendChild(section);

  function toggleVisibility(state) {
    const active = state.app.activeSection === 'history';
    const ready = state.app.hasDatabase;
    section.classList.toggle('d-none', !(active && ready));
  }

  services.state.subscribe((nextState) => {
    toggleVisibility(nextState);
    renderTable(nextState, section, nextState.fieldLabels);
    const detailCard = section.querySelector('#history-detail');
    if (detailCard && !detailCard.classList.contains('d-none')) {
      const detailIndex = Number(detailCard.dataset.index);
      if (!Number.isNaN(detailIndex) && nextState.history[detailIndex]) {
        renderDetail(nextState.history[detailIndex], section, detailIndex, nextState.fieldLabels);
      } else {
        renderDetail(null, section, null, nextState.fieldLabels);
      }
    }
    updateSelectionUI(section);
  });

  toggleVisibility(getState());
  renderTable(getState(), section, getState().fieldLabels);
  updateSelectionUI(section);

  section.addEventListener('click', event => {
    const action = event.target.dataset.action;
    if (!action) {
      return;
    }
    
    // Handle detail print
    if (action === 'detail-print') {
      const detailCard = event.target.closest('#history-detail');
      const indexAttr = detailCard ? detailCard.dataset.index : undefined;
      const index = typeof indexAttr === 'string' && indexAttr !== '' ? Number(indexAttr) : NaN;
      const state = getState();
      const entry = Number.isInteger(index) ? state.history[index] : null;
      printDetail(entry, state.fieldLabels);
      return;
    }
    
    // Handle print selected
    if (action === 'print-selected') {
      const state = getState();
      const entries = Array.from(selectedIndexes)
        .sort((a, b) => a - b)
        .map(idx => state.history[idx])
        .filter(Boolean);
      printSummary(entries, state.fieldLabels);
      return;
    }
    
    // Handle load more
    if (action === 'load-more') {
      const btn = event.target;
      const currentLimit = parseInt(btn.dataset.currentLimit, 10);
      const state = getState();
      const newLimit = Math.min(state.history.length, currentLimit + INITIAL_LOAD_LIMIT);
      
      const listContainer = section.querySelector('[data-role="history-list"]');
      const resolvedLabels = state.fieldLabels;
      
      // Render additional items
      const fragment = document.createDocumentFragment();
      for (let i = currentLimit; i < newLimit; i++) {
        const entry = state.history[i];
        const selected = selectedIndexes.has(i);
        const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
          showActions: true,
          includeCheckbox: true,
          index: i,
          selected
        });
        
        const wrapper = document.createElement('div');
        wrapper.innerHTML = cardHtml;
        fragment.appendChild(wrapper.firstElementChild);
      }
      
      // Remove the button and add new items
      btn.remove();
      listContainer.appendChild(fragment);
      
      // Add new button if more items remain
      if (newLimit < state.history.length) {
        const newBtn = document.createElement('button');
        newBtn.className = 'btn btn-secondary w-100 mt-3';
        newBtn.textContent = `Mehr laden (${state.history.length - newLimit} weitere)`;
        newBtn.dataset.action = 'load-more';
        newBtn.dataset.currentLimit = String(newLimit);
        listContainer.appendChild(newBtn);
      }
      
      return;
    }
    
    const index = Number(event.target.dataset.index);
    if (Number.isNaN(index)) {
      return;
    }
    const state = getState();
    
    if (action === 'view') {
      const entry = state.history[index];
      renderDetail(entry, section, index, state.fieldLabels);
    } else if (action === 'delete') {
      if (!confirm('Wirklich löschen?')) {
        return;
      }
      services.state.updateSlice('history', history => {
        const copy = [...history];
        copy.splice(index, 1);
        return copy;
      });
      selectedIndexes.clear();
      updateSelectionUI(section);
      renderDetail(null, section, null, state.fieldLabels);
      persistHistoryChanges().catch(err => {
        console.error('Persist delete history error', err);
      });
    }
  });

  section.addEventListener('change', event => {
    const action = event.target.dataset.action;
    if (action !== 'toggle-select') {
      return;
    }
    const index = Number(event.target.dataset.index);
    if (Number.isNaN(index)) {
      return;
    }
    
    // Incremental update: only modify this specific card
    const listContainer = section.querySelector('[data-role="history-list"]');
    if (event.target.checked) {
      selectedIndexes.add(index);
      updateCardSelection(listContainer, index, true);
    } else {
      selectedIndexes.delete(index);
      updateCardSelection(listContainer, index, false);
    }
    updateSelectionUI(section);
  });

  initialized = true;
}
