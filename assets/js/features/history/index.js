import { getState } from '../../core/state.js';
import { printHtml } from '../../core/print.js';
import { renderCalculationSnapshot, renderCalculationSnapshotForPrint } from '../shared/calculationSnapshot.js';

let initialized = false;
const selectedIndexes = new Set();

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function formatNumber(value) {
  const num = Number.parseFloat(value);
  if (Number.isNaN(num)) {
    return '-';
  }
  return num.toFixed(2);
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
          <div data-role="history-list"></div>
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

function renderTable(state, section, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  const container = section.querySelector('[data-role="history-list"]');
  container.innerHTML = '';
  
  for (const idx of Array.from(selectedIndexes)) {
    if (!state.history[idx]) {
      selectedIndexes.delete(idx);
    }
  }
  
  state.history.forEach((entry, index) => {
    const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
      showActions: true,
      includeCheckbox: true,
      index: index,
      selected: selectedIndexes.has(index)
    });
    
    const wrapper = document.createElement('div');
    wrapper.innerHTML = cardHtml;
    container.appendChild(wrapper.firstElementChild);
  });
  
  if (state.history.length === 0) {
    container.innerHTML = '<p class="text-muted text-center">Keine Einträge vorhanden</p>';
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
  
  const snapshotHtml = renderCalculationSnapshot(entry, resolvedLabels, {
    showActions: false,
    includeCheckbox: false
  });
  
  detailBody.innerHTML = `
    ${snapshotHtml}
    <button class="btn btn-outline-secondary no-print mt-3" data-action="detail-print">Drucken / PDF</button>
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

function mediumsList(items) {
  return (items || [])
    .map(item => `${escapeHtml(item.name)}: ${formatNumber(item.total)} ${escapeHtml(item.unit)}`)
    .join('<br />');
}

function buildSummaryTable(entries, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  const summaryTitle = resolvedLabels.history.summaryTitle;
  
  const cards = entries
    .map(entry => renderCalculationSnapshotForPrint(entry, resolvedLabels))
    .join('');
  
  return `
    <section class="history-summary">
      <h2>${escapeHtml(summaryTitle)}</h2>
      ${cards}
    </section>
  `;
}

function buildDetailSection(entry, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  const detailTitle = resolvedLabels.history.detail.title;
  const date = escapeHtml(entry.datum || entry.date || '');
  
  return `
    <section class="history-detail">
      <h2>${escapeHtml(detailTitle)} – ${date}</h2>
      ${renderCalculationSnapshotForPrint(entry, resolvedLabels)}
    </section>
  `;
}

function printSummary(entries, labels) {
  if (!entries.length) {
    alert('Keine Einträge zum Drucken ausgewählt.');
    return;
  }
  const company = getState().company || {};
  const content = `${buildCompanyHeader(company)}${buildSummaryTable(entries, labels)}`;
  printHtml({
    title: 'Historie – Übersicht',
    styles: HISTORY_SUMMARY_STYLES,
    content
  });
}

function printDetail(entry, labels) {
  if (!entry) {
    alert('Kein Eintrag zum Drucken vorhanden.');
    return;
  }
  const company = getState().company || {};
  const content = `${buildCompanyHeader(company)}${buildDetailSection(entry, labels)}`;
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
    if (action === 'detail-print') {
      const detailCard = event.target.closest('#history-detail');
      const indexAttr = detailCard ? detailCard.dataset.index : undefined;
      const index = typeof indexAttr === 'string' && indexAttr !== '' ? Number(indexAttr) : NaN;
      const state = getState();
      const entry = Number.isInteger(index) ? state.history[index] : null;
      printDetail(entry, state.fieldLabels);
      return;
    }
    if (action === 'print-selected') {
      const state = getState();
      const entries = Array.from(selectedIndexes)
        .sort((a, b) => a - b)
        .map(idx => state.history[idx])
        .filter(Boolean);
      printSummary(entries, state.fieldLabels);
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
    if (event.target.checked) {
      selectedIndexes.add(index);
      event.target.closest('.calc-snapshot-card')?.classList.add('calc-snapshot-card--selected');
    } else {
      selectedIndexes.delete(index);
      event.target.closest('.calc-snapshot-card')?.classList.remove('calc-snapshot-card--selected');
    }
    updateSelectionUI(section);
  });

  initialized = true;
}
