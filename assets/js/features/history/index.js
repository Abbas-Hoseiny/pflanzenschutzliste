import { getState } from '../../core/state.js';
import { printHtml } from '../../core/print.js';
import { saveDatabase, getActiveDriverKey } from '../../core/storage/index.js';
import { getDatabaseSnapshot } from '../../core/database.js';
import { buildMediumTableHTML, buildMediumSummaryLines } from '../shared/mediumTable.js';

let initialized = false;
const selectedIndexes = new Set();

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

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
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
          <div class="table-responsive">
            <table class="table table-dark table-bordered align-middle" id="history-table">
              <thead>
                <tr>
                  <th class="no-print text-center" style="width: 3rem;">Auswahl</th>
                  <th>Datum</th>
                  <th>Erstellt von</th>
                  <th>Standort</th>
                  <th>Kultur</th>
                  <th>Kisten</th>
                  <th>Aktion</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
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
  const tbody = section.querySelector('#history-table tbody');
  const thead = section.querySelector('#history-table thead');
  tbody.innerHTML = '';
  if (thead) {
    thead.innerHTML = `
      <tr>
        <th class="no-print text-center" style="width: 3rem;">Auswahl</th>
        <th>${escapeHtml(resolvedLabels.history.tableColumns.date)}</th>
        <th>${escapeHtml(resolvedLabels.history.tableColumns.creator)}</th>
        <th>${escapeHtml(resolvedLabels.history.tableColumns.location)}</th>
        <th>${escapeHtml(resolvedLabels.history.tableColumns.crop)}</th>
        <th>${escapeHtml(resolvedLabels.history.tableColumns.quantity)}</th>
        <th>Aktion</th>
      </tr>
    `;
  }
  for (const idx of Array.from(selectedIndexes)) {
    if (!state.history[idx]) {
      selectedIndexes.delete(idx);
    }
  }
  state.history.forEach((entry, index) => {
    const row = document.createElement('tr');
    if (selectedIndexes.has(index)) {
      row.classList.add('table-active');
    }
    row.innerHTML = `
      <td class="no-print text-center">
        <input type="checkbox" class="form-check-input" data-action="toggle-select" data-index="${index}" ${selectedIndexes.has(index) ? 'checked' : ''} />
      </td>
      <td>${escapeHtml(entry.datum || entry.date || '')}</td>
      <td>${escapeHtml(entry.ersteller || '')}</td>
      <td>${escapeHtml(entry.standort || '')}</td>
      <td>${escapeHtml(entry.kultur || '')}</td>
      <td>${escapeHtml(entry.kisten != null ? String(entry.kisten) : '')}</td>
      <td>
        <button class="btn btn-sm btn-info" data-action="view" data-index="${index}">Ansehen</button>
        <button class="btn btn-sm btn-danger" data-action="delete" data-index="${index}">Löschen</button>
      </td>
    `;
    tbody.appendChild(row);
  });
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

function buildSummaryTable(entries, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  const tableLabels = resolvedLabels.history.tableColumns;
  const summaryTitle = resolvedLabels.history.summaryTitle;
  const mediumsHeading = resolvedLabels.history.mediumsHeading;
  const renderMediumSummary = (items) => {
    const lines = buildMediumSummaryLines(items, resolvedLabels);
    if (!lines.length) {
      return '-';
    }
    return lines.map(line => `<div>${line}</div>`).join('');
  };
  const rows = entries
    .map(entry => `
      <tr>
        <td class="nowrap">${escapeHtml(entry.datum || entry.date || '')}</td>
        <td>${escapeHtml(entry.ersteller || '')}</td>
        <td>${escapeHtml(entry.standort || '')}</td>
        <td>${escapeHtml(entry.kultur || '')}</td>
        <td class="nowrap">${escapeHtml(entry.kisten != null ? String(entry.kisten) : '')}</td>
        <td>${renderMediumSummary(entry.items)}</td>
      </tr>
    `)
    .join('');
  return `
    <section class="history-summary">
      <h2>${escapeHtml(summaryTitle)}</h2>
      <table>
        <thead>
          <tr>
            <th>${escapeHtml(tableLabels.date)}</th>
            <th>${escapeHtml(tableLabels.creator)}</th>
            <th>${escapeHtml(tableLabels.location)}</th>
            <th>${escapeHtml(tableLabels.crop)}</th>
            <th>${escapeHtml(tableLabels.quantity)}</th>
            <th>${escapeHtml(mediumsHeading)}</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </section>
  `;
}

function buildDetailSection(entry, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  const detailLabels = resolvedLabels.history.detail;
  const snapshotTable = buildMediumTableHTML(entry.items, resolvedLabels, 'detail', { classes: 'history-detail-table' });
  return `
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
    if (event.target.checked) {
      selectedIndexes.add(index);
      event.target.closest('tr')?.classList.add('table-active');
    } else {
      selectedIndexes.delete(index);
      event.target.closest('tr')?.classList.remove('table-active');
    }
    updateSelectionUI(section);
  });

  initialized = true;
}
