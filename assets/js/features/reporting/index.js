import { getState } from '../../core/state.js';
import { printHtml } from '../../core/print.js';
import { renderCalculationSnapshot, renderCalculationSnapshotForPrint } from '../shared/calculationSnapshot.js';

let initialized = false;
let currentEntries = [];
let activeFilter = null;

function createSection() {
  const section = document.createElement('section');
  section.className = 'section-container d-none';
  section.dataset.section = 'report';
  section.innerHTML = `
    <div class="section-inner">
      <h2 class="text-center mb-4">Auswertung nach Datum</h2>
      <div class="card card-dark no-print mb-4">
        <div class="card-body">
          <form id="report-filter" class="row g-3">
            <div class="col-md-4">
              <label class="form-label" for="report-start">Startdatum</label>
              <input type="date" class="form-control" id="report-start" name="report-start" required />
            </div>
            <div class="col-md-4">
              <label class="form-label" for="report-end">Enddatum</label>
              <input type="date" class="form-control" id="report-end" name="report-end" required />
            </div>
            <div class="col-md-4 d-flex align-items-end">
              <button class="btn btn-success w-100" type="submit">Anzeigen</button>
            </div>
          </form>
        </div>
      </div>
      <div class="card card-dark">
        <div class="card-header d-flex flex-column flex-lg-row justify-content-between align-items-lg-center gap-3 no-print">
          <div class="small text-muted" data-role="report-info">Alle Einträge</div>
          <button class="btn btn-outline-light btn-sm" data-action="print-report" disabled>Drucken</button>
        </div>
        <div class="card-body">
          <div data-role="report-list"></div>
        </div>
      </div>
    </div>
  `;
  return section;
}

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

function parseDate(value) {
  if (!value) {
    return null;
  }
  const parts = value.split('-');
  if (parts.length !== 3) {
    return null;
  }
  const [year, month, day] = parts.map(Number);
  return new Date(year, month - 1, day);
}

function germanDateToIso(value) {
  if (!value) {
    return null;
  }
  const parts = value.split('.');
  if (parts.length !== 3) {
    return null;
  }
  const [day, month, year] = parts.map(Number);
  return new Date(year, month - 1, day);
}

function renderTable(section, entries, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  const container = section.querySelector('[data-role="report-list"]');
  container.innerHTML = '';
  const info = section.querySelector('[data-role="report-info"]');
  const printButton = section.querySelector('[data-action="print-report"]');
  currentEntries = entries.slice();
  
  entries.forEach((entry, index) => {
    const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
      showActions: false,
      includeCheckbox: false
    });
    
    const wrapper = document.createElement('div');
    wrapper.innerHTML = cardHtml;
    container.appendChild(wrapper.firstElementChild);
  });
  
  if (entries.length === 0) {
    container.innerHTML = '<p class="text-muted text-center">Keine Einträge vorhanden</p>';
  }
  
  if (info) {
    info.textContent = describeFilter(entries.length, resolvedLabels);
  }
  if (printButton) {
    printButton.disabled = entries.length === 0;
  }
}

function describeFilter(entryCount, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  if (!activeFilter) {
    if (!entryCount) {
      return resolvedLabels.reporting.infoEmpty;
    }
    return `${resolvedLabels.reporting.infoAll} (${entryCount})`;
  }
  const { startLabel, endLabel } = activeFilter;
  const prefix = `${resolvedLabels.reporting.infoPrefix} ${startLabel} – ${endLabel}`;
  if (!entryCount) {
    return `${prefix} (${resolvedLabels.reporting.infoEmpty})`;
  }
  return `${prefix} (${entryCount})`;
}

function applyFilter(section, state, filter) {
  const source = state.history || [];
  if (!filter) {
    renderTable(section, source, state.fieldLabels);
    return;
  }
  const filtered = source.filter(entry => {
    const isoDate = germanDateToIso(entry.datum || entry.date);
    if (!isoDate) {
      return false;
    }
    return isoDate >= filter.start && isoDate <= filter.end;
  });
  renderTable(section, filtered, state.fieldLabels);
}

export function initReporting(container, services) {
  if (!container || initialized) {
    return;
  }
  const section = createSection();
  container.appendChild(section);

  const filterForm = section.querySelector('#report-filter');
  filterForm.addEventListener('submit', event => {
    event.preventDefault();
    const formData = new FormData(filterForm);
    const start = parseDate(formData.get('report-start'));
    const end = parseDate(formData.get('report-end'));
    if (!start || !end) {
      alert('Bitte gültige Daten auswählen!');
      return;
    }
    if (start > end) {
      alert('Das Startdatum muss vor dem Enddatum liegen.');
      return;
    }
    activeFilter = {
      start,
      end,
      startLabel: new Intl.DateTimeFormat('de-DE').format(start),
      endLabel: new Intl.DateTimeFormat('de-DE').format(end)
    };
    applyFilter(section, getState(), activeFilter);
  });

  function toggle(state) {
    const ready = state.app.hasDatabase;
    const active = state.app.activeSection === 'report';
    section.classList.toggle('d-none', !(ready && active));
    if (ready) {
      applyFilter(section, state, activeFilter);
    }
  }

  toggle(getState());

  services.state.subscribe((nextState) => {
    toggle(nextState);
  });

  section.addEventListener('click', event => {
    const trigger = event.target.closest('[data-action="print-report"]');
    if (!trigger) {
      return;
    }
    if (!currentEntries.length) {
      alert('Keine Daten für den Druck vorhanden.');
      return;
    }
    printReport(currentEntries, activeFilter, getState().fieldLabels);
  });

  initialized = true;
}

const REPORT_STYLES = `
  .report-summary {
    margin-top: 1.5rem;
  }
  .report-summary table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.95rem;
  }
  .report-summary th,
  .report-summary td {
    border: 1px solid #555;
    padding: 6px 8px;
    vertical-align: top;
  }
  .report-summary th {
    background: #f2f2f2;
  }
  .report-summary td div + div {
    margin-top: 0.25rem;
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

function buildFilterInfo(filter, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  const prefix = escapeHtml(resolvedLabels.reporting.infoPrefix);
  if (!filter) {
    return `<p>${prefix}: ${escapeHtml(resolvedLabels.reporting.infoAll)}</p>`;
  }
  return `<p>${prefix}: ${escapeHtml(filter.startLabel)} – ${escapeHtml(filter.endLabel)}</p>`;
}

function buildReportTable(entries, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  const printTitle = resolvedLabels.reporting.printTitle;
  
  const cards = entries
    .map(entry => renderCalculationSnapshotForPrint(entry, resolvedLabels))
    .join('');
  
  return `
    <section class="report-summary">
      <h2>${escapeHtml(printTitle)}</h2>
      ${cards}
    </section>
  `;
}

function printReport(entries, filter, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  const company = getState().company || {};
  const content = `${buildCompanyHeader(company)}${buildFilterInfo(filter, resolvedLabels)}${buildReportTable(entries, resolvedLabels)}`;
  printHtml({
    title: resolvedLabels.reporting.printTitle,
    styles: REPORT_STYLES,
    content
  });
}

function formatMediumLine(item = {}) {
  const name = escapeHtml(item.name || '');
  const unit = escapeHtml(item.unit || '');
  const total = formatNumber(item.total);
  if (!name) {
    return '-';
  }
  if (total === '-') {
    return `${name}: -`;
  }
  return `${name}: ${total} ${unit}`.trim();
}
