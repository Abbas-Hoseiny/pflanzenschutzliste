import { getState } from '../../core/state.js';
import { printHtml } from '../../core/print.js';
import { buildMediumSummaryLines } from '../shared/mediumTable.js';
import { escapeHtml } from '../../core/utils.js';
import { initVirtualList } from '../../core/virtualList.js';
import { renderCalculationSnapshot } from '../shared/calculationSnapshot.js';
import { printEntriesChunked } from '../shared/printing.js';

let initialized = false;
let currentEntries = [];
let activeFilter = null;
let virtualListInstance = null;

// Feature flag for virtual scrolling
const USE_VIRTUAL_SCROLLING = true;
const INITIAL_LOAD_LIMIT = 200;

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
          <div data-role="report-list" class="report-list"></div>
        </div>
      </div>
    </div>
  `;
  return section;
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

/**
 * Render cards list with virtual scrolling support
 */
function renderCardsList(listContainer, entries, labels) {
  const resolvedLabels = labels || getState().fieldLabels;

  if (USE_VIRTUAL_SCROLLING && entries.length > INITIAL_LOAD_LIMIT) {
    // Use virtual scrolling for large lists
    if (!virtualListInstance) {
      virtualListInstance = initVirtualList(listContainer, {
        itemCount: entries.length,
        estimatedItemHeight: 200,
        overscan: 6,
        renderItem: (node, index) => {
          const entry = entries[index];
          node.innerHTML = renderCalculationSnapshot(entry, resolvedLabels, {
            showActions: false,
            includeCheckbox: false
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
      const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
        showActions: false,
        includeCheckbox: false
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

function renderTable(section, entries, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  const listContainer = section.querySelector('[data-role="report-list"]');
  const info = section.querySelector('[data-role="report-info"]');
  const printButton = section.querySelector('[data-action="print-report"]');
  
  currentEntries = entries.slice();

  if (listContainer) {
    renderCardsList(listContainer, entries, resolvedLabels);
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
    // Handle load more
    if (event.target.dataset.action === 'load-more') {
      const btn = event.target;
      const currentLimit = parseInt(btn.dataset.currentLimit, 10);
      const newLimit = Math.min(currentEntries.length, currentLimit + INITIAL_LOAD_LIMIT);
      
      const listContainer = section.querySelector('[data-role="report-list"]');
      const resolvedLabels = getState().fieldLabels;
      
      // Render additional items
      const fragment = document.createDocumentFragment();
      for (let i = currentLimit; i < newLimit; i++) {
        const entry = currentEntries[i];
        const cardHtml = renderCalculationSnapshot(entry, resolvedLabels, {
          showActions: false,
          includeCheckbox: false
        });
        
        const wrapper = document.createElement('div');
        wrapper.innerHTML = cardHtml;
        fragment.appendChild(wrapper.firstElementChild);
      }
      
      // Remove the button and add new items
      btn.remove();
      listContainer.appendChild(fragment);
      
      // Add new button if more items remain
      if (newLimit < currentEntries.length) {
        const newBtn = document.createElement('button');
        newBtn.className = 'btn btn-secondary w-100 mt-3';
        newBtn.textContent = `Mehr laden (${currentEntries.length - newLimit} weitere)`;
        newBtn.dataset.action = 'load-more';
        newBtn.dataset.currentLimit = String(newLimit);
        listContainer.appendChild(newBtn);
      }
      
      return;
    }
    
    // Handle print report
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

async function printReport(entries, filter, labels) {
  const resolvedLabels = labels || getState().fieldLabels;
  const company = getState().company || {};
  const headerHtml = buildCompanyHeader(company) + buildFilterInfo(filter, resolvedLabels);
  
  try {
    await printEntriesChunked(entries, resolvedLabels, {
      title: resolvedLabels.reporting.printTitle,
      headerHtml,
      chunkSize: 50
    });
  } catch (err) {
    console.error('Printing failed', err);
    alert('Fehler beim Drucken. Bitte erneut versuchen.');
  }
}
