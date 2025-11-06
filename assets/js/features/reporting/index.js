import { getState } from '../../core/state.js';

let initialized = false;

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
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-dark table-bordered" id="report-table">
              <thead>
                <tr>
                  <th>Datum</th>
                  <th>Erstellt von</th>
                  <th>Standort</th>
                  <th>Kultur</th>
                  <th>Kisten</th>
                  <th>Mittel &amp; Gesamtmengen</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
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

function renderTable(section, entries) {
  const tbody = section.querySelector('#report-table tbody');
  tbody.innerHTML = '';
  entries.forEach(entry => {
    const row = document.createElement('tr');
    const mittelHtml = (entry.items || [])
      .map(item => `<div><strong>${item.name}:</strong> ${Number.parseFloat(item.total).toFixed(2)} ${item.unit}</div>`)
      .join('');
    row.innerHTML = `
      <td>${entry.datum || entry.date}</td>
      <td>${entry.ersteller}</td>
      <td>${entry.standort}</td>
      <td>${entry.kultur}</td>
      <td>${entry.kisten}</td>
      <td>${mittelHtml}</td>
    `;
    tbody.appendChild(row);
  });
}

function applyFilter(section, startDate, endDate) {
  const history = getState().history;
  if (!startDate || !endDate) {
    renderTable(section, history);
    return;
  }
  const filtered = history.filter(entry => {
    const isoDate = germanDateToIso(entry.datum || entry.date);
    if (!isoDate) {
      return false;
    }
    return isoDate >= startDate && isoDate <= endDate;
  });
  renderTable(section, filtered);
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
    applyFilter(section, start, end);
  });

  function toggle(state) {
    const ready = state.app.hasDatabase;
    const active = state.app.activeSection === 'report';
    section.classList.toggle('d-none', !(ready && active));
    if (active) {
      renderTable(section, state.history);
    }
  }

  toggle(getState());
  renderTable(section, getState().history);

  services.state.subscribe((nextState) => {
    toggle(nextState);
  });

  initialized = true;
}
