import { getState } from '../../core/state.js';

let initialized = false;

function createSection() {
  const section = document.createElement('section');
  section.className = 'section-container d-none';
  section.dataset.section = 'history';
  section.innerHTML = `
    <div class="section-inner">
      <h2 class="text-center mb-4">Historie – Frühere Einträge</h2>
      <div class="card card-dark">
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-dark table-bordered align-middle" id="history-table">
              <thead>
                <tr>
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

function renderTable(state, section) {
  const tbody = section.querySelector('#history-table tbody');
  tbody.innerHTML = '';
  state.history.forEach((entry, index) => {
    const row = document.createElement('tr');
    row.innerHTML = `
      <td>${entry.datum || entry.date}</td>
      <td>${entry.ersteller}</td>
      <td>${entry.standort}</td>
      <td>${entry.kultur}</td>
      <td>${entry.kisten}</td>
      <td>
        <button class="btn btn-sm btn-info" data-action="view" data-index="${index}">Ansehen</button>
        <button class="btn btn-sm btn-danger" data-action="delete" data-index="${index}">Löschen</button>
      </td>
    `;
    tbody.appendChild(row);
  });
}

function renderDetail(entry, section) {
  const detailCard = section.querySelector('#history-detail');
  const detailBody = section.querySelector('#history-detail-body');
  if (!entry) {
    detailCard.classList.add('d-none');
    detailBody.innerHTML = '';
    return;
  }
  detailBody.innerHTML = `
    <p>
      <strong>Datum:</strong> ${entry.datum || entry.date}<br />
      <strong>Erstellt von:</strong> ${entry.ersteller}<br />
      <strong>Standort / Abteil:</strong> ${entry.standort}<br />
      <strong>Kultur:</strong> ${entry.kultur}<br />
      <strong>Kisten:</strong> ${entry.kisten}
    </p>
    <div class="table-responsive">
      <table class="table table-dark table-striped">
        <thead>
          <tr>
            <th>Mittel</th>
            <th>Einheit</th>
            <th>Methode</th>
            <th>Wert</th>
            <th>Gesamt</th>
          </tr>
        </thead>
        <tbody>
          ${(entry.items || [])
            .map(item => `
              <tr>
                <td>${item.name}</td>
                <td>${item.unit}</td>
                <td>${item.methodLabel || item.methodId}</td>
                <td>${Number.parseFloat(item.value).toFixed(2)}</td>
                <td>${Number.parseFloat(item.total).toFixed(2)} ${item.unit}</td>
              </tr>
            `)
            .join('')}
        </tbody>
      </table>
    </div>
    <button class="btn btn-outline-secondary no-print" data-action="detail-print">Drucken / PDF</button>
  `;
  detailCard.classList.remove('d-none');
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
    renderTable(nextState, section);
  });

  toggleVisibility(getState());
  renderTable(getState(), section);

  section.addEventListener('click', event => {
    const action = event.target.dataset.action;
    if (!action) {
      return;
    }
    if (action === 'detail-print') {
      window.print();
      return;
    }
    const index = Number(event.target.dataset.index);
    if (Number.isNaN(index)) {
      return;
    }
    const state = getState();
    if (action === 'view') {
      const entry = state.history[index];
      renderDetail(entry, section);
    } else if (action === 'delete') {
      if (!confirm('Wirklich löschen?')) {
        return;
      }
      services.state.updateSlice('history', history => {
        const copy = [...history];
        copy.splice(index, 1);
        return copy;
      });
      renderDetail(null, section);
    }
  });

  initialized = true;
}
