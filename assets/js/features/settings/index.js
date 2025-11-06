import { getState } from '../../core/state.js';
import { saveDatabase } from '../../core/storage/index.js';
import { getDatabaseSnapshot } from '../../core/database.js';

let initialized = false;

function createSection() {
  const section = document.createElement('section');
  section.className = 'section-container d-none';
  section.dataset.section = 'settings';
  section.innerHTML = `
    <div class="section-inner">
      <h2 class="text-center mb-4">Einstellungen</h2>
      <div class="tab-nav no-print">
        <button class="btn btn-outline-light" data-tab="company">Unternehmen</button>
        <button class="btn btn-outline-light" data-tab="mediums">Mittel</button>
        <button class="btn btn-outline-light" data-tab="methods">Berechnungsmethoden</button>
      </div>
      <div class="tab-content">
        <div class="tab-pane" data-pane="company"></div>
        <div class="tab-pane d-none" data-pane="mediums"></div>
        <div class="tab-pane d-none" data-pane="methods"></div>
      </div>
      <div class="mt-4 no-print">
        <button class="btn btn-success" data-action="persist">Änderungen speichern</button>
      </div>
    </div>
  `;
  return section;
}

function renderCompanyPane(pane, state, services) {
  pane.innerHTML = `
    <div class="card card-dark">
      <div class="card-body">
        <form id="company-form" class="row g-3">
          <div class="col-md-6">
            <label class="form-label" for="company-name">Firmenname</label>
            <input class="form-control" id="company-name" name="company-name" value="${state.company.name || ''}" />
          </div>
          <div class="col-md-6">
            <label class="form-label" for="company-email">Kontakt-E-Mail</label>
            <input class="form-control" id="company-email" name="company-email" value="${state.company.contactEmail || ''}" />
          </div>
          <div class="col-md-6">
            <label class="form-label" for="company-logo">Logo-URL</label>
            <input class="form-control" id="company-logo" name="company-logo" value="${state.company.logoUrl || ''}" />
          </div>
          <div class="col-md-6">
            <label class="form-label" for="company-accent">Akzentfarbe (Hex)</label>
            <input class="form-control" id="company-accent" name="company-accent" value="${state.company.accentColor || ''}" />
          </div>
          <div class="col-12">
            <label class="form-label" for="company-address">Adresse</label>
            <textarea class="form-control" id="company-address" name="company-address" rows="2">${state.company.address || ''}</textarea>
          </div>
          <div class="col-12">
            <button class="btn btn-primary" type="submit">Speichern</button>
          </div>
        </form>
      </div>
    </div>
  `;

  const form = pane.querySelector('#company-form');
  form.addEventListener('submit', event => {
    event.preventDefault();
    const formData = new FormData(form);
    services.state.updateSlice('company', company => ({
      ...company,
      name: formData.get('company-name')?.toString() ?? '',
      contactEmail: formData.get('company-email')?.toString() ?? '',
      logoUrl: formData.get('company-logo')?.toString() ?? '',
      accentColor: formData.get('company-accent')?.toString() ?? '',
      address: formData.get('company-address')?.toString() ?? ''
    }));
    alert('Firmendaten aktualisiert');
  });
}

function renderMediumsPane(pane, state, services) {
  const methods = state.measurementMethods;
  pane.innerHTML = `
    <div class="card card-dark">
      <div class="card-body">
        <div class="table-responsive">
          <table class="table table-dark table-bordered" id="mediums-table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Einheit</th>
                <th>Methode</th>
                <th>Wert</th>
                <th>Aktion</th>
              </tr>
            </thead>
            <tbody>
              ${state.mediums
                .map((medium, index) => `
                  <tr data-index="${index}">
                    <td><input class="form-control" data-field="name" value="${medium.name}" /></td>
                    <td><input class="form-control" data-field="unit" value="${medium.unit}" /></td>
                    <td>
                      <select class="form-select" data-field="methodId">
                        ${methods
                          .map(method => `
                            <option value="${method.id}" ${method.id === medium.methodId ? 'selected' : ''}>${method.label}</option>
                          `)
                          .join('')}
                      </select>
                    </td>
                    <td><input type="number" step="any" class="form-control" data-field="value" value="${medium.value}" /></td>
                    <td><button class="btn btn-sm btn-danger" data-action="delete">Löschen</button></td>
                  </tr>
                `)
                .join('')}
            </tbody>
          </table>
        </div>
        <hr class="my-4" />
        <form id="medium-add-form" class="row g-3">
          <div class="col-md-3">
            <input class="form-control" name="medium-name" placeholder="Name" required />
          </div>
          <div class="col-md-2">
            <input class="form-control" name="medium-unit" placeholder="Einheit" required />
          </div>
          <div class="col-md-3">
            <select class="form-select" name="medium-method" required>
              <option value="" disabled selected>Methode wählen</option>
              ${methods.map(method => `<option value="${method.id}">${method.label}</option>`).join('')}
            </select>
          </div>
          <div class="col-md-2">
            <input type="number" step="any" class="form-control" name="medium-value" placeholder="Wert" required />
          </div>
          <div class="col-md-2">
            <button class="btn btn-success w-100" type="submit">Hinzufügen</button>
          </div>
        </form>
      </div>
    </div>
  `;

  pane.querySelector('#mediums-table tbody').addEventListener('input', event => {
    const target = event.target;
    if (!target.dataset.field) {
      return;
    }
    const row = target.closest('tr');
    const index = Number(row.dataset.index);
    if (Number.isNaN(index)) {
      return;
    }
    const field = target.dataset.field;
    const value = field === 'value' ? Number(target.value) : target.value;
    services.state.updateSlice('mediums', mediums => {
      const copy = [...mediums];
      copy[index] = { ...copy[index], [field]: value };
      return copy;
    });
  });

  pane.querySelector('#mediums-table tbody').addEventListener('click', event => {
    if (event.target.dataset.action !== 'delete') {
      return;
    }
    const row = event.target.closest('tr');
    const index = Number(row.dataset.index);
    if (Number.isNaN(index)) {
      return;
    }
    services.state.updateSlice('mediums', mediums => {
      const copy = [...mediums];
      copy.splice(index, 1);
      return copy;
    });
  });

  pane.querySelector('#medium-add-form').addEventListener('submit', event => {
    event.preventDefault();
    const formData = new FormData(event.currentTarget);
    const id = typeof crypto !== 'undefined' && crypto.randomUUID ? crypto.randomUUID() : `medium-${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const newMedium = {
      id,
      name: formData.get('medium-name')?.toString().trim() ?? '',
      unit: formData.get('medium-unit')?.toString().trim() ?? '',
      methodId: formData.get('medium-method')?.toString() ?? '',
      value: Number(formData.get('medium-value'))
    };
    services.state.updateSlice('mediums', mediums => [...mediums, newMedium]);
    event.currentTarget.reset();
  });
}

function renderMethodsPane(pane, state, services) {
  pane.innerHTML = `
    <div class="card card-dark">
      <div class="card-body">
        <p class="text-muted mb-0">Die Verwaltung der Berechnungsmethoden wird in einem späteren Schritt implementiert.</p>
      </div>
    </div>
  `;
}

function activateTab(section, tabId) {
  const tabs = section.querySelectorAll('[data-tab]');
  const panes = section.querySelectorAll('[data-pane]');
  tabs.forEach(btn => {
    if (btn.dataset.tab === tabId) {
      btn.classList.add('btn-success');
      btn.classList.remove('btn-outline-light');
    } else {
      btn.classList.remove('btn-success');
      btn.classList.add('btn-outline-light');
    }
  });
  panes.forEach(pane => {
    pane.classList.toggle('d-none', pane.dataset.pane !== tabId);
  });
}

async function persistChanges() {
  try {
    const snapshot = getDatabaseSnapshot();
    await saveDatabase(snapshot);
    alert('Änderungen wurden gespeichert.');
  } catch (err) {
    console.error('Fehler beim Speichern', err);
    alert(err.message || 'Speichern fehlgeschlagen');
  }
}

export function initSettings(container, services) {
  if (!container || initialized) {
    return;
  }
  const section = createSection();
  container.appendChild(section);

  const panes = {
    company: section.querySelector('[data-pane="company"]'),
    mediums: section.querySelector('[data-pane="mediums"]'),
    methods: section.querySelector('[data-pane="methods"]')
  };

  function render(state) {
    renderCompanyPane(panes.company, state, services);
    renderMediumsPane(panes.mediums, state, services);
    renderMethodsPane(panes.methods, state, services);
  }

  render(getState());

  function toggle(state) {
    const ready = state.app.hasDatabase;
    const active = state.app.activeSection === 'settings';
    section.classList.toggle('d-none', !(ready && active));
  }

  toggle(getState());

  section.querySelector('.tab-nav').addEventListener('click', event => {
    const tabId = event.target.dataset.tab;
    if (!tabId) {
      return;
    }
    activateTab(section, tabId);
  });
  activateTab(section, 'company');

  services.state.subscribe((nextState) => {
    if (nextState.app.activeSection === 'settings') {
      render(nextState);
    }
    toggle(nextState);
  });

  section.querySelector('[data-action="persist"]').addEventListener('click', () => {
    persistChanges();
  });

  initialized = true;
}
