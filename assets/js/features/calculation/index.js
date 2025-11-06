import { getState } from '../../core/state.js';

let initialized = false;

function createSection() {
  const section = document.createElement('section');
  section.className = 'section-container d-none';
  section.dataset.section = 'calc';
  section.innerHTML = `
    <div class="section-inner">
      <div class="card card-dark mb-4">
        <div class="card-body">
          <form id="calculationForm" class="row g-3 no-print">
            <div class="col-md-3">
              <label for="calc-ersteller" class="form-label">Erstellt von</label>
              <input type="text" class="form-control" id="calc-ersteller" name="calc-ersteller" required />
            </div>
            <div class="col-md-3">
              <label for="calc-standort" class="form-label">Standort / Abteil</label>
              <input type="text" class="form-control" id="calc-standort" name="calc-standort" />
            </div>
            <div class="col-md-3">
              <label for="calc-kultur" class="form-label">Kultur</label>
              <input type="text" class="form-control" id="calc-kultur" name="calc-kultur" placeholder="z. B. Kohl, Salat" />
            </div>
            <div class="col-md-3">
              <label for="calc-kisten" class="form-label">Anzahl Kisten</label>
              <input type="number" min="0" step="any" class="form-control" id="calc-kisten" name="calc-kisten" required />
            </div>
            <div class="col-12 text-center">
              <button type="submit" class="btn btn-success px-4">Berechnen</button>
            </div>
          </form>
        </div>
      </div>
      <div id="calc-result" class="card card-dark d-none">
        <div class="card-header bg-success text-white">
          <h5 class="mb-0">Benötigte Mittel</h5>
        </div>
        <div class="card-body">
          <div class="row g-3 mb-3">
            <div class="col-md-6">
              <p class="mb-1"><strong>Erstellt von:</strong> <span data-field="ersteller"></span></p>
              <p class="mb-1"><strong>Standort / Abteil:</strong> <span data-field="standort"></span></p>
              <p class="mb-1"><strong>Kultur:</strong> <span data-field="kultur"></span></p>
            </div>
            <div class="col-md-6 text-md-end">
              <p class="mb-1"><strong>Datum:</strong> <span data-field="datum"></span></p>
              <p class="mb-1"><strong>Kisten:</strong> <span data-field="kisten"></span></p>
                <p class="mb-1"><strong>Gesamtwasser (L):</strong> <span data-field="wasser"></span></p>
                <p class="mb-1"><strong>Fläche (Ar / m²):</strong> <span data-field="flaeche"></span></p>
            </div>
          </div>
          <div class="table-responsive">
            <table class="table table-dark table-striped align-middle" id="calc-results-table">
              <thead>
                <tr>
                  <th>Mittel</th>
                  <th>Einheit</th>
                  <th>Methode</th>
                  <th>Wert</th>
                    <th>Kisten</th>
                    <th>Ar</th>
                    <th>m²</th>
                    <th>Gesamt</th>
                </tr>
              </thead>
              <tbody></tbody>
            </table>
          </div>
          <div class="mt-3 no-print">
            <button class="btn btn-outline-secondary" data-action="print">Drucken / PDF</button>
            <button class="btn btn-primary ms-2" data-action="save">Aktuelle Berechnung speichern</button>
          </div>
        </div>
      </div>
    </div>
  `;
  return section;
}

function formatNumber(value, fractionDigits = 2) {
  const num = Number.parseFloat(value);
  if (Number.isNaN(num)) {
    return (0).toFixed(fractionDigits);
  }
  return num.toFixed(fractionDigits);
}

function getWaterVolume(kisten, defaults) {
  const arValue = kisten / (defaults.kistenProAr || 1);
  return arValue * (defaults.waterPerKisteL || 0);
}

function executeFormula(medium, method, inputs, defaults) {
  if (!method) {
    return 0;
  }
  const value = Number(medium.value) || 0;
  switch (method.type) {
    case 'factor': {
      const base = inputs[method.config?.sourceField || 'kisten'] || 0;
      return value * base;
    }
    case 'percentOf': {
      const base = inputs[method.config?.baseField || 'waterVolume'] || 0;
      return (base * value) / 100;
    }
    case 'fixed':
      return value;
    default:
      console.warn('Unbekannter Methodentyp', method.type);
      return value;
  }
}

export function initCalculation(container, services) {
  if (!container || initialized) {
    return;
  }

  const section = createSection();
  container.appendChild(section);

  const form = section.querySelector('#calculationForm');
  const resultCard = section.querySelector('#calc-result');
  const resultsBody = section.querySelector('#calc-results-table tbody');
  const fieldEls = {
    ersteller: resultCard.querySelector('[data-field="ersteller"]'),
    standort: resultCard.querySelector('[data-field="standort"]'),
    kultur: resultCard.querySelector('[data-field="kultur"]'),
    datum: resultCard.querySelector('[data-field="datum"]'),
    kisten: resultCard.querySelector('[data-field="kisten"]'),
     wasser: resultCard.querySelector('[data-field="wasser"]'),
     flaeche: resultCard.querySelector('[data-field="flaeche"]')
  };

  function toggleVisibility(state) {
    const active = state.app.activeSection === 'calc';
    const ready = state.app.hasDatabase;
    section.classList.toggle('d-none', !(active && ready));
  }

  toggleVisibility(getState());
  services.state.subscribe((nextState) => toggleVisibility(nextState));

  function renderResults(calculation) {
    if (!calculation) {
      resultCard.classList.add('d-none');
      resultsBody.innerHTML = '';
      return;
    }
    const { header, items } = calculation;
    fieldEls.ersteller.textContent = header.ersteller;
    fieldEls.standort.textContent = header.standort;
    fieldEls.kultur.textContent = header.kultur;
    fieldEls.datum.textContent = header.datum;
    fieldEls.kisten.textContent = header.kisten;
    fieldEls.wasser.textContent = formatNumber(header.waterVolume, 2);
      fieldEls.flaeche.textContent = `${formatNumber(header.areaAr, 2)} / ${formatNumber(header.areaSqm, 2)}`;

    resultsBody.innerHTML = '';
    items.forEach(item => {
      const row = document.createElement('tr');
      row.innerHTML = `
        <td>${item.name}</td>
        <td>${item.unit}</td>
        <td>${item.methodLabel}</td>
        <td>${formatNumber(item.value)}</td>
          <td>${item.inputs.kisten}</td>
          <td>${formatNumber(item.inputs.areaAr)}</td>
          <td>${formatNumber(item.inputs.areaSqm)}</td>
          <td>${formatNumber(item.total)} ${item.unit}</td>
      `;
      resultsBody.appendChild(row);
    });
    resultCard.classList.remove('d-none');
  }

  services.state.subscribe((nextState) => renderResults(nextState.calcContext));

  form.addEventListener('submit', event => {
    event.preventDefault();
    const formData = new FormData(form);
    const ersteller = (formData.get('calc-ersteller') || '').toString().trim();
    const standort = (formData.get('calc-standort') || '').toString().trim() || '-';
    const kultur = (formData.get('calc-kultur') || '').toString().trim() || '-';
    const kisten = Number(formData.get('calc-kisten'));
    if (!ersteller || Number.isNaN(kisten)) {
      alert('Bitte Felder korrekt ausfüllen!');
      return;
    }

    const state = getState();
    const defaults = state.defaults;
    const measurementMethods = state.measurementMethods;
    const waterVolume = getWaterVolume(kisten, defaults);
    const areaAr = defaults.kistenProAr ? kisten / defaults.kistenProAr : 0;
    const areaSqm = areaAr * 100;

    const inputs = {
      kisten,
      waterVolume,
      areaAr,
      areaSqm
    };

    const items = state.mediums.map(medium => {
      const method = measurementMethods.find(m => m.id === medium.methodId);
      const total = executeFormula(medium, method, inputs, defaults);
      return {
        id: medium.id,
        name: medium.name,
        unit: medium.unit,
        methodLabel: method ? method.label : medium.methodId,
        methodId: medium.methodId,
        value: medium.value,
        total,
        inputs
      };
    });

    const header = {
      ersteller,
      standort,
      kultur,
      kisten,
      datum: new Date().toLocaleDateString('de-DE', { year: 'numeric', month: '2-digit', day: '2-digit' }),
  waterVolume,
  areaAr,
  areaSqm
    };

    const calculation = {
      header,
      items
    };

    services.state.updateSlice('calcContext', () => calculation);
  });

  resultCard.addEventListener('click', event => {
    const action = event.target.dataset.action;
    if (!action) {
      return;
    }
    if (action === 'print') {
      window.print();
    } else if (action === 'save') {
      const calc = getState().calcContext;
      if (!calc) {
        alert('Keine Berechnung vorhanden.');
        return;
      }
      services.state.updateSlice('history', history => {
        const entry = {
          ...calc.header,
          items: calc.items,
          savedAt: new Date().toISOString()
        };
        return [...history, entry];
      });
      alert('Berechnung gespeichert! (Siehe Historie)');
    }
  });

  renderResults(getState().calcContext);

  initialized = true;
}
