import { createDatabase, openDatabase, setActiveDriver, getActiveDriverKey } from '../../core/storage/index.js';
import { applyDatabase, createInitialDatabase } from '../../core/database.js';
import { getState } from '../../core/state.js';

let initialized = false;

const DOWNLOAD_FILENAME_FALLBACK = 'pflanzenschutz-datenbank.json';

function sanitizeFilename(name) {
  if (!name) {
    return DOWNLOAD_FILENAME_FALLBACK;
  }
  const slug = name.toString().toLowerCase().trim().replace(/[^a-z0-9]+/g, '-').replace(/^-+|-+$/g, '');
  return `${slug || 'pflanzenschutz-datenbank'}.json`;
}

async function withButtonBusy(button, task) {
  if (!button) {
    await task();
    return;
  }
  const originalText = button.textContent;
  button.disabled = true;
  button.dataset.busy = 'true';
  button.textContent = 'Bitte warten...';
  try {
    await task();
  } finally {
    button.disabled = false;
    button.dataset.busy = 'false';
    button.textContent = originalText;
  }
}

export function initStartup(container, services) {
  if (!container || initialized) {
    return;
  }

  let generatedDatabase = null;
  let generatedFilename = DOWNLOAD_FILENAME_FALLBACK;
  let activeView = 'landing';

  const stateSnapshot = getState();
  const baseCompany = stateSnapshot.company;
  let wizard = null;

  const landingSection = document.createElement('section');
  landingSection.className = 'section-container';
  
  // Check which storage driver is active
  const activeDriver = getActiveDriverKey();
  const usingSQLite = activeDriver === 'sqlite';
  const storageInfo = usingSQLite 
    ? 'Mit SQLite-WASM für optimale Performance bei großen Datenmengen.' 
    : 'Die erzeugte JSON-Datei kann später erneut geladen oder weitergegeben werden.';
  
  landingSection.innerHTML = `
    <div class="section-inner">
      <div class="card card-dark">
        <div class="card-body text-center">
          <h2 class="mb-3">Datenbank starten</h2>
          <p class="mb-4">
            Erstelle eine neue Datenbank mit deinen Stammdaten oder verbinde eine vorhandene Datei.
          </p>
          ${usingSQLite ? `
          <div class="alert alert-info text-start mb-4">
            <strong>🚀 SQLite-WASM aktiviert</strong><br>
            Ihre Daten werden in einer performanten SQLite-Datenbank gespeichert.
            Import und Export von JSON-Dateien wird weiterhin unterstützt.
          </div>
          ` : ''}
          <div class="d-flex flex-column flex-md-row gap-3 justify-content-center">
            <button class="btn btn-success px-4" data-action="start-wizard">Neue Datenbank erstellen</button>
            <button class="btn btn-outline-light px-4" data-action="open">Bestehende Datei verbinden</button>
            <button class="btn btn-secondary px-4" data-action="useDefaults">Defaults testen</button>
          </div>
          <p class="mt-3 text-muted mb-0 small">
            ${storageInfo}
          </p>
        </div>
      </div>
    </div>
  `;

  const escapeAttr = (value) => String(value ?? '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;');
  const escapeHtml = (value) => String(value ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;');

  wizard = (() => {
    const section = document.createElement('section');
    section.className = 'section-container d-none';
    section.innerHTML = `
      <div class="section-inner">
        <div class="card card-dark">
          <div class="card-body">
            <div class="d-flex justify-content-between align-items-start flex-wrap gap-3 mb-3">
              <div>
                <h2 class="mb-1">Neue Datenbank konfigurieren</h2>
                <p class="text-muted mb-0">Erfasse deine Eckdaten. Daraus entsteht eine JSON-Datei für den späteren Import.</p>
              </div>
              <button type="button" class="btn btn-outline-light" data-action="wizard-back">Zurück</button>
            </div>
            <form id="database-wizard-form" class="row g-3 text-start">
              <div class="col-md-6">
                <label class="form-label" for="wizard-company-name">Firmenname*</label>
                <input class="form-control" id="wizard-company-name" name="wizard-company-name" required value="${escapeAttr(baseCompany.name)}" />
              </div>
              <div class="col-md-6">
                <label class="form-label" for="wizard-company-headline">Überschrift / Claim</label>
                <input class="form-control" id="wizard-company-headline" name="wizard-company-headline" value="${escapeAttr(baseCompany.headline)}" />
              </div>
              <div class="col-md-6">
                <label class="form-label" for="wizard-company-logo">Logo-URL</label>
                <input class="form-control" id="wizard-company-logo" name="wizard-company-logo" placeholder="https://example.com/logo.png" value="${escapeAttr(baseCompany.logoUrl)}" />
              </div>
              <div class="col-md-6">
                <label class="form-label" for="wizard-company-email">Kontakt-E-Mail</label>
                <input class="form-control" id="wizard-company-email" name="wizard-company-email" value="${escapeAttr(baseCompany.contactEmail)}" />
              </div>
              <div class="col-12">
                <label class="form-label" for="wizard-company-address">Adresse</label>
                <textarea class="form-control" id="wizard-company-address" name="wizard-company-address" rows="2">${escapeHtml(baseCompany.address)}</textarea>
              </div>
              <div class="col-12 d-flex flex-column flex-md-row gap-3">
                <button type="submit" class="btn btn-success px-4">Datenbank erzeugen</button>
                <button type="button" class="btn btn-outline-light px-4" data-action="wizard-back">Abbrechen</button>
              </div>
            </form>
          </div>
        </div>
        <div class="card card-dark mt-4 d-none" data-role="wizard-result">
          <div class="card-body">
            <h3 class="h5 mb-3">Datenbank erstellt</h3>
            <p class="mb-2">Vorschlag für den Dateinamen: <code data-role="wizard-filename"></code></p>
            <div class="d-flex flex-column flex-lg-row gap-2 mb-3">
              <button type="button" class="btn btn-success" data-action="wizard-save">Datei speichern</button>
            </div>
            <p class="text-muted small mb-2" data-role="wizard-save-hint"></p>
            <p class="text-muted small mb-2">Vorschau der kompletten JSON-Struktur (scrollbar):</p>
            <pre class="bg-dark rounded p-3 small overflow-auto" style="max-height: 14rem;" data-role="wizard-preview"></pre>
          </div>
        </div>
      </div>
    `;

    const form = section.querySelector('#database-wizard-form');
    const resultCard = section.querySelector('[data-role="wizard-result"]');
    const preview = section.querySelector('[data-role="wizard-preview"]');
    const filenameLabel = section.querySelector('[data-role="wizard-filename"]');
    const saveHint = section.querySelector('[data-role="wizard-save-hint"]');
    const saveButton = section.querySelector('[data-action="wizard-save"]');

    return {
      section,
      form,
      resultCard,
      preview,
      filenameLabel,
      saveHint,
      saveButton,
      reset() {
        form.reset();
        resultCard.classList.add('d-none');
        preview.textContent = '';
        filenameLabel.textContent = '';
      }
    };
  })();

  container.innerHTML = '';
  container.appendChild(landingSection);
  container.appendChild(wizard.section);

  const fileSystemSupported = typeof window !== 'undefined' && typeof window.showSaveFilePicker === 'function';
  if (wizard.saveButton) {
    if (!fileSystemSupported) {
      wizard.saveButton.disabled = true;
      wizard.saveButton.textContent = 'Datei speichern (nicht verfügbar)';
      if (wizard.saveHint) {
        wizard.saveHint.textContent = 'Dieser Browser unterstützt keinen direkten Dateidialog. Bitte nutze einen Chromium-basierten Browser (z. B. Chrome, Edge) über HTTPS oder http://localhost.';
      }
    } else if (wizard.saveHint) {
      wizard.saveHint.textContent = 'Der Browser fragt nach einem Speicherort. Die erzeugte Datei kannst du später über "Bestehende Datei verbinden" erneut laden.';
    }
  }

  function updateViewForActive(state = services.state.getState()) {
    if (state.app.hasDatabase) {
      landingSection.classList.add('d-none');
      wizard.section.classList.add('d-none');
      return;
    }
    if (activeView === 'wizard') {
      landingSection.classList.add('d-none');
      wizard.section.classList.remove('d-none');
    } else {
      landingSection.classList.remove('d-none');
      wizard.section.classList.add('d-none');
    }
  }

  async function handleOpen() {
    try {
      setActiveDriver('fileSystem');
    } catch (err) {
      alert('Dateisystemzugriff wird nicht unterstützt in diesem Browser.');
      return;
    }
    try {
      const result = await openDatabase();
      applyDatabase(result.data);
      services.events.emit('database:connected', { driver: getActiveDriverKey() });
    } catch (err) {
      console.error('Fehler beim Öffnen der Datenbank', err);
      alert(err.message || 'Öffnen der Datenbank fehlgeschlagen');
    }
  }

  function handleUseDefaults() {
    try {
      try {
        setActiveDriver('fallback');
      } catch (err) {
        console.warn('Fallback-Speicher nicht verfügbar, nutze In-Memory', err);
      }
      const initialData = createInitialDatabase();
      applyDatabase(initialData);
      services.events.emit('database:connected', { driver: getActiveDriverKey() || 'memory' });
    } catch (err) {
      console.error('Fehler beim Laden der Defaults', err);
      alert(err.message || 'Defaults konnten nicht geladen werden');
    }
  }

  async function handleWizardSave(button) {
    if (!generatedDatabase) {
      alert('Bitte erst die Datenbank erzeugen.');
      return;
    }
    await withButtonBusy(button, async () => {
      try {
        setActiveDriver('fileSystem');
      } catch (err) {
        alert('Dateisystemzugriff wird nicht unterstützt in diesem Browser.');
        return;
      }
      try {
        const result = await createDatabase(generatedDatabase);
        applyDatabase(result.data);
        services.events.emit('database:connected', { driver: getActiveDriverKey() });
      } catch (err) {
        console.error('Fehler beim Speichern der Datenbank', err);
        alert(err.message || 'Die Datei konnte nicht gespeichert werden');
      }
    });
  }

  function handleWizardSubmit(event) {
    event.preventDefault();
    const formData = new FormData(wizard.form);
    const name = (formData.get('wizard-company-name') || '').toString().trim();
    if (!name) {
      alert('Bitte einen Firmennamen angeben.');
      return;
    }

    const headline = (formData.get('wizard-company-headline') || '').toString().trim();
    const logoUrl = (formData.get('wizard-company-logo') || '').toString().trim();
    const contactEmail = (formData.get('wizard-company-email') || '').toString().trim();
    const address = (formData.get('wizard-company-address') || '').toString().trim();

    const overrides = {
      meta: {
        company: {
          name,
          headline,
          logoUrl,
          contactEmail,
          address
        }
      }
    };

    generatedDatabase = createInitialDatabase(overrides);
    generatedFilename = sanitizeFilename(name);

    wizard.preview.textContent = JSON.stringify(generatedDatabase, null, 2);
    wizard.filenameLabel.textContent = generatedFilename;
    wizard.resultCard.classList.remove('d-none');
    wizard.resultCard.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  function showLanding() {
    activeView = 'landing';
    generatedDatabase = null;
    generatedFilename = DOWNLOAD_FILENAME_FALLBACK;
    wizard.reset();
    updateViewForActive();
  }

  function showWizard() {
    activeView = 'wizard';
    updateViewForActive();
  }

  landingSection.addEventListener('click', event => {
    const button = event.target.closest('button[data-action]');
    if (!button) {
      return;
    }
    const action = button.dataset.action;
    if (action === 'start-wizard') {
      showWizard();
      return;
    }
    if (action === 'open') {
      withButtonBusy(button, () => handleOpen());
    } else if (action === 'useDefaults') {
      withButtonBusy(button, async () => {
        handleUseDefaults();
      });
    }
  });

  wizard.form.addEventListener('submit', handleWizardSubmit);

  wizard.section.addEventListener('click', event => {
    const button = event.target.closest('[data-action]');
    if (!button) {
      return;
    }
    const action = button.dataset.action;
    if (action === 'wizard-back') {
      showLanding();
      return;
    }
    if (action === 'wizard-save') {
      handleWizardSave(button);
    }
  });

  updateViewForActive(getState());
  services.state.subscribe((nextState) => updateViewForActive(nextState));

  initialized = true;
}
