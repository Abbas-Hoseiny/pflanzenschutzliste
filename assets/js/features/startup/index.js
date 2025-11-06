import { createDatabase, openDatabase, setActiveDriver } from '../../core/storage/index.js';
import { applyDatabase, createInitialDatabase } from '../../core/database.js';
import { getState } from '../../core/state.js';

let initialized = false;

export function initStartup(container, services) {
  if (!container || initialized) {
    return;
  }

  const card = document.createElement('section');
  card.className = 'section-container' ;
  card.innerHTML = `
    <div class="section-inner">
      <div class="card card-dark">
        <div class="card-body text-center">
          <h2 class="mb-3">JSON-Datenbank auswählen</h2>
          <p class="mb-4">
            Wählen Sie, ob Sie eine neue Datenbank erstellen oder eine bestehende Datei verbinden möchten.
          </p>
          <div class="d-flex flex-column flex-md-row gap-3 justify-content-center">
            <button class="btn btn-success px-4" data-action="create">Neue Datenbank erstellen</button>
            <button class="btn btn-outline-light px-4" data-action="open">Bestehende Datenbank verbinden</button>
            <button class="btn btn-secondary px-4" data-action="useDefaults">Nur Defaults laden</button>
          </div>
          <p class="mt-3 text-muted mb-0">
            (Empfohlener Speicherort: derselbe Ordner wie diese Web-App)
          </p>
        </div>
      </div>
    </div>
  `;

  container.innerHTML = '';
  container.appendChild(card);

  async function handleCreate() {
    try {
      try {
        setActiveDriver('fileSystem');
      } catch (err) {
        alert('Dateisystemzugriff wird nicht unterstützt in diesem Browser.');
        return;
      }
      const initialData = createInitialDatabase();
      const result = await createDatabase(initialData);
      applyDatabase(result.data);
      services.events.emit('database:connected', { driver: 'active' });
    } catch (err) {
      console.error('Fehler beim Erstellen der Datenbank', err);
      alert(err.message || 'Erstellen der Datenbank fehlgeschlagen');
    }
  }

  async function handleOpen() {
    try {
      try {
        setActiveDriver('fileSystem');
      } catch (err) {
        alert('Dateisystemzugriff wird nicht unterstützt in diesem Browser.');
        return;
      }
      const result = await openDatabase();
      applyDatabase(result.data);
      services.events.emit('database:connected', { driver: 'active' });
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
      services.events.emit('database:connected', { driver: 'memory' });
    } catch (err) {
      console.error('Fehler beim Laden der Defaults', err);
      alert(err.message || 'Defaults konnten nicht geladen werden');
    }
  }

  card.addEventListener('click', event => {
    const action = event.target.dataset.action;
    if (!action) {
      return;
    }
    if (action === 'create') {
      handleCreate();
    } else if (action === 'open') {
      handleOpen();
    } else if (action === 'useDefaults') {
      handleUseDefaults();
    }
  });

  function toggleVisibility(state) {
    const shouldHide = state.app.hasDatabase;
    card.classList.toggle('d-none', shouldHide);
  }

  toggleVisibility(getState());
  services.state.subscribe((nextState) => toggleVisibility(nextState));

  initialized = true;
}
