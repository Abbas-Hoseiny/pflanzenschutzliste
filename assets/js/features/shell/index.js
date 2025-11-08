const SECTION_MAP = [
  { id: 'calc', label: 'Berechnung' },
  { id: 'history', label: 'Historie' },
  { id: 'settings', label: 'Einstellungen' },
  { id: 'report', label: 'Auswertung' },
  { id: 'zulassung', label: 'Zulassung' }
];

let initialized = false;
let printHooksInstalled = false;
let isPrinting = false;
let activeDocumentTitle = document.title || 'Pflanzenschutz';

function ensurePrintHooks() {
  if (printHooksInstalled) {
    return;
  }
  const beforePrint = () => {
    if (isPrinting) {
      return;
    }
    isPrinting = true;
    document.title = ' ';
  };
  const afterPrint = () => {
    if (!isPrinting) {
      return;
    }
    isPrinting = false;
    document.title = activeDocumentTitle;
  };
  window.addEventListener('beforeprint', beforePrint);
  window.addEventListener('afterprint', afterPrint);
  printHooksInstalled = true;
}

export function initShell(regions, services) {
  if (initialized) {
    return;
  }
  const { shell, footer } = regions;
  if (!shell) {
    console.warn('shell region fehlt');
    return;
  }

  const nav = document.createElement('nav');
  nav.className = 'navbar navbar-dark shell-navbar mb-4 no-print';
  nav.innerHTML = `
    <div class="container">
      <a class="navbar-brand d-flex align-items-center gap-2" href="#">
        <img class="brand-logo" alt="Firmalogo" />
        <div class="brand-copy d-flex flex-column">
          <span class="brand-title h4 mb-0"></span>
          <span class="brand-tagline small text-muted"></span>
        </div>
      </a>
      <div class="d-flex flex-wrap gap-2"></div>
    </div>
  `;
  const actionContainer = nav.querySelector('.d-flex');

  SECTION_MAP.forEach(section => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'btn btn-sm btn-outline-light nav-btn';
    button.dataset.section = section.id;
    button.textContent = section.label;
    button.addEventListener('click', evt => {
      evt.preventDefault();
      services.state.updateSlice('app', app => ({
        ...app,
        activeSection: section.id
      }));
    });
    actionContainer.appendChild(button);
  });

  const brandLogo = nav.querySelector('.brand-logo');
  const brandTitle = nav.querySelector('.brand-title');
  const brandTagline = nav.querySelector('.brand-tagline');

  function updateShell(state) {
    const { company, app } = state;
    if (company.logoUrl) {
      brandLogo.src = company.logoUrl;
      brandLogo.classList.remove('d-none');
    } else {
      brandLogo.classList.add('d-none');
    }
    brandLogo.alt = company.name || 'Logo';
    brandTitle.textContent = company.name || 'Pflanzenschutz';
    brandTagline.textContent = company.headline || '';
  brandTagline.classList.toggle('d-none', !company.headline);
    if (company.accentColor) {
      brandTagline.style.color = company.accentColor;
    } else {
      brandTagline.style.removeProperty('color');
    }

    nav.style.setProperty('--accent-color', company.accentColor || '');
    const nextTitle = company.name ? `${company.name} – Pflanzenschutz` : 'Pflanzenschutz';
    activeDocumentTitle = nextTitle;
    if (!isPrinting) {
      document.title = nextTitle;
    }

    const buttons = nav.querySelectorAll('[data-section]');
    buttons.forEach(btn => {
      if (btn.dataset.section === app.activeSection) {
        btn.classList.add('active');
        btn.classList.remove('btn-outline-light');
        btn.classList.add('btn-success');
      } else {
        btn.classList.remove('active');
        btn.classList.add('btn-outline-light');
        btn.classList.remove('btn-success');
      }
      btn.disabled = !app.hasDatabase;
    });
    nav.classList.toggle('d-none', !app.hasDatabase);
  }

  updateShell(services.state.getState());
  ensurePrintHooks();
  services.state.subscribe((nextState) => updateShell(nextState));

  shell.innerHTML = '';
  shell.appendChild(nav);

  if (footer) {
    footer.innerHTML = '';
    const footerEl = document.createElement('footer');
    footerEl.className = 'site-footer no-print';
    const currentYear = new Date().getFullYear();
    footerEl.innerHTML = `
      <div class="container d-flex justify-content-between align-items-center flex-wrap gap-2">
        <span class="footer-copy">&copy; ${currentYear} Pflanzenschutzliste</span>
        <a class="footer-link footer-link--icon" href="https://github.com/Abbas-Hoseiny/pflanzenschutzliste" target="_blank" rel="noopener" aria-label="GitHub Repository">
          <svg width="20" height="20" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true">
            <path d="M8 0C3.58 0 0 3.58 0 8a8 8 0 005.47 7.59c.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2 .37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82a7.62 7.62 0 012-.27c.68 0 1.36.09 2 .27 1.52-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.19 0 .21.15.46.55.38A8 8 0 0016 8c0-4.42-3.58-8-8-8z" />
          </svg>
        </a>
      </div>
    `;
    footer.appendChild(footerEl);
  }

  initialized = true;
}
