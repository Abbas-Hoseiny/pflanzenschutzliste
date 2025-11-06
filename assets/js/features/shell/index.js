const SECTION_MAP = [
  { id: 'calc', label: 'Berechnung' },
  { id: 'history', label: 'Historie' },
  { id: 'settings', label: 'Einstellungen' },
  { id: 'report', label: 'Auswertung' }
];

let initialized = false;

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
      <a class="navbar-brand" href="#">
        <img class="brand-logo" alt="Firmalogo" />
        <span class="brand-title h4 mb-0"></span>
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

  function updateShell(state) {
    const { company, app } = state;
    if (company.logoUrl) {
      brandLogo.src = company.logoUrl;
      brandLogo.classList.remove('d-none');
    } else {
      brandLogo.classList.add('d-none');
    }
    brandLogo.alt = company.name || 'Logo';
    brandTitle.textContent = company.name || 'Bio-Pflanzenschutz';

    nav.style.setProperty('--accent-color', company.accentColor || '');

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
  services.state.subscribe((nextState) => updateShell(nextState));

  shell.innerHTML = '';
  shell.appendChild(nav);

  if (footer) {
    footer.innerHTML = '';
    const footerEl = document.createElement('div');
    footerEl.className = 'site-footer no-print';
    footerEl.innerHTML = `
      <div class="container">
        <p class="mb-0">
          Entwickler: <a class="footer-link" href="#"></a>
        </p>
      </div>
    `;
    const footerLink = footerEl.querySelector('.footer-link');

    function updateFooter(state) {
      footerLink.textContent = state.company.contactEmail || '';
      footerLink.href = state.company.contactEmail ? `mailto:${state.company.contactEmail}` : '#';
    }

    updateFooter(services.state.getState());
  services.state.subscribe((nextState) => updateFooter(nextState));
    footer.appendChild(footerEl);
  }

  initialized = true;
}
