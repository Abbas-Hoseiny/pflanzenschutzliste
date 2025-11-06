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

/**
 * Renders a calculation snapshot as a card.
 * @param {Object} entry - History entry object with calculation data
 * @param {Object} labels - Field labels from state.fieldLabels
 * @param {Object} options - Rendering options
 * @param {boolean} options.compact - Use compact layout (default: false)
 * @param {boolean} options.showActions - Show action buttons (default: false)
 * @param {boolean} options.includeCheckbox - Include selection checkbox (default: false)
 * @param {number} options.index - Entry index for actions/selection
 * @param {boolean} options.selected - Whether the card is selected (default: false)
 * @param {boolean} options.forPrint - Optimize for print output (default: false)
 * @returns {string} HTML markup for the snapshot card
 */
export function renderCalculationSnapshot(entry, labels, options = {}) {
  const {
    compact = false,
    showActions = false,
    includeCheckbox = false,
    index = null,
    selected = false,
    forPrint = false
  } = options;

  const calcFields = labels.calculation.fields;
  const calcColumns = labels.calculation.tableColumns;
  
  const creator = escapeHtml(entry.ersteller || '');
  const location = escapeHtml(entry.standort || '');
  const crop = escapeHtml(entry.kultur || '');
  const date = escapeHtml(entry.datum || entry.date || '');
  const quantity = escapeHtml(entry.kisten != null ? String(entry.kisten) : '');
  
  const selectedClass = selected ? ' calc-snapshot-card--selected' : '';
  
  const checkboxHtml = includeCheckbox && !forPrint ? `
    <div class="snapshot-select no-print">
      <input type="checkbox" class="form-check-input" data-action="toggle-select" data-index="${index}" ${selected ? 'checked' : ''} />
    </div>
  ` : '';
  
  const mediumsHtml = (entry.items || [])
    .map(item => {
      const name = escapeHtml(item.name || '');
      const unit = escapeHtml(item.unit || '');
      const method = escapeHtml(item.methodLabel || item.methodId || '');
      const value = formatNumber(item.value);
      const total = formatNumber(item.total);
      const totalDisplay = total === '-' ? '-' : `${total} ${unit}`.trim();
      
      if (forPrint) {
        return `
          <tr>
            <td>${name}</td>
            <td>${unit}</td>
            <td>${method}</td>
            <td>${value}</td>
            <td>${totalDisplay}</td>
          </tr>
        `;
      }
      
      return `
        <div class="snapshot-medium-row">
          <span class="snapshot-medium-name">${name}</span>
          <span class="snapshot-medium-details">${method}: ${value}</span>
          <span class="snapshot-medium-total">${totalDisplay}</span>
        </div>
      `;
    })
    .join('');
  
  const mediumsContent = forPrint ? `
    <table class="snapshot-mediums-table">
      <thead>
        <tr>
          <th>${escapeHtml(calcColumns.medium)}</th>
          <th>${escapeHtml(calcColumns.unit)}</th>
          <th>${escapeHtml(calcColumns.method)}</th>
          <th>${escapeHtml(calcColumns.value)}</th>
          <th>${escapeHtml(calcColumns.total)}</th>
        </tr>
      </thead>
      <tbody>${mediumsHtml}</tbody>
    </table>
  ` : `
    <div class="snapshot-mediums">${mediumsHtml || '<div class="snapshot-medium-row text-muted">-</div>'}</div>
  `;
  
  const actionsHtml = showActions && !forPrint ? `
    <div class="snapshot-actions no-print">
      <button class="btn btn-sm btn-info" data-action="view" data-index="${index}">Ansehen</button>
      <button class="btn btn-sm btn-danger" data-action="delete" data-index="${index}">Löschen</button>
    </div>
  ` : '';
  
  return `
    <div class="calc-snapshot-card${selectedClass}" ${index !== null ? `data-index="${index}"` : ''}>
      ${checkboxHtml}
      <div class="snapshot-header">
        <div class="snapshot-meta-row">
          <span class="snapshot-meta-label">${escapeHtml(calcFields.creator.label)}:</span>
          <span class="snapshot-meta-value">${creator || '-'}</span>
        </div>
        <div class="snapshot-meta-row">
          <span class="snapshot-meta-label">${escapeHtml(calcFields.location.label)}:</span>
          <span class="snapshot-meta-value">${location || '-'}</span>
        </div>
        <div class="snapshot-meta-row">
          <span class="snapshot-meta-label">${escapeHtml(calcFields.crop.label)}:</span>
          <span class="snapshot-meta-value">${crop || '-'}</span>
        </div>
        <div class="snapshot-meta-row">
          <span class="snapshot-meta-label">${escapeHtml(labels.history.tableColumns.date)}:</span>
          <span class="snapshot-meta-value">${date || '-'}</span>
        </div>
        <div class="snapshot-meta-row">
          <span class="snapshot-meta-label">${escapeHtml(calcFields.quantity.label)}:</span>
          <span class="snapshot-meta-value">${quantity || '-'}</span>
        </div>
      </div>
      <div class="snapshot-body">
        ${mediumsContent}
      </div>
      ${actionsHtml}
    </div>
  `;
}

/**
 * Renders a calculation snapshot optimized for print output.
 * This is a convenience wrapper around renderCalculationSnapshot with forPrint=true.
 */
export function renderCalculationSnapshotForPrint(entry, labels) {
  return renderCalculationSnapshot(entry, labels, { forPrint: true });
}
