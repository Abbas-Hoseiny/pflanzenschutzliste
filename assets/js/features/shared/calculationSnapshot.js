/**
 * Unified rendering for calculation snapshot cards.
 * Provides consistent display for history entries in both UI and print modes.
 */

import { escapeHtml, formatNumber } from '../../core/utils.js';
import { buildMediumTableHTML } from './mediumTable.js';

/**
 * Renders a calculation snapshot entry as an HTML card.
 * 
 * @param {Object} entry - The history entry to render
 * @param {Object} labels - Field labels from state
 * @param {Object} options - Rendering options
 * @param {boolean} [options.showActions=false] - Whether to show action buttons
 * @param {boolean} [options.includeCheckbox=false] - Whether to include selection checkbox
 * @param {number} [options.index] - Entry index (required for checkbox/actions)
 * @param {boolean} [options.selected=false] - Whether the entry is selected
 * @returns {string} - HTML string for the card
 */
export function renderCalculationSnapshot(entry, labels, options = {}) {
  const {
    showActions = false,
    includeCheckbox = false,
    index,
    selected = false
  } = options;

  const tableLabels = labels?.history?.tableColumns || {};
  const detailLabels = labels?.history?.detail || {};

  const selectedClass = selected ? ' calc-snapshot-card--selected' : '';
  const dataIndex = typeof index === 'number' ? ` data-index="${index}"` : '';

  const checkboxHtml = includeCheckbox && typeof index === 'number'
    ? `<div class="calc-snapshot-card__checkbox no-print">
         <input type="checkbox" 
                class="form-check-input" 
                data-action="toggle-select" 
                data-index="${index}" 
                ${selected ? 'checked' : ''} 
                aria-label="Eintrag auswählen" />
       </div>`
    : '';

  const actionsHtml = showActions && typeof index === 'number'
    ? `<div class="calc-snapshot-card__actions no-print">
         <button class="btn btn-sm btn-info" 
                 data-action="view" 
                 data-index="${index}">
           Ansehen
         </button>
         <button class="btn btn-sm btn-danger" 
                 data-action="delete" 
                 data-index="${index}">
           Löschen
         </button>
       </div>`
    : '';

  const mediumTable = buildMediumTableHTML(entry.items || [], labels, 'summary', {
    classes: 'calc-snapshot-table'
  });

  return `
    <div class="calc-snapshot-card${selectedClass}"${dataIndex}>
      ${checkboxHtml}
      <div class="calc-snapshot-card__header">
        <div class="calc-snapshot-card__meta">
          <div class="calc-snapshot-card__date">
            <strong>${escapeHtml(tableLabels.date || 'Datum')}:</strong>
            ${escapeHtml(entry.datum || entry.date || '–')}
          </div>
          <div class="calc-snapshot-card__creator">
            <strong>${escapeHtml(detailLabels.creator || tableLabels.creator || 'Erstellt von')}:</strong>
            ${escapeHtml(entry.ersteller || '–')}
          </div>
        </div>
      </div>
      <div class="calc-snapshot-card__body">
        <div class="calc-snapshot-card__info">
          <div class="calc-snapshot-card__info-item">
            <strong>${escapeHtml(detailLabels.location || tableLabels.location || 'Standort')}:</strong>
            ${escapeHtml(entry.standort || '–')}
          </div>
          <div class="calc-snapshot-card__info-item">
            <strong>${escapeHtml(detailLabels.crop || tableLabels.crop || 'Kultur')}:</strong>
            ${escapeHtml(entry.kultur || '–')}
          </div>
          <div class="calc-snapshot-card__info-item">
            <strong>${escapeHtml(detailLabels.quantity || tableLabels.quantity || 'Kisten')}:</strong>
            ${escapeHtml(entry.kisten != null ? String(entry.kisten) : '–')}
          </div>
        </div>
        <div class="calc-snapshot-card__mediums">
          ${mediumTable}
        </div>
      </div>
      ${actionsHtml}
    </div>
  `;
}

/**
 * Renders a calculation snapshot entry for printing (simplified layout).
 * 
 * @param {Object} entry - The history entry to render
 * @param {Object} labels - Field labels from state
 * @returns {string} - HTML string for printing
 */
export function renderCalculationSnapshotForPrint(entry, labels) {
  const tableLabels = labels?.history?.tableColumns || {};
  const detailLabels = labels?.history?.detail || {};

  const mediumTable = buildMediumTableHTML(entry.items || [], labels, 'detail', {
    classes: 'history-detail-table'
  });

  return `
    <div class="calc-snapshot-print">
      <div class="calc-snapshot-print__header">
        <h3>${escapeHtml(detailLabels.title || 'Details')} – ${escapeHtml(entry.datum || entry.date || '')}</h3>
      </div>
      <div class="calc-snapshot-print__meta">
        <p>
          <strong>${escapeHtml(detailLabels.creator || 'Erstellt von')}:</strong> 
          ${escapeHtml(entry.ersteller || '–')}<br />
          <strong>${escapeHtml(detailLabels.location || 'Standort')}:</strong> 
          ${escapeHtml(entry.standort || '–')}<br />
          <strong>${escapeHtml(detailLabels.crop || 'Kultur')}:</strong> 
          ${escapeHtml(entry.kultur || '–')}<br />
          <strong>${escapeHtml(detailLabels.quantity || 'Kisten')}:</strong> 
          ${escapeHtml(entry.kisten != null ? String(entry.kisten) : '–')}
        </p>
      </div>
      <div class="calc-snapshot-print__mediums">
        ${mediumTable}
      </div>
    </div>
  `;
}
