import { escapeHtml, formatNumber } from '../../core/utils.js';

const COLUMN_FALLBACK_LABELS = {
  medium: 'Mittel',
  unit: 'Einheit',
  method: 'Methode',
  value: 'Wert',
  perQuantity: 'Kisten',
  areaAr: 'Ar',
  areaSqm: 'm²',
  total: 'Gesamt'
};

const VARIANT_CONFIG = {
  calculation: {
    columns: ['medium', 'unit', 'method', 'value', 'perQuantity', 'areaAr', 'areaSqm', 'total'],
    numberFallback: '0.00',
    missingValue: '-'
  },
  detail: {
    columns: ['medium', 'unit', 'method', 'value', 'total'],
    numberFallback: '-',
    missingValue: '-'
  },
  summary: {
    columns: ['medium', 'total'],
    numberFallback: '-',
    missingValue: '-'
  }
};

function resolveVariant(variant) {
  return VARIANT_CONFIG[variant] || VARIANT_CONFIG.calculation;
}

function resolveLabel(labels, key) {
  const label = labels?.calculation?.tableColumns?.[key];
  return escapeHtml(label || COLUMN_FALLBACK_LABELS[key] || key);
}

const COLUMN_DEFS = {
  medium: {
    cell: (item, config) => {
      const value = item?.name;
      return value ? escapeHtml(value) : config.missingValue;
    }
  },
  unit: {
    cell: (item, config) => {
      const value = item?.unit;
      return value ? escapeHtml(value) : config.missingValue;
    },
    className: 'nowrap'
  },
  method: {
    cell: (item, config) => {
      const value = item?.methodLabel || item?.methodId;
      return value ? escapeHtml(value) : config.missingValue;
    }
  },
  value: {
    cell: (item, config) => formatNumber(item?.value, 2, config.numberFallback)
  },
  perQuantity: {
    cell: (item, config) => {
      const raw = item?.inputs?.kisten;
      if (raw === null || raw === undefined) {
        return config.missingValue;
      }
      return escapeHtml(String(raw));
    },
    className: 'nowrap'
  },
  areaAr: {
    cell: (item, config) => formatNumber(item?.inputs?.areaAr, 2, config.numberFallback)
  },
  areaSqm: {
    cell: (item, config) => formatNumber(item?.inputs?.areaSqm, 2, config.numberFallback)
  },
  total: {
    cell: (item, config) => {
      const total = formatNumber(item?.total, 2, config.numberFallback);
      if (total === config.numberFallback) {
        return config.numberFallback;
      }
      const unit = item?.unit ? ` ${escapeHtml(item.unit)}` : '';
      return `${total}${unit}`;
    },
    className: 'nowrap'
  }
};

function renderHeadCells(columns, labels) {
  return columns
    .map(columnKey => `<th>${resolveLabel(labels, columnKey)}</th>`)
    .join('');
}

function renderRowCells(item, columns, config) {
  return columns
    .map(columnKey => {
      const column = COLUMN_DEFS[columnKey];
      if (!column) {
        return '<td>-</td>';
      }
      const value = column.cell(item, config);
      const className = column.className ? ` class="${column.className}"` : '';
      return `<td${className}>${value}</td>`;
    })
    .join('');
}

export function buildMediumTableHead(labels, variant = 'calculation') {
  const config = resolveVariant(variant);
  return `<tr>${renderHeadCells(config.columns, labels)}</tr>`;
}

export function buildMediumTableRows(items = [], labels, variant = 'calculation') {
  const config = resolveVariant(variant);
  if (!Array.isArray(items) || !items.length) {
    return '';
  }
  return items
    .map(item => `<tr>${renderRowCells(item, config.columns, config)}</tr>`)
    .join('');
}

export function buildMediumTableHTML(items = [], labels, variant = 'calculation', options = {}) {
  const { classes = 'table table-dark table-striped align-middle calc-medium-table' } = options;
  const head = buildMediumTableHead(labels, variant);
  const body = buildMediumTableRows(items, labels, variant);
  return `<table class="${classes}"><thead>${head}</thead><tbody>${body}</tbody></table>`;
}

export function buildMediumSummaryLines(items = [], labels) {
  if (!Array.isArray(items) || !items.length) {
    return [];
  }
  const config = resolveVariant('summary');
  return items.map(item => {
    const name = item?.name ? escapeHtml(item.name) : config.missingValue;
    if (!item || !item.name) {
      return name;
    }
    const total = formatNumber(item.total, 2, config.numberFallback);
    if (total === config.numberFallback) {
      return `${name}: ${config.numberFallback}`;
    }
    const unit = item?.unit ? ` ${escapeHtml(item.unit)}` : '';
    return `${name}: ${total}${unit}`;
  });
}
