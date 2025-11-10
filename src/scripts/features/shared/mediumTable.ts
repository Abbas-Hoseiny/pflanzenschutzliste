import { escapeHtml, formatNumber } from "@scripts/core/utils";

const COLUMN_FALLBACK_LABELS = {
  medium: "Mittel",
  unit: "Einheit",
  method: "Methode",
  value: "Wert",
  perQuantity: "Kisten",
  areaAr: "Ar",
  areaSqm: "m²",
  total: "Gesamt",
} as const;

type VariantKey = "calculation" | "detail" | "summary";

type VariantConfig = {
  columns: Array<keyof typeof COLUMN_FALLBACK_LABELS>;
  numberFallback: string;
  missingValue: string;
};

const VARIANT_CONFIG: Record<VariantKey, VariantConfig> = {
  calculation: {
    columns: [
      "medium",
      "unit",
      "method",
      "value",
      "perQuantity",
      "areaAr",
      "areaSqm",
      "total",
    ],
    numberFallback: "0.00",
    missingValue: "-",
  },
  detail: {
    columns: ["medium", "unit", "method", "value", "total"],
    numberFallback: "-",
    missingValue: "-",
  },
  summary: {
    columns: ["medium", "total"],
    numberFallback: "-",
    missingValue: "-",
  },
};

function resolveVariant(variant: VariantKey): VariantConfig {
  return VARIANT_CONFIG[variant] || VARIANT_CONFIG.calculation;
}

function resolveLabel(
  labels: any,
  key: keyof typeof COLUMN_FALLBACK_LABELS
): string {
  const label = labels?.calculation?.tableColumns?.[key];
  return escapeHtml(label || COLUMN_FALLBACK_LABELS[key] || key);
}

type ColumnRenderer = {
  cell: (item: any, config: VariantConfig) => string;
  className?: string;
};

const COLUMN_DEFS: Record<keyof typeof COLUMN_FALLBACK_LABELS, ColumnRenderer> =
  {
    medium: {
      cell: (item, config) => {
        const value = item?.name;
        return value ? escapeHtml(value) : config.missingValue;
      },
    },
    unit: {
      cell: (item, config) => {
        const value = item?.unit;
        return value ? escapeHtml(value) : config.missingValue;
      },
      className: "nowrap",
    },
    method: {
      cell: (item, config) => {
        const value = item?.methodLabel || item?.methodId;
        return value ? escapeHtml(value) : config.missingValue;
      },
    },
    value: {
      cell: (item, config) =>
        formatNumber(item?.value, 2, config.numberFallback),
    },
    perQuantity: {
      cell: (item, config) => {
        const raw = item?.inputs?.kisten;
        if (raw === null || raw === undefined) {
          return config.missingValue;
        }
        return escapeHtml(String(raw));
      },
      className: "nowrap",
    },
    areaAr: {
      cell: (item, config) =>
        formatNumber(item?.inputs?.areaAr, 2, config.numberFallback),
    },
    areaSqm: {
      cell: (item, config) =>
        formatNumber(item?.inputs?.areaSqm, 2, config.numberFallback),
    },
    total: {
      cell: (item, config) => {
        const total = formatNumber(item?.total, 2, config.numberFallback);
        if (total === config.numberFallback) {
          return config.numberFallback;
        }
        const unit = item?.unit ? ` ${escapeHtml(item.unit)}` : "";
        return `${total}${unit}`;
      },
      className: "nowrap",
    },
  };

function renderHeadCells(
  columns: Array<keyof typeof COLUMN_FALLBACK_LABELS>,
  labels: any
): string {
  return columns
    .map((columnKey) => `<th>${resolveLabel(labels, columnKey)}</th>`)
    .join("");
}

function renderRowCells(
  item: any,
  columns: Array<keyof typeof COLUMN_FALLBACK_LABELS>,
  config: VariantConfig
): string {
  return columns
    .map((columnKey) => {
      const column = COLUMN_DEFS[columnKey];
      if (!column) {
        return "<td>-</td>";
      }
      const value = column.cell(item, config);
      const className = column.className ? ` class="${column.className}"` : "";
      return `<td${className}>${value}</td>`;
    })
    .join("");
}

export function buildMediumTableHead(
  labels: any,
  variant: VariantKey = "calculation"
): string {
  const config = resolveVariant(variant);
  return `<tr>${renderHeadCells(config.columns, labels)}</tr>`;
}

export function buildMediumTableRows(
  items: any[] = [],
  variant: VariantKey = "calculation"
): string {
  const config = resolveVariant(variant);
  if (!Array.isArray(items) || !items.length) {
    return "";
  }
  return items
    .map((item) => `<tr>${renderRowCells(item, config.columns, config)}</tr>`)
    .join("");
}

export function buildMediumTableHTML(
  items: any[] = [],
  labels: any,
  variant: VariantKey = "calculation",
  options: { classes?: string } = {}
): string {
  const {
    classes = "table table-dark table-striped align-middle calc-medium-table",
  } = options;
  const head = buildMediumTableHead(labels, variant);
  const body = buildMediumTableRows(items, variant);
  return `<table class="${classes}"><thead>${head}</thead><tbody>${body}</tbody></table>`;
}

export function buildMediumSummaryLines(items: any[] = []): string[] {
  if (!Array.isArray(items) || !items.length) {
    return [];
  }
  const config = resolveVariant("summary");
  return items.map((item) => {
    const name = item?.name ? escapeHtml(item.name) : config.missingValue;
    if (!item || !item.name) {
      return name;
    }
    const total = formatNumber(item.total, 2, config.numberFallback);
    if (total === config.numberFallback) {
      return `${name}: ${config.numberFallback}`;
    }
    const unit = item?.unit ? ` ${escapeHtml(item.unit)}` : "";
    return `${name}: ${total}${unit}`;
  });
}
