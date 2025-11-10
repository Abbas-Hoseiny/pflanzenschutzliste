import { nextFrame } from "@scripts/core/utils";
import {
  renderCalculationSnapshotForPrint,
  type CalculationSnapshotEntry,
  type CalculationSnapshotLabels,
} from "./calculationSnapshot";

const BASE_FONT_STACK = '"Helvetica Neue", Helvetica, Arial, sans-serif';

const PRINT_BASE_STYLES = `
  @page { size: A4; margin: 18mm; }
  body {
    font-family: ${BASE_FONT_STACK};
    color: #111;
    line-height: 1.45;
    margin: 0;
    padding: 0;
  }
  h1, h2, h3 { margin: 0 0 0.75rem; }
  table {
    width: 100%;
    border-collapse: collapse;
    margin-bottom: 1rem;
  }
  th, td {
    border: 1px solid #555;
    padding: 6px 8px;
    text-align: left;
    vertical-align: top;
  }
  th {
    background: #f2f2f2;
    font-weight: 600;
  }
  .nowrap { white-space: nowrap; }
  .print-meta { margin-bottom: 1.5rem; }
  .print-meta p { margin: 0; }
  .print-meta h1 { margin-bottom: 0.5rem; }
  .calc-snapshot-print {
    page-break-inside: avoid;
    margin-bottom: 2rem;
    border-bottom: 2px solid #ddd;
    padding-bottom: 1rem;
  }
  .calc-snapshot-print:last-child {
    border-bottom: none;
  }
  .calc-snapshot-print__header h3 {
    margin: 0 0 0.5rem;
    font-size: 1.1rem;
  }
  .calc-snapshot-print__meta {
    margin-bottom: 1rem;
  }
  .calc-snapshot-print__meta p {
    margin: 0;
    font-size: 0.9rem;
  }
  .calc-snapshot-table,
  .history-detail-table {
    font-size: 0.9rem;
  }
`;

function createPrintFrame(): HTMLIFrameElement {
  const iframe = document.createElement("iframe");
  iframe.style.position = "fixed";
  iframe.style.top = "-9999px";
  iframe.style.left = "-9999px";
  iframe.style.width = "0";
  iframe.style.height = "0";
  iframe.style.border = "0";
  document.body.appendChild(iframe);
  return iframe;
}

function cleanupPrintFrame(iframe: HTMLIFrameElement | null): void {
  if (iframe?.parentNode) {
    iframe.parentNode.removeChild(iframe);
  }
}

type PrintOptions = {
  chunkSize?: number;
  title?: string;
  headerHtml?: string;
  additionalStyles?: string;
};

export async function printEntriesChunked(
  entries: CalculationSnapshotEntry[],
  labels: CalculationSnapshotLabels,
  options: PrintOptions = {}
): Promise<void> {
  const {
    chunkSize = 50,
    title = "Druck",
    headerHtml = "",
    additionalStyles = "",
  } = options;

  if (!Array.isArray(entries) || entries.length === 0) {
    throw new Error("No entries to print");
  }

  const iframe = createPrintFrame();
  const doc = iframe.contentDocument || iframe.contentWindow?.document;
  if (!doc) {
    cleanupPrintFrame(iframe);
    throw new Error("Print-Frame konnte nicht initialisiert werden");
  }

  const safeTitle = String(title).replace(/[<>]/g, "");
  const styles = PRINT_BASE_STYLES + (additionalStyles || "");

  doc.open();
  doc.write(`<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <title>${safeTitle}</title>
  <style>${styles}</style>
</head>
<body>`);

  if (headerHtml) {
    doc.write(headerHtml);
  }

  const container = doc.createElement("div");
  container.className = "calc-snapshots-container";
  doc.body.appendChild(container);

  const totalChunks = Math.ceil(entries.length / chunkSize);

  for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex += 1) {
    const start = chunkIndex * chunkSize;
    const end = Math.min(start + chunkSize, entries.length);
    const chunk = entries.slice(start, end);

    const fragment = doc.createDocumentFragment();

    for (const entry of chunk) {
      const html = renderCalculationSnapshotForPrint(entry, labels);
      const wrapper = doc.createElement("div");
      wrapper.innerHTML = html;
      while (wrapper.firstChild) {
        fragment.appendChild(wrapper.firstChild);
      }
    }

    container.appendChild(fragment);

    if (chunkIndex < totalChunks - 1) {
      await nextFrame();
    }
  }

  doc.write("</body></html>");
  doc.close();

  await new Promise<void>((resolve) => {
    const frameWindow = iframe.contentWindow;
    if (!frameWindow) {
      resolve();
      return;
    }
    if (frameWindow.document.readyState === "complete") {
      resolve();
    } else {
      frameWindow.addEventListener("load", () => resolve(), { once: true });
    }
  });

  await new Promise<void>((resolve) => setTimeout(resolve, 100));

  try {
    iframe.contentWindow?.focus();
    iframe.contentWindow?.print();
  } catch (err) {
    console.error("Print failed", err);
    cleanupPrintFrame(iframe);
    throw err;
  }

  const frameWindow = iframe.contentWindow;
  if (frameWindow && "addEventListener" in frameWindow) {
    frameWindow.addEventListener(
      "afterprint",
      () => cleanupPrintFrame(iframe),
      { once: true }
    );
  }

  setTimeout(() => cleanupPrintFrame(iframe), 5000);
}
