/**
 * Chunked printing service for large datasets.
 * Renders print content in blocks to avoid memory issues.
 */

import { nextFrame } from '../../core/utils.js';
import { renderCalculationSnapshotForPrint } from './calculationSnapshot.js';

const BASE_FONT_STACK = '"Helvetica Neue", Helvetica, Arial, sans-serif';

/**
 * Base styles for print documents
 */
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

/**
 * Creates an offscreen iframe for printing
 */
function createPrintFrame() {
  const iframe = document.createElement('iframe');
  iframe.style.position = 'fixed';
  iframe.style.top = '-9999px';
  iframe.style.left = '-9999px';
  iframe.style.width = '0';
  iframe.style.height = '0';
  iframe.style.border = '0';
  document.body.appendChild(iframe);
  return iframe;
}

/**
 * Removes the iframe and cleans up references
 */
function cleanupPrintFrame(iframe) {
  if (iframe && iframe.parentNode) {
    iframe.parentNode.removeChild(iframe);
  }
}

/**
 * Prints entries in chunks to avoid memory issues.
 * 
 * @param {Array} entries - Array of history entries to print
 * @param {Object} labels - Field labels from state
 * @param {Object} options - Configuration options
 * @param {number} [options.chunkSize=50] - Number of entries per chunk
 * @param {string} [options.title='Druck'] - Document title
 * @param {string} [options.headerHtml=''] - Optional HTML to prepend (e.g., company header)
 * @param {string} [options.additionalStyles=''] - Additional CSS styles
 * @returns {Promise<void>}
 */
export async function printEntriesChunked(entries, labels, options = {}) {
  const {
    chunkSize = 50,
    title = 'Druck',
    headerHtml = '',
    additionalStyles = ''
  } = options;

  if (!Array.isArray(entries) || entries.length === 0) {
    throw new Error('No entries to print');
  }

  // Create offscreen iframe
  const iframe = createPrintFrame();
  const doc = iframe.contentDocument || iframe.contentWindow.document;

  // Write initial HTML structure
  const safeTitle = String(title).replace(/[<>]/g, '');
  const styles = PRINT_BASE_STYLES + (additionalStyles || '');
  
  doc.open();
  doc.write(`<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="utf-8" />
  <title>${safeTitle}</title>
  <style>${styles}</style>
</head>
<body>`);

  // Add header if provided
  if (headerHtml) {
    doc.write(headerHtml);
  }

  // Create container for entries
  const container = doc.createElement('div');
  container.className = 'calc-snapshots-container';
  doc.body.appendChild(container);

  // Render entries in chunks
  const totalChunks = Math.ceil(entries.length / chunkSize);
  
  for (let chunkIndex = 0; chunkIndex < totalChunks; chunkIndex++) {
    const start = chunkIndex * chunkSize;
    const end = Math.min(start + chunkSize, entries.length);
    const chunk = entries.slice(start, end);

    // Create a document fragment for this chunk
    const fragment = doc.createDocumentFragment();
    
    for (const entry of chunk) {
      const html = renderCalculationSnapshotForPrint(entry, labels);
      const wrapper = doc.createElement('div');
      wrapper.innerHTML = html;
      
      // Append all children from the wrapper to the fragment
      while (wrapper.firstChild) {
        fragment.appendChild(wrapper.firstChild);
      }
    }

    // Append the chunk to the container
    container.appendChild(fragment);

    // Yield to the browser between chunks
    if (chunkIndex < totalChunks - 1) {
      await nextFrame();
    }
  }

  // Close the document
  doc.write('</body></html>');
  doc.close();

  // Wait for content to be ready
  await new Promise(resolve => {
    if (iframe.contentWindow.document.readyState === 'complete') {
      resolve();
    } else {
      iframe.contentWindow.addEventListener('load', resolve, { once: true });
    }
  });

  // Small delay to ensure rendering is complete
  await new Promise(resolve => setTimeout(resolve, 100));

  // Trigger print
  try {
    iframe.contentWindow.focus();
    iframe.contentWindow.print();
  } catch (err) {
    console.error('Print failed', err);
    throw err;
  }

  // Cleanup after print dialog closes
  const cleanup = () => {
    cleanupPrintFrame(iframe);
    // Help GC by nullifying large references
    entries = null;
    labels = null;
  };

  // Listen for afterprint event
  if (iframe.contentWindow.addEventListener) {
    iframe.contentWindow.addEventListener('afterprint', cleanup, { once: true });
  }

  // Fallback cleanup after delay
  setTimeout(cleanup, 5000);
}
