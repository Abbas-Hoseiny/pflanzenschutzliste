# Compact History & Reporting Redesign

The goal is to replace the table-based history/reporting views with compact "calculation snapshot" cards that automatically pick up the customizable field labels from the calculation module. The same layout must drive both the on-screen display and the print/PDF exports.

## Scope

- `assets/js/features/history/index.js`
- `assets/js/features/reporting/index.js`
- `assets/css/components.css` (or dedicated SCSS/CSS for the new card layout)
- Any shared helpers that live under `assets/js/core/*`

## Desired UX

1. Each saved calculation renders as a **card**:
   - Header block showing creator/location/crop/date with the current labels (`state.fieldLabels.calculation.fields.*`).
   - Body block listing all mediums. This can be a slim 2–3 column layout or a small table; keep it compact.
   - Footer actions: buttons for "Ansehen", "Löschen", Print checkbox, etc. The existing functionality must survive.
2. **History view**
   - Cards replace the `<table>` under `#history-table`.
   - Selection still works (checkbox + highlighted card when selected).
   - Detail drawer (`#history-detail`) reuses the same snapshot markup or gets simplified to display the card again with larger type.
3. **Reporting view**
   - Same card renderer (loop over filtered entries).
   - Filter summary text remains at the top (no cards when empty).
4. **Printing**
   - Both history and reporting printouts use the card markup. No static column headers; labels come from state.
   - Company header stays as-is at the top; card borders should render well in grey-scale.

## Implementation Outline

1. **Shared helpers**
   - Create a pure function (e.g. `renderCalculationSnapshot(entry, labels, options)`) returning DOM or HTML string.
   - Inputs: `entry` (history object), `labels` (`state.fieldLabels`), optional flags (`compact`, `showActions`, `includeCheckbox`).
   - Output: markup string you can inject with `innerHTML`.
   - Handle value formatting (numbers → `formatNumber`), escaping HTML, and fallbacks (`'-'`).
   - Add a variation helper for print that returns plain HTML (no buttons, no checkboxes).

2. **History module**
   - Replace `renderTable` with a renderer that clears a card container (`section.querySelector('[data-role="history-list"]')`).
   - Each card:
     - Checkbox (retain `data-action="toggle-select"`).
     - Snapshot content.
     - Action buttons (view/delete) at the bottom.
     - Selected state toggles a class to highlight the card.
   - Update event delegation to work with the new markup (`closest('[data-action]')`).
   - Update `renderDetail` to reuse snapshot markup (maybe plus totals). Option: when "Ansehen" clicked, scroll to detail card and render snapshot with bigger typography.
   - Update print helpers (`buildSummaryTable`, `buildDetailSection`) to call the shared renderer (print mode) so layout stays consistent.

3. **Reporting module**
   - Similar changes: replace table with container of cards.
   - `renderTable` becomes something like `renderCards(section, entries, labels)`.
   - Print function uses shared print renderer.

4. **Styling**
   - Define `.calc-snapshot-card` in `assets/css/components.css`:
     - Border, padding, dark-mode friendly backgrounds.
     - Responsive layout (stack on mobile, two-column for medium header).
     - Print overrides to force white background, black text, 1px borders.
   - `.snapshot-mediums` for the list of mediums (use CSS grid or definition list).
   - `.snapshot-meta` for the header labels, align colon and values cleanly.
   - Add `.snapshot-actions`, `.snapshot-select` etc.

5. **State/Labels**
   - Use the same field labels as calculation form: `labels.calculation.fields.creator.label`, etc. for the header.
   - For the medium list, reuse `labels.calculation.tableColumns` for column titles (if you show table headings) or embed label text in each row.

6. **Testing / Validation**
   - Manual regression:
     - Load defaults, create new calculation, save, open history/reporting, verify labels match edited names.
     - Toggle selection, print selected, ensure PDF uses card layout.
     - Filter reporting entries by date; confirm only matching cards render.
     - Delete an entry; ensure card disappears and selection count updates.
   - Console: no errors or warnings.
   - Run `node --check` on modified JS modules.
   - (Optional) Write a quick DOM test harness if feasible; otherwise manual QA instructions suffice.

7. **Acceptance criteria**
   - No static strings like "Erstellt von" remain; everything resolves through labels/state.
   - History and reporting show identical card layout (only contextual differences like actions and filter info).
   - Printing history/reporting yields cards with clear borders and correct data; no leftover table headers.
   - All existing actions (select, delete, view, print, filter) still function.

## Notes for Coding Agent

- Keep helper functions in plain JS modules (no framework). Consider putting them under `assets/js/features/shared/` if the folder exists; otherwise create one.
- Use `escapeHtml` utilities already present (import if needed) to avoid XSS.
- Maintain backward compatibility with saved data structure (`entry.items` etc.).
- Treat `entry.datum` and `entry.date` interchangeably as currently handled.
- Remember to update event listeners to accommodate the new markup; watch for `closest()` usage.
- Add minimal inline comments only where behavior is not self-explanatory.
- Ask for clarification before changing unrelated modules.
