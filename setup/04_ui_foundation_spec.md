# TiDB PoV Copilot UI Foundation Spec

## Layout Options (Shortlist)

### Option A: Tri-Panel Workbench (Recommended)
- Left rail for global navigation and saved views.
- Central workspace for conversation, code/test workflow, and outputs.
- Right context panel for run status, active config, and tool controls.
- Best for long sessions: stable navigation, high signal, predictable scanning.

### Option B: Top Nav + Split Workspace
- Top navigation for all sections.
- Main split view with conversation and tool output.
- Context tools in bottom drawer.
- Cleaner visually, but slower for heavy navigation and less state visibility.

### Option C: Two-Column Focus Mode
- Left combined nav + context drawer.
- Right main content only.
- Minimal chrome, but context switching causes more toggling and hidden state.

## Chosen Direction
**Option A: Tri-Panel Workbench.**
Reason: strongest hierarchy and lowest cognitive load for power users who stay in the tool for long periods. It keeps global orientation (left), primary work (center), and operational context (right) visible simultaneously.

---

## 1) Information Architecture

### Primary Areas
1. Global Navigation (left rail)
- Workbench
- Runs
- Reports
- Config
- Security
- History

2. Main Workspace (center)
- Conversation/workstream thread
- Active workflow tabs (Chat, Plan, Diff, Results)
- Composer + action row
- Contextual output modules (tables, logs, code blocks)

3. Context and Tools (right panel)
- Session status (connected/running/errors)
- Quick actions (run, validate, generate report)
- Active environment summary
- Tool toggles and diagnostics

4. Global Layer
- Command bar (Cmd/Ctrl+K)
- Toast notifications
- Modal dialog for destructive or high-impact actions

### Panel Behavior
- Left rail: fixed width on desktop, collapsible to icon rail.
- Right panel: fixed width on desktop, collapsible via toggle.
- Main workspace: fluid width, never below readable line length.

---

## 2) Component Inventory

### Navigation
- `AppNav`
- `NavSection`
- `NavItem` (default, active, keyboard-focused)

### Surfaces and Containers
- `Panel`
- `Card`
- `SubtleDivider`
- `SectionHeader`

### Actions
- `Button` variants: primary, secondary, ghost, danger
- `IconButton`
- `Toggle`

### Inputs
- `TextInput`
- `Select`
- `Textarea`
- `Checkbox`
- `RadioGroup`
- `SearchInput`

### Data and Readability
- `Tabs`
- `Table`
- `CodeBlock`
- `Badge`
- `StatusDot`
- `Kbd`

### Feedback
- `Toast`
- `InlineAlert`
- `EmptyState`
- `LoadingBar` / `SkeletonRow`

### Overlays
- `CommandBar` (dialog)
- `ConfirmModal`

---

## 3) Layout Rules

### Grid
- Desktop shell: `240px left rail | minmax(0, 1fr) main | 320px right panel`
- Main content max width inside center area: `1200px`
- Internal grids: 12-column equivalent via CSS grid utility classes

### Spacing Scale
- Base unit: 4px
- Common paddings: 12, 16, 20, 24
- Section gaps: 16–24
- Component vertical rhythm: minimum 8px

### Container Rules
- Panels use subtle borders and soft neutral surfaces.
- Avoid nested heavy containers.
- Prefer one surface level per section; use dividers for substructure.

### Density Rules
- Default density: comfortable (long-session safe).
- Compact mode reserved for tables/logs only.

---

## 4) Typography Rules

### Families
- UI: `IBM Plex Sans`
- Monospace/code: `IBM Plex Mono`

### Scale
- Display title: 24/32, weight 600
- Section title: 18/26, weight 600
- Body: 14/22, weight 400
- Supporting text: 13/20, weight 400
- Caption/meta: 12/18, weight 500
- Code: 13/20, weight 400

### Rules
- Line length target: 65–90 characters for prose.
- Use uppercase labels sparingly (meta only).
- Keep contrast high and hierarchy explicit.

---

## 5) Interaction Rules

### State Model
- Hover: subtle surface lift (`background` shift only)
- Focus: visible 2px ring with offset
- Active: high-contrast border + muted fill shift
- Disabled: reduced contrast + no hover transitions
- Loading: deterministic labels (`Running checks…`), avoid spinners-only

### Empty States
- Always include:
  - what this area is for
  - why it is empty
  - immediate next action

### Command Bar
- Opens with `Cmd/Ctrl+K`
- Arrow keys navigate
- Enter executes
- Esc closes

### Error Handling
- Inline errors for field-level issues.
- Toasts for operation-level outcomes.
- Destructive actions must confirm in modal.

---

## 6) Accessibility Rules

### Contrast
- Body text >= WCAG AA contrast on panel backgrounds.
- Status colors are never the sole signal; include icon/label.

### Keyboard
- Full tab reachability for controls.
- Visible focus rings on all interactive elements.
- Logical tab order: nav -> top bar -> main -> right panel -> overlays.

### Semantics
- Landmark roles: `nav`, `main`, `aside`, `header`, `footer` where appropriate.
- Overlays use ARIA dialog semantics.
- Tab lists use `role=tablist`, `role=tab`, `role=tabpanel`.

### Motion
- Respect reduced motion settings.
- Keep transitions under 200ms and low amplitude.
