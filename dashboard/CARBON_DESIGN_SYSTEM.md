
# Carbon Design System Implementation Guide

This document details the high-fidelity Carbon Design System implementation for the IceVector dashboard, following IBM's design principles and token system.

## Layer Architecture

The UI follows a strict layering system where each "canvas" sits at a specific depth, creating visual hierarchy and context.

### Layer Hierarchy (Bottom to Top)

```
┌─────────────────────────────────────────────────────────────┐
│ $background (#161616 - Gray 100)                            │
│ The infinite floor - Base layer for the entire application  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐ │
│  │ $layer-01 (#262626 - Gray 90)                          │ │
│  │ Global Header, Breadcrumbs, Metric Tiles, Data Table   │ │
│  │                                                         │ │
│  │  ┌──────────────────────────────────────────────────┐  │ │
│  │  │ $layer-02 (#393939 - Gray 80)                    │  │ │
│  │  │ Semantic Playground (Right Rail)                 │  │ │
│  │  │ Closer to user - Indicates active context        │  │ │
│  │  └──────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

## Spacing Token System

All spacing uses Carbon's spacing scale for consistency:

| Token | Value | Usage |
|-------|-------|-------|
| `$spacing-02` | 4px | Minimal gaps (icon-to-text) |
| `$spacing-03` | 8px | Small gaps (column padding) |
| `$spacing-04` | 12px | Input padding |
| `$spacing-05` | 16px | **Gutter between components** |
| `$spacing-06` | 24px | **Card internal padding (Golden Rule)** |
| `$spacing-07` | 32px | Page margins, section separation |
| `$spacing-09` | 48px | Large empty state padding |

### Golden Rules

1. **Card Padding**: All tiles, tables, and panels use `$spacing-06` (24px) internal padding
2. **Gutters**: Use `$spacing-05` (16px) between major components
3. **Page Margins**: Use `$spacing-07` (32px) for outer page margins
4. **Row Height**: DataTable rows use 48px height (lg) for readability

## Component Breakdown

### 1. Global Shell (Header)

**Layer**: `$layer-01` (#262626)  
**Height**: 48px (fixed Carbon header height)  
**Border**: 1px bottom using `$border-subtle-01` (#393939)

```tsx
<Header aria-label="IceVector Dashboard">
  <HeaderName prefix="">
    <span className="app-logo">IceVector</span>
  </HeaderName>
  <HeaderGlobalBar>
    {/* Global utilities: Search, Notifications, Profile */}
  </HeaderGlobalBar>
</Header>
```

**Typography**:
- Product Name: IBM Plex Sans Semibold, 1.125rem
- Color: `$text-primary` (#f4f4f4)

### 2. Breadcrumb Zone

**Layer**: `$layer-01` (#262626)  
**Padding**: `$spacing-05` vertical (16px), `$spacing-07` horizontal (32px)  
**Border**: 1px bottom using `$border-subtle-01`

```scss
.breadcrumb-container {
  padding: $spacing-05 $spacing-07;
  background: var(--cds-layer-01, #262626);
  border-bottom: 1px solid var(--cds-border-subtle-01, #393939);
}
```

### 3. Page Header

**Padding**: `$spacing-07` top (32px) for separation from breadcrumbs  
**Typography**:
- H1: 2.5rem, font-weight 300, letter-spacing -0.03em
- Subtitle: 1rem, `$text-secondary` (#c6c6c6)

```scss
.page-header {
  padding: $spacing-07 $spacing-07 $spacing-05;
  
  .page-title {
    font-size: 2.5rem;
    font-weight: 300;
    color: var(--cds-text-primary, #f4f4f4);
    margin: 0 0 $spacing-03 0;
  }
}
```

### 4. KPI Metrics (Zone B)

**Layout**: 4 tiles × 4 columns = 16 columns  
**Layer**: `$layer-01` (#262626)  
**Padding**: `$spacing-06` (24px) - Golden Rule  
**Gap**: `$spacing-05` (16px) between tiles

```scss
.system-metrics {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: $spacing-05;
  
  .metric-tile {
    background: var(--cds-layer-01, #262626);
    padding: $spacing-06; // 24px Golden Rule
    border: 1px solid var(--cds-border-subtle-01, #393939);
  }
}
```

**Interactive States**:
