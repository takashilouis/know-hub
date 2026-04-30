---
name: KNOW-hub Terminal
colors:
  surface: '#0d1511'
  surface-dim: '#0d1511'
  surface-bright: '#333b36'
  surface-container-lowest: '#08100c'
  surface-container-low: '#161d19'
  surface-container: '#19211d'
  surface-container-high: '#242c27'
  surface-container-highest: '#2f3731'
  on-surface: '#dce5dd'
  on-surface-variant: '#bacabf'
  inverse-surface: '#dce5dd'
  inverse-on-surface: '#2a322d'
  outline: '#85948a'
  outline-variant: '#3c4a41'
  surface-tint: '#31e09f'
  primary: '#48f0ad'
  on-primary: '#003824'
  primary-container: '#12d393'
  on-primary-container: '#005539'
  inverse-primary: '#006c49'
  secondary: '#b7cbc2'
  on-secondary: '#23342e'
  secondary-container: '#3b4d46'
  on-secondary-container: '#a9bdb4'
  tertiary: '#ffcba6'
  on-tertiary: '#4e2600'
  tertiary-container: '#ffa55c'
  on-tertiary-container: '#743b00'
  error: '#ffb4ab'
  on-error: '#690005'
  error-container: '#93000a'
  on-error-container: '#ffdad6'
  primary-fixed: '#5afeba'
  primary-fixed-dim: '#31e09f'
  on-primary-fixed: '#002113'
  on-primary-fixed-variant: '#005236'
  secondary-fixed: '#d3e7de'
  secondary-fixed-dim: '#b7cbc2'
  on-secondary-fixed: '#0e1f19'
  on-secondary-fixed-variant: '#394a44'
  tertiary-fixed: '#ffdcc4'
  tertiary-fixed-dim: '#ffb780'
  on-tertiary-fixed: '#2f1400'
  on-tertiary-fixed-variant: '#6f3800'
  background: '#0d1511'
  on-background: '#dce5dd'
  surface-variant: '#2f3731'
typography:
  display-lg:
    fontFamily: Space Grotesk
    fontSize: 30px
    fontWeight: '600'
    lineHeight: '1.2'
  headline-md:
    fontFamily: Space Grotesk
    fontSize: 18px
    fontWeight: '500'
    lineHeight: '1.4'
  body-base:
    fontFamily: Inter
    fontSize: 14px
    fontWeight: '400'
    lineHeight: '1.6'
  label-mono:
    fontFamily: JetBrains Mono
    fontSize: 12px
    fontWeight: '500'
    letterSpacing: 0.02em
  nav-link:
    fontFamily: Space Grotesk
    fontSize: 14px
    fontWeight: '500'
    lineHeight: normal
spacing:
  container-max: 1200px
  sidebar-width: 280px
  gutter: 1.5rem
  stack-gap: 1.25rem
  section-padding: 2rem
---

## Brand & Style
The brand identity is rooted in the "Technical/Futuristic" aesthetic, drawing inspiration from high-end developer tools and cybernetic terminals. It evokes a sense of "Instant Intelligence" and "Zero Latency," targeting technical professionals and engineering teams. 

The visual style is a fusion of **Minimalism** and **Brutalism**. It prioritizes extreme data clarity through a high-contrast dark palette, while utilizing "hacker-green" accents to emphasize speed and precision. The interface feels raw and functional, yet sophisticated, using sharp edges and monospaced accents to signal its role as a high-performance semantic search engine.

## Colors
The palette is dominated by an absolute black (`#000000`) background to maximize the vibrance of the **Primary Neon Mint** (`#12d393`). This primary color is used sparingly for highlights, status indicators, and interactive states to maintain its visual impact.

Secondary surfaces use a deep obsidian green (`#111816`) for the sidebar and a very dark charcoal (`#09090b`) for cards. Text follows a strict hierarchy: pure white for titles, zinc-gray for descriptions, and dimmed zinc for metadata. Highlighting is handled via a unique inverted style: black text on a neon mint background, creating a "search hit" effect that is impossible to miss.

## Typography
The system uses a triple-font approach to define its technical character. **Space Grotesk** is the primary display face, providing a geometric, futuristic feel for headlines and navigation. **Inter** (or a clean system sans) is used for body copy to ensure long-form readability of search results. **JetBrains Mono** is utilized for all metadata, IDs, and match percentages, reinforcing the "database" and "code-like" nature of the tool.

Letter spacing is tightened for display styles and slightly loosened for monospaced labels to aid legibility at small sizes.

## Layout & Spacing
The layout follows a **Fixed Sidebar + Fluid Content** model. The main results canvas is constrained to a max-width of 1200px to prevent excessive line lengths in search results. A 2-column grid is used for the results on desktop, collapsing to a single column on smaller screens.

Spacing follows a rigorous 4px/8px rhythm. Content density is moderately high to allow for quick scanning of multiple results, but clear vertical separation is maintained through consistent 20px (1.25rem) gaps between card elements.

## Elevation & Depth
Depth is created through **Bold Borders** and **Tonal Layers** rather than shadows. 
- **Tier 1:** The base background is absolute black.
- **Tier 2:** The sidebar and cards use slightly elevated dark grays with thin `1px` borders (`#27272a`).
- **Interactive Depth:** On hover, cards do not lift; instead, they swap their border color to the primary neon mint and reveal a `1px` top highlight.
- **Glassmorphism:** The header uses a `80%` opacity black background with a heavy `blur(12px)` effect to maintain context while scrolling through results.

## Shapes
The shape language is **Sharp and Brutalist**. The primary radius is `2px` (effectively sharp), applied to input fields, buttons, and cards. This reinforces the "unrefined" engineering aesthetic. The only exception is the user profile avatar and specific circular icons, which provide a singular point of organic contrast in an otherwise rigid, rectangular grid.

## Components
- **Buttons & Chips:** Filter chips use a `zinc-900` background with a subtle border. The "Active" state is highly distinct, using a `10%` primary tint for the background and a full-weight primary border with a faint outer glow.
- **Input Fields:** Search bars are oversized, utilizing `zinc-950` backgrounds and transitioning to a primary mint border upon focus. Keyboard shortcuts (e.g., ⌘K) are displayed as monospaced inline tags.
- **Result Cards:** These are the primary data container. They must include a header (Icon + Filename + Match %), a body (Text snippet with primary-color highlight spans), and a footer (Metadata with monospaced font).
- **Match Indicators:** Use a monospaced "Pill" style with a low-opacity primary background (`primary/5`) and a subtle border to denote relevance without overpowering the title.
- **Scrollbars:** Custom slim-line scrollbars that turn primary neon green on hover.