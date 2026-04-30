import type { Config } from "tailwindcss";

const config: Config = {
  darkMode: ["class"],
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        /* ── Obsidian Void palette ── */
        "kh-black":   "#000000",
        "kh-surface": "#0A0A0A",
        "kh-text":    "#EDEDED",
        "kh-muted":   "#737373",
        "kh-border":  "#262626",
        "kh-accent":  "#12d393",
        "kh-success": "#12d393",
        "kh-warning": "#F59E0B",
        "kh-danger":  "#EF4444",

        /* ── shadcn CSS-var wiring ── */
        background: "hsl(var(--background))",
        foreground: "hsl(var(--foreground))",
        card: {
          DEFAULT:    "hsl(var(--card))",
          foreground: "hsl(var(--card-foreground))",
        },
        popover: {
          DEFAULT:    "hsl(var(--popover))",
          foreground: "hsl(var(--popover-foreground))",
        },
        primary: {
          DEFAULT:    "hsl(var(--primary))",
          foreground: "hsl(var(--primary-foreground))",
        },
        secondary: {
          DEFAULT:    "hsl(var(--secondary))",
          foreground: "hsl(var(--secondary-foreground))",
        },
        muted: {
          DEFAULT:    "hsl(var(--muted))",
          foreground: "hsl(var(--muted-foreground))",
        },
        accent: {
          DEFAULT:    "hsl(var(--accent))",
          foreground: "hsl(var(--accent-foreground))",
        },
        destructive: {
          DEFAULT:    "hsl(var(--destructive))",
          foreground: "hsl(var(--destructive-foreground))",
        },
        border: "hsl(var(--border))",
        input:  "hsl(var(--input))",
        ring:   "hsl(var(--ring))",
        sidebar: {
          DEFAULT:              "hsl(var(--sidebar))",
          foreground:           "hsl(var(--sidebar-foreground))",
          primary:              "hsl(var(--sidebar-primary))",
          "primary-foreground": "hsl(var(--sidebar-primary-foreground))",
          accent:               "hsl(var(--sidebar-accent))",
          "accent-foreground":  "hsl(var(--sidebar-accent-foreground))",
          border:               "hsl(var(--sidebar-border))",
          ring:                 "hsl(var(--sidebar-ring))",
        },
      },

      fontFamily: {
        display: ["Space Grotesk", "sans-serif"],
        body:    ["Geist", "var(--font-geist-sans)", "sans-serif"],
        mono:    ["JetBrains Mono", "var(--font-geist-mono)", "monospace"],
      },

      /* Zero border-radius everywhere — the Obsidian Void spec */
      borderRadius: {
        DEFAULT: "0px",
        none:    "0px",
        sm:      "0px",
        md:      "0px",
        lg:      "0px",
        xl:      "0px",
        "2xl":   "0px",
        "3xl":   "0px",
        full:    "0px",
      },

      keyframes: {
        "accordion-down": {
          from: { height: "0" },
          to:   { height: "var(--radix-accordion-content-height)" },
        },
        "accordion-up": {
          from: { height: "var(--radix-accordion-content-height)" },
          to:   { height: "0" },
        },
        shimmer: {
          "0%, 100%": { transform: "translateX(-100%)" },
          "50%":      { transform: "translateX(100%)" },
        },
      },
      animation: {
        "accordion-down": "accordion-down 0.2s ease-out",
        "accordion-up":   "accordion-up 0.2s ease-out",
        shimmer:          "shimmer 2s ease-in-out infinite",
      },
    },
  },
  plugins: [require("tailwindcss-animate"), require("@tailwindcss/container-queries")],
};
export default config;
