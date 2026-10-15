// Number formatting shared by charts and tables.
export const int = (v) => (v == null ? '–' : Math.round(v).toLocaleString('en-US'))
export const pct = (v, digits = 1) => (v == null ? '–' : `${(v * 100).toFixed(digits)}%`)
export const num = (v, digits = 2) => (v == null ? '–' : Number(v).toFixed(digits))
export const label = (s) => (s == null ? '–' : String(s).replace(/_/g, ' ').replace(/^\w/, (c) => c.toUpperCase()))
