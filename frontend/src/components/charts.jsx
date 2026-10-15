// Small SVG charts (no chart library): one series colour, value labels at the bar ends,
// recessive grid, and a hover tooltip on every mark. Every chart sits next to a table with the same numbers.
import { useState } from 'react'
import { int } from './format.js'

function Tooltip({ tip }) {
  if (!tip) return null
  return (
    <div className="tooltip" style={{ left: `${tip.x}%`, top: `${tip.y}%` }} role="status">
      <strong>{tip.title}</strong>
      {tip.lines.map((line) => <div key={line}>{line}</div>)}
    </div>
  )
}

/** Bar chart. data: [{label, value, detail?}]; format(value) gives the value label. */
export function BarChart({ data, horizontal = false, format = int, title, xLabel, height }) {
  const [tip, setTip] = useState(null)
  const max = Math.max(...data.map((d) => d.value), 0) || 1
  const W = 640
  if (horizontal) {
    const labelW = 230, row = 30, H = height || data.length * row + 24
    const scale = (v) => (v / max) * (W - labelW - 70)
    return (
      <figure className="chart">
        {title && <figcaption>{title}</figcaption>}
        <div className="chart-box">
          <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={title}>
            {data.map((d, i) => {
              const y = i * row + 8
              const show = () => setTip({ x: ((labelW + scale(d.value)) / W) * 100, y: ((y + 4) / H) * 100,
                title: d.label, lines: [format(d.value), ...(d.detail ? [d.detail] : [])] })
              return (
                <g key={d.label} onMouseEnter={show} onMouseLeave={() => setTip(null)}>
                  <rect className="hit" x="0" y={y - 4} width={W} height={row} />
                  <text className="axis-label" x={labelW - 8} y={y + 13} textAnchor="end">{d.label}</text>
                  <rect className="bar" x={labelW} y={y} width={Math.max(scale(d.value), 2)} height={row - 12} rx="3" />
                  <text className="value-label" x={labelW + scale(d.value) + 6} y={y + 13}>{format(d.value)}</text>
                </g>
              )
            })}
          </svg>
          <Tooltip tip={tip} />
        </div>
        {xLabel && <p className="axis-title">{xLabel}</p>}
      </figure>
    )
  }
  const VW = 480  // narrower canvas than horizontal charts, so labels stay legible when two charts sit side by side
  const H = height || 240, bottom = 40, top = 22, band = VW / data.length
  const scale = (v) => (v / max) * (H - bottom - top)
  return (
    <figure className="chart">
      {title && <figcaption>{title}</figcaption>}
      <div className="chart-box">
        <svg viewBox={`0 0 ${VW} ${H}`} role="img" aria-label={title}>
          {[0.25, 0.5, 0.75, 1].map((f) => (
            <line key={f} className="grid" x1="0" x2={VW} y1={H - bottom - f * (H - bottom - top)} y2={H - bottom - f * (H - bottom - top)} />
          ))}
          <line className="baseline" x1="0" x2={VW} y1={H - bottom} y2={H - bottom} />
          {data.map((d, i) => {
            const w = Math.min(band * 0.6, 90), x = i * band + (band - w) / 2, h = scale(d.value)
            const show = () => setTip({ x: ((x + w / 2) / VW) * 100, y: ((H - bottom - h) / H) * 100,
              title: d.label, lines: [format(d.value), ...(d.detail ? [d.detail] : [])] })
            return (
              <g key={d.label} onMouseEnter={show} onMouseLeave={() => setTip(null)}>
                <rect className="hit" x={i * band} y="0" width={band} height={H} />
                <rect className="bar" x={x} y={H - bottom - h} width={w} height={Math.max(h, 1)} rx="3" />
                <text className="value-label" x={x + w / 2} y={H - bottom - h - 6} textAnchor="middle">{format(d.value)}</text>
                <text className="axis-label" x={x + w / 2} y={H - bottom + 18} textAnchor="middle">{d.label}</text>
              </g>
            )
          })}
        </svg>
        <Tooltip tip={tip} />
      </div>
      {xLabel && <p className="axis-title">{xLabel}</p>}
    </figure>
  )
}

const SEQUENTIAL = ['#cde2fb', '#9ec5f4', '#6da7ec', '#3987e5', '#256abf', '#184f95', '#0d366b']

/** Colour at position t (0–1) along a ramp, interpolated between its steps (no banding at step edges). */
function ramp(stops, t) {
  const x = Math.min(1, Math.max(0, t)) * (stops.length - 1)
  const i = Math.min(stops.length - 2, Math.floor(x)), f = x - i
  const rgb = (hex) => [1, 3, 5].map((k) => parseInt(hex.slice(k, k + 2), 16))
  const [a, b] = [rgb(stops[i]), rgb(stops[i + 1])]
  return `rgb(${a.map((v, k) => Math.round(v + (b[k] - v) * f)).join(',')})`
}

/** Heatmap over the full 1–5 scale. cells: [{row, col, value, count}] */
export function Heatmap({ rows, cols, cells, title, note }) {
  const [tip, setTip] = useState(null)
  const W = 640, labelW = 170, top = 34, cell = 44
  const cw = (W - labelW) / cols.length, H = top + rows.length * cell + 4
  const at = Object.fromEntries(cells.map((c) => [`${c.row}|${c.col}`, c]))
  const color = (v) => ramp(SEQUENTIAL, (v - 1) / 4)
  return (
    <figure className="chart">
      {title && <figcaption>{title}</figcaption>}
      {note && <p className="chart-note">{note}</p>}
      <div className="chart-box">
        <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={title}>
          {cols.map((c, j) => (
            <text key={c} className="axis-label" x={labelW + j * cw + cw / 2} y={top - 12} textAnchor="middle">{c}</text>
          ))}
          {rows.map((r, i) => (
            <g key={r}>
              <text className="axis-label" x={labelW - 10} y={top + i * cell + cell / 2 + 4} textAnchor="end">{r}</text>
              {cols.map((c, j) => {
                const d = at[`${r}|${c}`]
                if (!d) return null
                const x = labelW + j * cw, y = top + i * cell
                return (
                  <g key={c} onMouseEnter={() => setTip({ x: ((x + cw / 2) / W) * 100, y: (y / H) * 100, title: `${r} · ${c}`,
                    lines: [`Average proficiency ${d.value.toFixed(2)}`, `${int(d.count)} assessments`] })}
                     onMouseLeave={() => setTip(null)}>
                    <rect x={x + 1} y={y + 1} width={cw - 2} height={cell - 2} rx="3" fill={color(d.value)} />
                    <text x={x + cw / 2} y={y + cell / 2 + 4} textAnchor="middle"
                          className={d.value >= 3.4 ? 'cell-label light' : 'cell-label'}>{d.value.toFixed(2)}</text>
                  </g>
                )
              })}
            </g>
          ))}
        </svg>
        <Tooltip tip={tip} />
      </div>
    </figure>
  )
}

/** Scatter plot. points: [{x, y, label, detail}] */
export function Scatter({ points, title, xLabel, yLabel, xDomain, yDomain, note }) {
  const [tip, setTip] = useState(null)
  const W = 640, H = 320, left = 52, bottom = 40, top = 14, right = 16
  const [x0, x1] = xDomain, [y0, y1] = yDomain
  const sx = (v) => left + ((v - x0) / (x1 - x0)) * (W - left - right)
  const sy = (v) => H - bottom - ((v - y0) / (y1 - y0)) * (H - bottom - top)
  const xTicks = Array.from({ length: 5 }, (_, i) => x0 + ((x1 - x0) * i) / 4)
  const yTicks = Array.from({ length: 5 }, (_, i) => y0 + ((y1 - y0) * i) / 4)
  return (
    <figure className="chart">
      {title && <figcaption>{title}</figcaption>}
      <div className="chart-box">
        <svg viewBox={`0 0 ${W} ${H}`} role="img" aria-label={title}>
          {yTicks.map((t) => (
            <g key={t}>
              <line className="grid" x1={left} x2={W - right} y1={sy(t)} y2={sy(t)} />
              <text className="axis-label" x={left - 8} y={sy(t) + 4} textAnchor="end">{t.toFixed(0)}</text>
            </g>
          ))}
          {xTicks.map((t) => (
            <text key={t} className="axis-label" x={sx(t)} y={H - bottom + 18} textAnchor="middle">{t.toFixed(1)}</text>
          ))}
          <line className="baseline" x1={left} x2={W - right} y1={H - bottom} y2={H - bottom} />
          {points.map((p) => (
            <g key={p.label} onMouseEnter={() => setTip({ x: (sx(p.x) / W) * 100, y: (sy(p.y) / H) * 100, title: p.label, lines: p.detail })}
               onMouseLeave={() => setTip(null)}>
              <circle className="hit" cx={sx(p.x)} cy={sy(p.y)} r="14" />
              <circle className="dot" cx={sx(p.x)} cy={sy(p.y)} r="6" />
            </g>
          ))}
        </svg>
        <Tooltip tip={tip} />
      </div>
      <p className="axis-title">{xLabel} → · ↑ {yLabel}</p>
      {note && <p className="chart-note">{note}</p>}
    </figure>
  )
}
