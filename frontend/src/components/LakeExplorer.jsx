// Data-lake browser: pick a layer, then a table, and page through every row and column as stored.
import { useEffect, useState } from 'react'
import { fetchLake, fetchLakeTable } from '../services/api.js'
import { int } from './format.js'

const LAYER_TITLES = { bronze: 'Bronze', silver: 'Silver', gold: 'Gold' }
const PAGE_SIZES = [25, 50, 100]

export default function LakeExplorer() {
  const [index, setIndex] = useState(null)
  const [error, setError] = useState(null)
  const [layer, setLayer] = useState(null)
  const [table, setTable] = useState(null)

  useEffect(() => {
    fetchLake().then(setIndex).catch((e) => setError(e.message))
  }, [])

  if (error) return <p className="error">Could not load the data-lake catalog: {error}</p>
  if (!index) return <p className="muted">Loading the data-lake catalog…</p>
  const current = index.layers.find((l) => l.layer === layer)
  return (
    <div className="lake">
      <div className="layer-cards" role="tablist" aria-label="Data-lake layers">
        {index.layers.map((l) => (
          <button type="button" key={l.layer} role="tab" aria-selected={l.layer === layer}
                  className={`layer-card layer-${l.layer} ${l.layer === layer ? 'active' : ''}`}
                  onClick={() => { setLayer(l.layer); setTable(null) }}>
            <span className="layer-name">{LAYER_TITLES[l.layer] || l.layer}</span>
            <span className="layer-meta">{l.tables.length} tables · {int(l.tables.reduce((a, t) => a + t.rows, 0))} rows</span>
            <span className="layer-desc">{l.description}</span>
          </button>
        ))}
      </div>
      {current && (
        <>
          <h3>{LAYER_TITLES[current.layer]} tables</h3>
          <div className="table-cards">
            {current.tables.map((t) => (
              <button type="button" key={t.table} className={`table-card ${t.table === table ? 'active' : ''}`}
                      onClick={() => setTable(t.table)} title={t.example_file}>
                <span className="table-name">{t.table}</span>
                <span className="muted">{int(t.rows)} rows · {t.columns.length} columns{t.files > 1 ? ` · ${int(t.files)} files` : ''}</span>
              </button>
            ))}
          </div>
        </>
      )}
      {current && table && <LakeTable key={`${current.layer}/${table}`} layer={current.layer} table={table}
                                      entry={current.tables.find((t) => t.table === table)} />}
      {!current && <p className="muted">Click a layer to see its tables.</p>}
    </div>
  )
}

function LakeTable({ layer, table, entry }) {
  const [page, setPage] = useState(1)
  const [size, setSize] = useState(25)
  const [state, setState] = useState({ key: null, data: null, error: null })
  const key = `${page}/${size}`

  useEffect(() => {
    let current = true
    fetchLakeTable(layer, table, { page, size })
      .then((data) => { if (current) setState({ key: `${page}/${size}`, data, error: null }) })
      .catch((e) => { if (current) setState({ key: `${page}/${size}`, data: null, error: e.message }) })
    return () => { current = false }
  }, [layer, table, page, size])

  const data = state.key === key ? state.data : null
  const pages = Math.max(1, Math.ceil(entry.rows / size))
  return (
    <div className="lake-table">
      <div className="lake-table-head">
        <h3><code>{layer}.{table}</code></h3>
        <span className="muted">{int(entry.rows)} rows · source <code>{entry.example_file}</code>{entry.files > 1 ? ` (+${int(entry.files - 1)} more files)` : ''}</span>
      </div>
      {state.error && state.key === key && <p className="error">Could not load the rows: {state.error}</p>}
      <div className="table-wrap lake-grid">
        <table>
          <thead>
            <tr>
              <th className="num">#</th>
              {entry.columns.map((c) => <th key={c.name} title={c.type}>{c.name}<span className="col-type">{c.type}</span></th>)}
            </tr>
          </thead>
          <tbody>
            {(data?.rows || []).map((row, i) => (
              <tr key={i}>
                <td className="num muted">{int((page - 1) * size + i + 1)}</td>
                {entry.columns.map((c) => <Cell key={c.name} value={row[c.name]} />)}
              </tr>
            ))}
          </tbody>
        </table>
        {!data && !state.error && <p className="muted pad">Loading…</p>}
      </div>
      <div className="pager">
        <label className="muted">Rows per page{' '}
          <select value={size} onChange={(e) => { setSize(Number(e.target.value)); setPage(1) }}>
            {PAGE_SIZES.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
        </label>
        <button type="button" disabled={page <= 1} onClick={() => setPage(1)}>« First</button>
        <button type="button" disabled={page <= 1} onClick={() => setPage(page - 1)}>← Previous</button>
        <span className="muted">Page {int(page)} of {int(pages)}</span>
        <button type="button" disabled={page >= pages} onClick={() => setPage(page + 1)}>Next →</button>
        <button type="button" disabled={page >= pages} onClick={() => setPage(pages)}>Last »</button>
      </div>
    </div>
  )
}

function Cell({ value }) {
  if (value == null) return <td className="muted">null</td>
  const text = typeof value === 'object' ? JSON.stringify(value) : String(value)
  return <td className={typeof value === 'number' ? 'num' : undefined} title={text.length > 60 ? text : undefined}>
    {text.length > 60 ? `${text.slice(0, 60)}…` : text}
  </td>
}
