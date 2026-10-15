// Project walkthrough at /presentation: the whole pipeline step by step, like a slide deck.
// Counts, rules, columns and lineage are read live from the published data (summary + data-lake catalog).
import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import { fetchEmployee, fetchEmployees, fetchLake, fetchLakeTable, fetchSummary } from '../services/api.js'
import { strengthWord } from '../components/stats.js'
import './presentation.css'

const fmt = (v) => (v == null ? '–' : Math.round(v).toLocaleString('en-US'))
const pct = (v, d = 1) => (v == null ? '–' : `${(v * 100).toFixed(d)}%`)
const num = (v, d = 2) => (v == null ? '–' : Number(v).toFixed(d))
const lineageCols = (cols) => cols.filter((c) => !c.name.startsWith('_'))
// Storage types in plain words (Bronze keeps PostgreSQL's types; dates are stored as calendar dates).
const TYPE_NAMES = { int32: 'integer', int64: 'integer', float64: 'decimal', str: 'text', object: 'date', bool: 'yes/no' }
const typeName = (t) => TYPE_NAMES[t] || (t.startsWith('datetime64') ? 'timestamp' : t)

const STEPS = [['intro', 'Overview'], ['architecture', 'Big picture'], ['received', 'Data received'], ['bronze', 'Bronze'],
  ['silver', 'Silver'], ['gold', 'Gold'], ['answers', 'How it is answered'], ['questions', 'Columns used']]

// From the provided schema.sql (the source contract we were given).
const RELATIONSHIPS = [
  ['employees', 'team_id', 'teams'], ['projects', 'team_id', 'teams'], ['performance_reviews', 'employee_id', 'employees'],
  ['competency_assessments', 'employee_id', 'employees'], ['competency_assessments', 'competency_id', 'competencies'],
  ['talent_review', 'employee_id', 'employees (one per employee)'], ['learning events (JSONL)', 'employee_id', 'employees'],
]
const ACTION_TEXT = {
  accept: 'Keep the record as it is. Just for information.',
  warn: 'Keep the record, but flag it with the reason.',
  deduplicate: 'Remove the extra copy. One copy is kept.',
  quarantine: 'Set the record aside, with the reason. It is kept, but left out of the clean data.',
  fail: 'Stop for that table: the data can’t be trusted as a whole.',
}
const ACTION_NAMES = { accept: 'Accept', warn: 'Flag', deduplicate: 'Remove duplicate', quarantine: 'Set aside', fail: 'Stop' }
const CATEGORY_NAMES = { schema: 'Structure & format', integrity: 'Duplicates & links', business: 'Makes business sense',
  volume: 'Overall volume' }
const tableName = (entity) => (entity === '*' ? 'all tables' : entity === 'postgres entities' ? 'HR tables'
  : entity.split(', ').map((e) => e.replace(/_/g, ' ')).join(', '))
const ACTION_COLORS = { accept: '#0f766e', warn: '#b45309', deduplicate: '#7c3aed', quarantine: '#be123c', fail: '#0f172a' }
const QUESTIONS = {
  1: 'What are the performance ratings and review history for each employee?',
  2: 'Which employees have critical skill gaps in key competencies?',
  3: 'Who are the high-potential employees ready for promotion?',
  4: 'What training and development activities have been completed by each employee?',
  5: 'Which employees are at risk of attrition based on performance trends?',
  6: 'How does employee performance correlate with team and project outcomes?',
  7: 'What is the distribution of skills across the organization?',
}

export default function Presentation() {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [active, setActive] = useState('intro')

  useEffect(() => {
    document.title = 'ACME Talent Data Platform: project walkthrough'
    Promise.all([fetchSummary(), fetchLake()]).then(([summary, lake]) => setData({ summary, lake })).catch((e) => setError(e.message))
  }, [])

  useEffect(() => {  // highlight the step in view
    if (!data) return undefined
    const observer = new IntersectionObserver((entries) => {
      const visible = entries.filter((e) => e.isIntersecting).sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top)
      if (visible[0]) setActive(visible[0].target.id)
    }, { rootMargin: '-45% 0px -50% 0px' })
    STEPS.forEach(([id]) => { const el = document.getElementById(id); if (el) observer.observe(el) })
    return () => observer.disconnect()
  }, [data])

  return (
    <div className="pres">
      <header className="pres-top">
        <div className="pres-top-inner">
          <div className="pres-brand">ACME <span>Talent Data Platform</span></div>
          <nav className="pres-steps" aria-label="Steps">
            {STEPS.map(([id, name], i) => (
              <a key={id} href={`#${id}`} className={active === id ? 'active' : undefined}><b>{i + 1}</b>{name}</a>
            ))}
          </nav>
          <a className="pres-link" href="/">Open the dashboard →</a>
        </div>
      </header>
      {error && <p className="pres-loading">Could not load the published data: {error}</p>}
      {!data && !error && <p className="pres-loading">Loading the latest published data…</p>}
      {data && <Deck summary={data.summary} lake={data.lake} />}
    </div>
  )
}

function Deck({ summary, lake }) {
  const layer = (name) => lake.layers.find((l) => l.layer === name)
  const bronze = layer('bronze'), silver = layer('silver'), gold = layer('gold')
  const bronzeTables = bronze.tables.filter((t) => t.table !== 'learning_activity')
  const learning = bronze.tables.find((t) => t.table === 'learning_activity')
  const rules = summary.pipeline.rules
  const totalRead = summary.quality.reduce((a, e) => a + e.read, 0)
  return (
    <>
      <Intro summary={summary} bronzeTables={bronzeTables} learning={learning} rules={rules} gold={gold} totalRead={totalRead} />
      <Architecture />
      <Received bronzeTables={bronzeTables} learning={learning} />
      <Bronze bronze={bronze} learning={learning} />
      <Silver summary={summary} silver={silver} totalRead={totalRead} />
      <Gold summary={summary} gold={gold} />
      <Answers summary={summary} />
      <Questions summary={summary} />
      <footer className="pres-footer">
        Live data: Gold version <code>{summary.freshness.gold_run_id}</code>, as of {summary.freshness.as_of},
        from <code>{summary.freshness.lake_bucket}</code> ({summary.freshness.environment}).
      </footer>
    </>
  )
}

function Slide({ id, step, kicker, title, lead, className, children }) {
  return (
    <section id={id} className={`slide ${className || ''}`}>
      {kicker && <div className="kicker">{step && <span className="num">{step}</span>}{kicker}</div>}
      {title && <h2>{title}</h2>}
      {lead && <p className="lead">{lead}</p>}
      {children}
    </section>
  )
}

/* 1. Overview */
function Intro({ summary, bronzeTables, learning, rules, gold, totalRead }) {
  return (
    <Slide id="intro" className="hero">
      <span className="live-pill"><i />Live from {summary.freshness.environment} · data as of {summary.freshness.as_of}</span>
      <h1>From raw HR files to <span>answers HR can trust</span></h1>
      <p className="lead">A batch data pipeline for ACME’s performance and talent data: it ingests the sources, validates every record,
        builds curated tables and answers seven business questions — running on AWS, with a dashboard and notebooks on top.</p>
      <div className="kpis">
        <div className="kpi"><b>{bronzeTables.length + 1}</b><span>source datasets ({bronzeTables.length} CSV tables + learning events)</span></div>
        <div className="kpi"><b>{fmt(totalRead)}</b><span>source records validated</span></div>
        <div className="kpi"><b>{rules.length}</b><span>data-quality rules</span></div>
        <div className="kpi"><b>{gold.tables.length}</b><span>curated Gold tables</span></div>
        <div className="kpi"><b>7</b><span>business questions answered</span></div>
      </div>
    </Slide>
  )
}

/* 2. Big picture */
function Architecture() {
  const steps = [
    ['layer-src', 'Sources', 'HR tables (CSV) loaded into PostgreSQL; learning events (JSONL) in S3.', 'Aurora PostgreSQL · S3'],
    ['layer-bronze', 'Bronze', 'Exact copies of the sources, with lineage. Nothing is changed.', 's3://…/bronze/'],
    ['layer-silver', 'Silver', 'Every record checked against 39 rules: clean, typed, de-duplicated.', 's3://…/silver/'],
    ['layer-gold', 'Gold', 'Ten curated tables, one or more per business question.', 's3://…/gold/'],
    ['layer-out', 'Answers', 'Dashboard, notebooks and this walkthrough read the published Gold data.', 'CloudFront · Lambda · JupyterHub'],
  ]
  return (
    <Slide id="architecture" step="2" kicker="The big picture" title="A medallion data lake: Bronze → Silver → Gold"
           lead="Each layer has one job. Data only moves forward, every step is validated, and each layer is published as a complete version, so readers never see half-finished data.">
      <div className="flow">
        {steps.map(([cls, title, text, where], i) => (
          <FlowPart key={title} cls={cls} title={title} text={text} where={where} last={i === steps.length - 1} />
        ))}
      </div>
      <div className="grid g3" style={{ marginTop: 18 }}>
        <div className="card"><h4>Runs as one job on Amazon EKS</h4><p>A Kubernetes job runs the pipeline end to end: load the sources if
          needed, then Bronze, Silver, Gold and the dashboard data.</p></div>
        <div className="card"><h4>Incremental</h4><p>Only new or changed learning files are re-processed, and Gold tables are rebuilt only
          when their inputs change.</p></div>
        <div className="card"><h4>Safe to re-run</h4><p>Every layer is written to a new version and switched over in one step, so a failed
          run never leaves mixed data behind.</p></div>
      </div>
    </Slide>
  )
}

function FlowPart({ cls, title, text, where, last }) {
  return (
    <>
      <div className={`flow-step ${cls}`}><h4>{title}</h4><p>{text}</p><div className="where">{where}</div></div>
      {!last && <div className="flow-arrow" aria-hidden="true">→</div>}
    </>
  )
}

/* 3. Data received */
function Received({ bronzeTables, learning }) {
  return (
    <Slide id="received" step="3" kicker="Data received" title="What we were given"
           lead={`${bronzeTables.length} CSV files of HR data, ${fmt(learning.files)} JSONL files of learning events, and a schema.sql that defines the HR tables, their keys and their relationships.`}>
      <div className="grid g3">
        <div className="card"><span className="tag" style={{ background: '#e0f2fe', color: '#0369a1' }}>CSV</span>
          <div className="big" style={{ marginTop: 8 }}>{bronzeTables.length} files</div>
          <p>One file per HR table: {bronzeTables.map((t) => t.table).join(', ')}.</p></div>
        <div className="card"><span className="tag" style={{ background: '#f3e8ff', color: '#7e22ce' }}>JSONL</span>
          <div className="big" style={{ marginTop: 8 }}>{fmt(learning.files)} files</div>
          <p>{fmt(learning.rows)} learning events (one JSON object per line), organised by day: <code>year=/month=/day=</code>.</p></div>
        <div className="card"><span className="tag" style={{ background: '#dcfce7', color: '#15803d' }}>schema.sql</span>
          <div className="big" style={{ marginTop: 8 }}>{bronzeTables.length} tables</div>
          <p>Column types, primary keys, foreign keys and allowed values (e.g. rating 1–5) for the PostgreSQL source.</p></div>
      </div>
      <h3>The HR tables and their columns</h3>
      <div className="grid g3">
        {bronzeTables.map((t) => (
          <div className="card" key={t.table}>
            <h4><code>{t.table}</code></h4>
            <p>{fmt(t.rows)} rows</p>
            <div className="chips">{lineageCols(t.columns).map((c) => <span className="chip" key={c.name}>{c.name}<i>{typeName(c.type)}</i></span>)}</div>
          </div>
        ))}
        <div className="card">
          <h4><code>learning_activity</code> (JSONL)</h4>
          <p>{fmt(learning.rows)} lines in {fmt(learning.files)} files</p>
          <div className="chips">{lineageCols(learning.columns).map((c) => <span className="chip" key={c.name}>{c.name}</span>)}</div>
        </div>
      </div>
      <h3>How the tables relate (from schema.sql)</h3>
      <div className="rel-list">
        {RELATIONSHIPS.map(([from, key, to]) => (
          <div className="rel" key={`${from}-${key}-${to}`}><code>{from}</code><span className="arrow">— {key} →</span><code>{to}</code></div>
        ))}
      </div>
    </Slide>
  )
}

/* 4. Bronze */
function Bronze({ bronze, learning }) {
  const max = Math.max(...bronze.tables.map((t) => t.rows))
  return (
    <Slide id="bronze" step="4" kicker="Bronze layer" title="Land the sources exactly as they are"
           lead="Bronze is an untouched, traceable copy of both sources. If anything later looks wrong, we can always go back to exactly what we received.">
      <div className="steps">
        <div className="step"><h4>CSV → PostgreSQL</h4><p>schema.sql creates the 7 tables in the <code>hr_source</code> schema of Aurora
          PostgreSQL, and each CSV is loaded with <code>COPY</code> — all in one transaction, with row counts verified against the files.</p></div>
        <div className="step"><h4>PostgreSQL → S3 (Parquet)</h4><p>The job reads every table and writes a snapshot, keeping the database’s
          types: <code>bronze/postgres/&lt;table&gt;/ingest_date=YYYY-MM-DD/part-00000.parquet</code>.</p></div>
        <div className="step"><h4>JSONL → S3, as is</h4><p>The {fmt(learning.files)} learning files are copied byte for byte — bad lines
          included — to <code>bronze/learning_activity/year=…/month=…/day=…/</code>. Unchanged files are skipped.</p></div>
        <div className="step"><h4>Lineage on every row</h4><p>Each snapshot row records where it came from and when:</p>
          <div className="chips">{['_run_id', '_ingest_date', '_ingested_at', '_source_system', '_source_object'].map((c) => <span key={c} className="chip">{c}</span>)}</div>
          <p style={{ marginTop: 8 }}>Every copied file is verified against its source by size and S3 ETag, and each run writes a
            manifest of what landed.</p></div>
      </div>
      <h3>What landed in Bronze</h3>
      <div className="bar-list">
        {bronze.tables.map((t) => (
          <div className="bar-row" key={t.table}>
            <code>{t.table}</code>
            <div className="bar-track"><div className="bar-fill" style={{ width: `${(t.rows / max) * 100}%`, background: '#b45309' }} /></div>
            <span className="n">{fmt(t.rows)}</span>
          </div>
        ))}
      </div>
    </Slide>
  )
}

/* 5. Silver */
function Silver({ summary, silver, totalRead }) {
  const rules = summary.pipeline.rules
  const actions = summary.pipeline.actions
  const [filter, setFilter] = useState('all')
  const byAction = Object.fromEntries(actions.map((a) => [a, rules.filter((r) => r.action === a)]))
  const categories = [...new Set(rules.map((r) => r.category))]
  const shown = filter === 'all' ? rules : byAction[filter]
  const q = summary.quality
  const totals = q.reduce((a, e) => ({ accepted: a.accepted + e.accepted, warned: a.warned + e.accepted_with_warnings,
    dedup: a.dedup + e.deduplicated, quarantined: a.quarantined + e.quarantined }), { accepted: 0, warned: 0, dedup: 0, quarantined: 0 })
  const max = Math.max(...q.map((e) => e.read))
  return (
    <Slide id="silver" step="5" kicker="Silver layer" title={`Check every record against ${rules.length} rules`}
           lead="Silver turns raw copies into trustworthy, typed tables. Every rule says exactly what is checked and what happens when it fails — so nothing is silently dropped or changed.">
      <h3>The 5 actions a rule can take</h3>
      <div className="actions">
        {actions.map((a) => (
          <button type="button" key={a} className={`action a-${a} ${filter === a ? 'active' : ''}`}
                  onClick={() => setFilter(filter === a ? 'all' : a)}>
            <div className="count">{byAction[a].length}</div><h4>{ACTION_NAMES[a]}</h4><p>{ACTION_TEXT[a]}</p>
          </button>
        ))}
      </div>
      <h3>Rules by type of check and what happens</h3>
      <div style={{ overflowX: 'auto' }}>
        <table className="matrix">
          <thead><tr><th />{actions.map((a) => <th key={a}>{ACTION_NAMES[a]}</th>)}<th>Total</th></tr></thead>
          <tbody>
            {categories.map((c) => {
              const row = rules.filter((r) => r.category === c)
              return (
                <tr key={c}>
                  <th style={{ textAlign: 'right' }}>{CATEGORY_NAMES[c] || c}</th>
                  {actions.map((a) => {
                    const n = row.filter((r) => r.action === a).length
                    return n ? <td key={a} style={{ background: `${ACTION_COLORS[a]}1a`, color: ACTION_COLORS[a] }}>{n}</td>
                      : <td key={a} className="z">–</td>
                  })}
                  <td style={{ background: '#f1f5f9' }}>{row.length}</td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
      <p className="muted-p" style={{ fontSize: '0.9rem' }}><b>Structure &amp; format</b>: missing columns, wrong kinds of values,
        unreadable lines · <b>Duplicates &amp; links</b>: repeated records, or records pointing to someone or something that doesn’t exist ·
        {' '}<b>Makes business sense</b>: readable data that looks wrong for HR (for example a review before the hire date) ·
        {' '}<b>Overall volume</b>: too few records left, or too many set aside.</p>

      <h3>What happened to the {fmt(totalRead)} source records</h3>
      <div className="kpis" style={{ marginTop: 0 }}>
        <div className="kpi"><b>{fmt(totals.accepted - totals.warned)}</b><span>accepted, clean</span></div>
        <div className="kpi"><b>{fmt(totals.warned)}</b><span>accepted with a warning</span></div>
        <div className="kpi"><b>{fmt(totals.dedup)}</b><span>duplicates removed</span></div>
        <div className="kpi"><b>{fmt(totals.quarantined)}</b><span>quarantined, with the reason</span></div>
      </div>
      <div className="legend">
        <span><i style={{ background: '#0f766e' }} />accepted, clean</span><span><i style={{ background: '#f59e0b' }} />accepted with warning</span>
        <span><i style={{ background: '#7c3aed' }} />duplicate removed</span><span><i style={{ background: '#be123c' }} />quarantined</span>
      </div>
      <div className="bar-list">
        {q.map((e) => (
          <div className="bar-row" key={e.entity}>
            <code>{e.entity}</code>
            <div className="bar-track" style={{ width: `${(e.read / max) * 100}%`, minWidth: '4%' }}
                 title={`${fmt(e.read)} read: ${fmt(e.accepted - e.accepted_with_warnings)} clean, ${fmt(e.accepted_with_warnings)} with warnings, ${fmt(e.deduplicated)} duplicates, ${fmt(e.quarantined)} quarantined`}>
              {[[e.accepted - e.accepted_with_warnings, '#0f766e'], [e.accepted_with_warnings, '#f59e0b'], [e.deduplicated, '#7c3aed'],
                [e.quarantined, '#be123c']].map(([v, color], i) => <div key={i} className="bar-fill" style={{ width: `${(v / e.read) * 100}%`, background: color }} />)}
            </div>
            <span className="n">{fmt(e.read)}</span>
          </div>
        ))}
      </div>

      <h3>All {rules.length} rules</h3>
      <div className="filter-row">
        {['all', ...actions].map((a) => (
          <button type="button" key={a} className={filter === a ? 'on' : undefined} onClick={() => setFilter(a)}>
            {a === 'all' ? 'All' : ACTION_NAMES[a]} ({a === 'all' ? rules.length : byAction[a].length})
          </button>
        ))}
      </div>
      <div className="ptable-wrap scroll-box">
        <table className="ptable">
          <thead><tr><th>Rule</th><th>Applies to</th><th>What it checks</th><th>What happens</th><th>Times found</th></tr></thead>
          <tbody>
            {shown.map((r) => (
              <tr key={r.rule_id}>
                <td><code>{r.rule_id}</code></td><td style={{ textTransform: 'capitalize' }}>{tableName(r.entity)}</td>
                <td title={`Technical detail: ${r.logic}`}>{r.plain}</td>
                <td><span className={`badge-a a-${r.action}`}>{ACTION_NAMES[r.action]}</span></td><td className="n">{fmt(r.fired)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="muted-p" style={{ fontSize: '0.88rem' }}>“Times found” is how many records the rule caught in the published Silver
        version. Hover a description for the technical detail. Silver now holds {silver.tables.filter((t) => !t.table.endsWith('.quarantine')).length} validated tables and the quarantine.</p>
    </Slide>
  )
}

/* 6. Gold */
function Gold({ summary, gold }) {
  const tables = summary.pipeline.gold_tables
  const rows = Object.fromEntries(gold.tables.map((t) => [t.table, t.rows]))
  const [selected, setSelected] = useState('promotion_pipeline')
  const layout = useMemo(() => lineageLayout(tables), [tables])
  const sel = tables.find((t) => t.table === selected)
  return (
    <Slide id="gold" step="6" kicker="Gold layer" title="Build business-ready tables from Silver"
           lead="Each Gold table has one clear grain (what one row means), is built from Silver with documented joins, and must pass its checks — schema, unique key, allowed values and reconciliation with Silver — before it is published. Click a table.">
      <div className="lineage">
        <svg viewBox={`0 0 ${layout.width} ${layout.height}`} role="img" aria-label="Lineage from Silver to Gold tables">
          {layout.columns.map((c) => <text key={c.label} className="lin-col" x={c.x} y={18}>{c.label}</text>)}
          {layout.edges.map((e) => {
            const hot = e.to === selected
            const dim = selected && !hot
            const a = layout.nodes[e.from], b = layout.nodes[e.to]
            const x1 = a.x + layout.w, y1 = a.y + layout.h / 2, x2 = b.x, y2 = b.y + layout.h / 2, mx = (x1 + x2) / 2
            return <path key={`${e.from}-${e.to}`} className={`lin-edge ${hot ? 'hot' : ''} ${dim ? 'dim' : ''}`}
                         d={`M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`} />
          })}
          {Object.entries(layout.nodes).map(([name, n]) => {
            const related = name === selected || layout.edges.some((e) => e.to === selected && e.from === name)
            return (
              <g key={name} className={`lin-node ${n.kind} ${name === selected ? 'sel' : ''} ${related ? '' : 'dim'}`}
                 onClick={() => n.kind === 'gold' && setSelected(name)} role={n.kind === 'gold' ? 'button' : undefined}>
                <rect x={n.x} y={n.y} width={layout.w} height={layout.h} rx="8" />
                <text x={n.x + 10} y={n.y + layout.h / 2 + 4}>{name}</text>
              </g>
            )
          })}
        </svg>
      </div>
      <p className="muted-p" style={{ margin: '18px 0 8px', fontSize: '0.9rem' }}>Pick a Gold table (or click it in the diagram):</p>
      <div className="grid g4">
        {tables.map((t) => (
          <button type="button" key={t.table} className="card" style={{ textAlign: 'left', font: 'inherit', cursor: 'pointer',
            borderColor: t.table === selected ? '#2563eb' : undefined }} onClick={() => setSelected(t.table)}>
            <h4><code>{t.table}</code></h4>
            <p>{fmt(rows[t.table])} rows · {t.question}</p>
          </button>
        ))}
      </div>
      {sel && (
        <div className="detail">
          <h4><code>{sel.table}</code> · {fmt(rows[sel.table])} rows</h4>
          <dl>
            <dt>Answers</dt><dd>{sel.question}</dd>
            <dt>One row is</dt><dd>one {sel.primary_key.join(' × ').replace(/_id/g, '').replace(/_/g, ' ')} (key: {sel.primary_key.map((k) => <code key={k}>{k}</code>)})</dd>
            <dt>Reads</dt><dd>{[...sel.silver.map((s) => `Silver ${s}`), ...sel.gold.map((g) => `Gold ${g}`)].join(' · ')}</dd>
            <dt>How it is built</dt><dd>{sel.how}</dd>
          </dl>
        </div>
      )}
      {sel && <ColumnLineage key={sel.table} table={sel} />}
      {sel && <TablePreview key={`rows-${sel.table}`} table={sel.table} total={rows[sel.table]} />}
      <GoldChecks details={summary.gold_check_details} />
    </Slide>
  )
}

function lineageLayout(tables) {
  const silverOrder = ['employees', 'teams', 'performance_reviews', 'competency_assessments', 'competencies', 'learning_activity',
    'talent_review', 'projects']
  const byName = Object.fromEntries(tables.map((t) => [t.table, t]))
  const depthCache = {}
  const depth = (name) => {
    if (depthCache[name] == null) depthCache[name] = 1 + Math.max(0, ...byName[name].gold.map(depth))
    return depthCache[name]
  }
  const maxDepth = Math.max(...tables.map((t) => depth(t.table)))
  const w = 200, h = 32, gapY = 16, colGap = 70, top = 34
  const width = (maxDepth + 1) * w + maxDepth * colGap
  const nodes = {}, columns = [{ label: 'Silver', x: 0 }]
  silverOrder.forEach((s, i) => { nodes[s] = { kind: 'silver', x: 0, y: top + i * (h + gapY) } })
  for (let d = 1; d <= maxDepth; d++) {
    const x = d * (w + colGap)
    columns.push({ label: d === 1 ? 'Gold (from Silver)' : `Gold (step ${d})`, x })
    tables.filter((t) => depth(t.table) === d).forEach((t, i) => { nodes[t.table] = { kind: 'gold', x, y: top + i * (h + gapY) * 1.5 } })
  }
  const edges = tables.flatMap((t) => [...t.silver, ...t.gold].filter((src) => nodes[src]).map((src) => ({ from: src, to: t.table })))
  const height = Math.max(...Object.values(nodes).map((n) => n.y)) + h + 10
  return { nodes, edges, columns, width, height, w, h }
}

const CHECK_KINDS = [
  ['structure', 'Built correctly', 'The table has the right columns, every row has its id, and nothing appears twice.'],
  ['values', 'Values make sense', 'Ratings stay between 1 and 5, percentages between 0% and 100%, and labels are only the allowed ones.'],
  ['totals', 'Numbers add up', 'Totals are counted a second way from the cleaned data, and both counts must match.'],
]

/** The checks every Gold table must pass before it is published, in plain words, from the published run records. */
function GoldChecks({ details }) {
  const all = details.flatMap((t) => t.checks)
  const passed = all.filter((c) => c.passed).length
  const count = (kind) => all.filter((c) => c.kind === kind)
  const numbers = (text) => text.replace(/\d{4,}/g, (n) => Number(n).toLocaleString('en-US'))
  return (
    <div className="gc">
      <h3>Before publishing: every Gold table must pass its checks</h3>
      <div className="gc-hero">
        <div className="gc-score"><b>{passed} / {all.length}</b><span>checks passed</span></div>
        <p>Each Gold table is checked before anyone can see it. <strong>If even one check fails, nothing is published</strong> —
          the previous good version stays in place, and the failure is recorded with its numbers. “{passed} / {all.length}” means every
          check passed for the version you are looking at.</p>
      </div>
      <div className="gc-kinds">
        {CHECK_KINDS.map(([kind, title, text]) => {
          const list = count(kind)
          return (
            <div key={kind} className={`gc-kind k-${kind}`}>
              <div className="gc-kind-n">{list.length}</div>
              <h4>{title}</h4>
              <p>{text}</p>
              <span className="gc-kind-ok">✓ {list.filter((c) => c.passed).length} passed</span>
            </div>
          )
        })}
      </div>
      <p className="muted-p" style={{ margin: '18px 0 8px', fontSize: '0.9rem' }}>Every check, table by table (click a table):</p>
      <div className="gc-tables">
        {details.map((t) => (
          <details key={t.table} className="gc-table">
            <summary><code>{t.table}</code><span className="gc-count">✓ {t.checks.filter((c) => c.passed).length} / {t.checks.length}</span></summary>
            <ul>
              {t.checks.map((c) => (
                <li key={c.name} className={c.passed ? 'ok' : 'bad'}>
                  <span className="gc-tick" aria-label={c.passed ? 'passed' : 'failed'}>{c.passed ? '✓' : '✗'}</span>
                  <span className="gc-text">{c.plain}</span>
                  <span className={`gc-tag t-${c.kind}`}>{CHECK_KINDS.find(([k]) => k === c.kind)[1]}</span>
                  {c.detail && <span className="gc-detail">{numbers(c.detail)}</span>}
                </li>
              ))}
            </ul>
          </details>
        ))}
      </div>
    </div>
  )
}

/* 7. How each question is answered: one real example per question */
function Answers({ summary }) {
  const [ex, setEx] = useState(null)
  const [error, setError] = useState(null)
  useEffect(() => {
    const first = (params) => fetchEmployees({ ...params, size: 1 }).then((b) => b.rows[0])
    const detail = (params) => first(params).then((row) => fetchEmployee(row.employee_id))
    Promise.all([first({ list: 'performance', trend: 'improving' }), detail({ list: 'critical_gap' }),
      detail({ list: 'promotion_ready' }), detail({ list: 'top_learners' }), detail({ list: 'high_risk' })])
      .then(([q1, q2, q3, q4, q5]) => setEx({ q1, q2, q3, q4, q5 })).catch((e) => setError(e.message))
  }, [])
  return (
    <Slide id="answers" step="7" kicker="How each question is answered" title="One real example per question"
           lead="The data that goes in, the calculation, and the result — shown on a real employee, team or skill from the current data.">
      {error && <p className="muted-p">Could not load the examples: {error}</p>}
      {!ex && !error && <p className="muted-p">Loading examples…</p>}
      {ex && <div className="ex-grid">{answerCards(summary, ex).map((c) => <AnswerCard key={c.n} {...c} />)}</div>}
    </Slide>
  )
}

function AnswerCard({ n, title, who, data, calc, result, tone, overall }) {
  return (
    <article className="ex-card">
      <header className="ex-head"><span className="ex-q">Q{n}</span><h4>{title}</h4></header>
      <p className="ex-who">{who}</p>
      <div className="ex-flow">
        <div className="ex-box"><span className="ex-label">Data</span>
          <div className="ex-chips">{data.map((d) => <span key={d} className="ex-chip">{d}</span>)}</div></div>
        <span className="ex-arrow" aria-hidden="true">→</span>
        <div className="ex-box"><span className="ex-label">Calculation</span><div className="ex-calc">{calc}</div></div>
        <span className="ex-arrow" aria-hidden="true">→</span>
        <div className="ex-box"><span className="ex-label">Result</span><span className={`ex-result ${tone}`}>{result}</span></div>
      </div>
      <p className="ex-overall"><b>Across everyone:</b> {overall}</p>
    </article>
  )
}

function answerCards(s, ex) {
  const r = s.rules, a = r.attrition, sp = a.signal_points
  const trend = Object.fromEntries(s.q1.trend_distribution.map((t) => [t.trend, t.employees]))
  // Q1: the employee's two valid reviews and the change between them
  const [prev, last] = ex.q1.review_history.filter((h) => h.is_valid).slice(-2)
  const delta = last.rating - prev.rating
  // Q2: the employee's first critical gap
  const gap = ex.q2.skill_gaps.find((g) => g.is_critical_gap)
  const ratio = (gap.required_level - gap.proficiency_level) / gap.required_level
  // Q3: the three conditions of the promotion rule
  const p3 = ex.q3.promotion, rating3 = ex.q3.performance.latest_valid_rating
  const ok = (cond) => (cond ? '✓' : '✗')
  // Q4: learning counts
  const l4 = ex.q4.learning
  // Q5: points from each signal
  const k5 = ex.q5.attrition
  const flags = [['declining rating', k5.is_rating_declining], [`latest rating ≤ ${r.low_performer_max_rating}`, k5.is_low_latest_rating],
    [`no learning in ${r.learning_window_days} days`, k5.is_learning_inactive_12m]]
  const parts = [k5.hr_points, ...flags.map(([, on]) => (on ? sp : 0))]
  // Q6 and Q7 from the published aggregates
  const ds = s.q6.delivery_status
  const rows7 = s.q7.readiness, worst = rows7[rows7.length - 1]
  const assessed7 = rows7.reduce((x, y) => x + y.assessed, 0)
  const meetsAll = rows7.reduce((x, y) => x + y.meets_required * y.assessed, 0) / assessed7
  const meetCount = Math.round(worst.meets_required * worst.assessed)
  const high = s.q5.bands.find((b) => b.band === 'High'), med = s.q5.bands.find((b) => b.band === 'Medium')
  return [
    { n: 1, title: 'Performance and review history', who: `${ex.q1.name} · ${ex.q1.department}`,
      data: [`${prev.review_period}: rating ${prev.rating}`, `${last.review_period}: rating ${last.rating}`],
      calc: `${last.rating} − ${prev.rating} = ${delta > 0 ? '+' : ''}${delta}`,
      result: delta > 0 ? 'Improving' : delta < 0 ? 'Declining' : 'Stable', tone: delta > 0 ? 'good' : delta < 0 ? 'bad' : 'neutral',
      overall: `${fmt(trend.improving)} improving · ${fmt(trend.stable)} stable · ${fmt(trend.declining)} declining` },
    { n: 2, title: 'Critical skill gaps', who: `${ex.q2.name} · ${gap.competency_name}`,
      data: [`required level ${gap.required_level}`, `actual level ${gap.proficiency_level}`],
      calc: `(${gap.required_level} − ${gap.proficiency_level}) ÷ ${gap.required_level} = ${num(ratio)} ≥ ${r.critical_gap_ratio}`,
      result: 'Critical gap', tone: 'bad',
      overall: `${fmt(s.q2.critical_gaps)} critical gaps held by ${fmt(s.q2.employees_with_critical_gap)} employees` },
    { n: 3, title: 'Ready for promotion', who: `${ex.q3.name} · ${ex.q3.department}`,
      data: [`potential ${p3.potential_rating} ${ok(p3.potential_rating === r.promotion.potential)}`,
        `${p3.promotion_readiness} ${ok(p3.promotion_readiness === r.promotion.readiness)}`,
        `latest rating ${num(rating3, 0)} ${ok(rating3 >= r.promotion.min_rating)}`],
      calc: `${r.promotion.potential} + ${r.promotion.readiness} + rating ≥ ${r.promotion.min_rating}: all three met`,
      result: 'Promotion-ready', tone: 'good', overall: `${fmt(s.q3.promotion_ready)} of ${fmt(s.q1.employees)} employees` },
    { n: 4, title: 'Training completed', who: `${ex.q4.name} · ${ex.q4.department}`,
      data: [`${l4.courses_completed} courses completed`, `${l4.certifications_earned} certifications`, `last activity ${l4.last_activity_date}`],
      calc: `each course counted once · active if any learning in the ${r.learning_window_days} days before ${s.freshness.as_of}`,
      result: `${l4.courses_completed} courses · ${l4.is_learning_active_12m ? 'active' : 'inactive'}`, tone: 'good',
      overall: `${fmt(s.q4.totals.distinct_course_completions)} completions by ${fmt(s.q4.totals.employees_with_completion)} employees` },
    { n: 5, title: 'Attrition risk', who: `${ex.q5.name} · ${ex.q5.department}`,
      data: [`HR risk ${k5.hr_attrition_risk} +${k5.hr_points}`, ...flags.map(([name, on]) => `${name} +${on ? sp : 0}`)],
      calc: `${parts.join(' + ')} = ${k5.risk_points} points (High ≥ ${a.high_band_min_points})`,
      result: `${k5.risk_band} risk`, tone: k5.risk_band === 'High' ? 'bad' : k5.risk_band === 'Medium' ? 'warn' : 'good',
      overall: `${fmt(high.employees)} High · ${fmt(med.employees)} Medium risk` },
    { n: 6, title: 'Performance vs project outcomes', who: `${ds.teams} teams with projects`,
      data: [`${ds.n_in} teams all on time: avg ${num(ds.mean_in)}`, `${ds.n_out} teams with a late project: avg ${num(ds.mean_out)}`],
      calc: `${num(ds.mean_in)} − ${num(ds.mean_out)} = ${ds.difference >= 0 ? '+' : ''}${num(ds.difference)} · by chance ≈ ${Math.round(ds.p_value * 100)} in 100`,
      result: ds.p_value < 0.05 ? 'Real link' : 'Weak hint only', tone: ds.p_value < 0.05 ? 'good' : 'warn',
      overall: `link with project outcome scores is ${strengthWord(s.q6.pearson_r)} (r = ${num(s.q6.pearson_r)})` },
    { n: 7, title: 'Skills across the organisation', who: `${worst.competency_name} · ${worst.competency_category}`,
      data: [`${fmt(worst.assessed)} assessed`, `${fmt(meetCount)} meet the required level`],
      calc: `${fmt(meetCount)} ÷ ${fmt(worst.assessed)} = ${pct(worst.meets_required, 0)} (vs ${pct(meetsAll, 0)} overall)`,
      result: 'Needs attention most', tone: 'warn',
      overall: `${pct(Math.min(...rows7.map((x) => x.meets_required)), 0)}–${pct(Math.max(...rows7.map((x) => x.meets_required)), 0)} meet the required level across ${rows7.length} skills` },
  ]
}

/* 8. Questions: tables and columns */
function Questions({ summary }) {
  const [n, setN] = useState(1)
  const spec = summary.pipeline.questions.find((q) => q.question === n)
  const columnsOf = Object.fromEntries(summary.pipeline.gold_tables.map((t) => [t.table, t.columns.map((c) => c.name)]))
  return (
    <Slide id="questions" step="8" kicker="Columns used" title="Which Gold tables and columns answer each question"
           lead="For every question: the Gold tables used, the exact columns (highlighted), how they are combined, and the result in the current data.">
      <div className="q-tabs" role="tablist">
        {[1, 2, 3, 4, 5, 6, 7].map((i) => (
          <button type="button" key={i} role="tab" aria-selected={i === n} className={i === n ? 'on' : undefined} onClick={() => setN(i)}>Q{i}</button>
        ))}
      </div>
      <div className="q-card">
        <div className="q-head"><span className="tag">Question {n}</span><h3>{QUESTIONS[n]}</h3></div>
        <div className="q-body">
          <div>
            <h5>Gold tables and columns used</h5>
            {Object.entries(spec.tables).map(([table, used]) => (
              <div className="q-table" key={table}>
                <b>{table}</b>
                <div className="chips">
                  {[...used, ...columnsOf[table].filter((c) => !used.includes(c))].map((c) => (
                    <span key={c} className={`chip ${used.includes(c) ? 'on' : ''}`}>{c}</span>
                  ))}
                </div>
              </div>
            ))}
          </div>
          <div>
            <h5>How it is calculated</h5>
            <ol className="q-steps">{spec.steps.map((s) => <li key={s}>{s}</li>)}</ol>
            <h5 style={{ marginTop: 20 }}>Result in the current data</h5>
            <div className="q-result">{questionResult(n, summary).map(([value, label]) => <div key={label}><b>{value}</b><span>{label}</span></div>)}</div>
          </div>
        </div>
      </div>
    </Slide>
  )
}

function questionResult(n, s) {
  const trend = Object.fromEntries(s.q1.trend_distribution.map((t) => [t.trend, t.employees]))
  const r = s.rules
  switch (n) {
    case 1: return [[fmt(s.q1.rated_employees), 'employees with a valid review'], [fmt(s.q1.high_performers), `high performers (≥ ${r.high_performer_min_rating})`],
      [fmt(trend.improving), 'improving'], [fmt(trend.declining), 'declining']]
    case 2: return [[fmt(s.q2.critical_gaps), 'critical gaps'], [fmt(s.q2.employees_with_critical_gap), 'employees affected'],
      [pct(s.q2.critical_gap_rate), 'of assessments'], [s.q2.competencies[0].competency_name, 'highest-rate competency']]
    case 3: return s.q3.stages.map((x) => [fmt(x.employees), x.stage.replace(/^… /, '')])
    case 4: return [[fmt(s.q4.totals.distinct_course_completions), 'course completions'], [fmt(s.q4.totals.certifications_earned), 'certifications'],
      [fmt(s.q4.totals.employees_with_completion), 'employees completed a course'], [fmt(s.q4.totals.employees_inactive_12m), `no learning in ${r.learning_window_days} days`]]
    case 5: return [...s.q5.bands.map((b) => [fmt(b.employees), `${b.band} risk band`]), [fmt(s.q5.declining), 'declining performance']]
    case 6: return [[`${s.q6.teams_with_projects} of ${s.q6.teams_total}`, 'teams compared'], [strengthWord(s.q6.pearson_r), 'link with outcome scores'],
      [`${num(s.q6.delivery_status.mean_in)} vs ${num(s.q6.delivery_status.mean_out)}`, 'rating: all-on-time teams vs others'],
      [`p = ${num(s.q6.delivery_status.p_value)}`, 'on-time gap could be chance']]
    case 7: {
      const meets = s.q7.readiness.map((x) => x.meets_required), experts = s.q7.readiness.map((x) => x.expert_share)
      return [[`${pct(Math.min(...meets), 0)}–${pct(Math.max(...meets), 0)}`, 'meet the required level, by competency'],
        [`${pct(Math.min(...experts), 0)}–${pct(Math.max(...experts), 0)}`, 'experts (level 4–5), by competency'],
        [s.q7.department_variation.p_value < 0.05 ? 'Yes' : 'No', `departments differ? (p = ${num(s.q7.department_variation.p_value)})`]]
    }
    default: return []
  }
}

const KIND_TEXT = { copied: 'copied', joined: 'joined in', computed: 'calculated', aggregated: 'aggregated', pipeline: 'set by pipeline' }

/** Column-level lineage: lines from each source column (left) to the Gold column it feeds (right). */
function ColumnLineage({ table }) {
  const lineage = table.column_lineage
  const goldColumns = useMemo(() => Object.keys(lineage), [lineage])   // stable, so measuring runs once per table
  const groups = useMemo(() => {
    const byTable = new Map()
    goldColumns.forEach((c) => lineage[c].from.forEach((ref) => {
      const [layer, name, column] = ref.split('.')
      const key = `${layer}.${name}`
      if (!byTable.has(key)) byTable.set(key, { layer, name, columns: [] })
      if (!byTable.get(key).columns.includes(column)) byTable.get(key).columns.push(column)
    }))
    return [...byTable.values()].sort((a, b) => (a.layer === b.layer ? 0 : a.layer === 'silver' ? -1 : 1))
  }, [lineage, goldColumns])
  const box = useRef(null)
  const nodes = useRef({})
  const [lines, setLines] = useState([])
  const [hover, setHover] = useState(null)   // {type: 'gold'|'source', id}

  const measure = useCallback(() => {
    if (!box.current) return
    const origin = box.current.getBoundingClientRect()
    const at = (id, side) => {
      const el = nodes.current[id]
      if (!el) return null
      const r = el.getBoundingClientRect()
      return { x: (side === 'right' ? r.right : r.left) - origin.left, y: r.top + r.height / 2 - origin.top }
    }
    setLines(goldColumns.flatMap((c) => lineage[c].from.map((ref) => {
      const a = at(`src:${ref}`, 'right'), b = at(`gold:${c}`, 'left')
      return a && b ? { from: ref, to: c, a, b } : null
    }).filter(Boolean)))
  }, [goldColumns, lineage])

  useLayoutEffect(() => {
    measure()
    const observer = new ResizeObserver(measure)
    if (box.current) observer.observe(box.current)
    return () => observer.disconnect()
  }, [measure])

  const linked = (ref, column) => !hover || (hover.type === 'gold' ? hover.id === column : hover.id === ref)
  const activeSources = hover?.type === 'gold' ? new Set(lineage[hover.id].from) : null
  const activeGold = hover?.type === 'source' ? new Set(goldColumns.filter((c) => lineage[c].from.includes(hover.id))) : null

  return (
    <div className="col-lineage">
      <h3>Where every column of <code>{table.table}</code> comes from</h3>
      <p className="muted-p" style={{ marginTop: -4 }}>Hover a column to follow its lines. Left: the Silver (grey) and Gold (amber) tables
        it reads; right: each column of <code>{table.table}</code> and how it is produced.</p>
      <div className="cl-box" ref={box} onMouseLeave={() => setHover(null)}>
        <svg className="cl-lines" aria-hidden="true">
          {lines.map((l) => {
            const on = linked(l.from, l.to) && hover
            const mx = (l.a.x + l.b.x) / 2
            return <path key={`${l.from}>${l.to}`} className={`cl-line ${on ? 'on' : ''} ${hover && !linked(l.from, l.to) ? 'off' : ''}`}
                         d={`M${l.a.x},${l.a.y} C${mx},${l.a.y} ${mx},${l.b.y} ${l.b.x},${l.b.y}`} />
          })}
        </svg>
        <div className="cl-sources">
          {groups.map((g) => (
            <div key={`${g.layer}.${g.name}`} className={`cl-table ${g.layer}`}>
              <div className="cl-table-name"><span className="tag">{g.layer}</span> {g.name}</div>
              {g.columns.map((c) => {
                const ref = `${g.layer}.${g.name}.${c}`
                const dim = (activeSources && !activeSources.has(ref)) || (hover?.type === 'source' && hover.id !== ref)
                return (
                  <div key={c} ref={(el) => { nodes.current[`src:${ref}`] = el }} className={`cl-col ${dim ? 'dim' : ''}`}
                       onMouseEnter={() => setHover({ type: 'source', id: ref })}>{c}</div>
                )
              })}
            </div>
          ))}
        </div>
        <div className="cl-gold">
          <div className="cl-table-name"><span className="tag gold">gold</span> {table.table}</div>
          {goldColumns.map((c) => {
            const o = lineage[c]
            const dim = (activeGold && !activeGold.has(c)) || (hover?.type === 'gold' && hover.id !== c)
            return (
              <div key={c} ref={(el) => { nodes.current[`gold:${c}`] = el }} className={`cl-gcol ${dim ? 'dim' : ''}`}
                   onMouseEnter={() => setHover({ type: 'gold', id: c })}>
                <div className="cl-gname"><code>{c}</code><span className={`kind k-${o.kind}`}>{KIND_TEXT[o.kind]}</span></div>
                <div className="cl-how">{o.how}</div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

/** The first rows of the selected Gold table, from the published data-lake catalog. */
function TablePreview({ table, total }) {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  useEffect(() => {
    fetchLakeTable('gold', table, { page: 1, size: 25 }).then(setData).catch((e) => setError(e.message))
  }, [table])
  return (
    <div style={{ marginTop: 26 }}>
      <h3>The table itself: <code>{table}</code> <span className="muted-p" style={{ fontWeight: 400 }}>· first 25 of {fmt(total)} rows</span></h3>
      {error && <p className="muted-p">Could not load the rows: {error}</p>}
      {!data && !error && <p className="muted-p">Loading…</p>}
      {data && (
        <div className="ptable-wrap scroll-box" style={{ maxHeight: 420 }}>
          <table className="ptable">
            <thead><tr>{data.columns.map((c) => <th key={c.name}>{c.name}</th>)}</tr></thead>
            <tbody>
              {data.rows.map((row, i) => (
                <tr key={i}>{data.columns.map((c) => {
                  const v = row[c.name]
                  const text = v == null ? 'null' : typeof v === 'number' && !Number.isInteger(v) ? num(v, 3) : String(v)
                  return <td key={c.name} className={typeof v === 'number' ? 'n' : undefined}
                             style={{ whiteSpace: 'nowrap', color: v == null ? '#94a3b8' : undefined }}>{text}</td>
                })}</tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      <p className="muted-p" style={{ fontSize: '0.88rem' }}>All rows of every table are in the dashboard’s <a href="/#lake" style={{ color: '#2563eb' }}>Data lake</a> section.</p>
    </div>
  )
}
