// Layout building blocks: sections, stat tiles, tables and callouts.
export function Section({ id, number, title, question, children }) {
  return (
    <section id={id} className="section">
      <h2><span className="section-number">{number}</span>{title}</h2>
      {question && <blockquote className="question"><span>Question {number} (verbatim)</span>{question}</blockquote>}
      {children}
    </section>
  )
}

export function Stats({ items }) {
  return (
    <div className="stats">
      {items.map(({ label, value, note }) => (
        <div key={label} className="stat">
          <div className="stat-value">{value}</div>
          <div className="stat-label">{label}</div>
          {note && <div className="stat-note">{note}</div>}
        </div>
      ))}
    </div>
  )
}

/** Table. columns: [{key, label, format?, align?}] */
export function Table({ columns, rows, caption, onRowClick, rowKey }) {
  return (
    <div className="table-wrap">
      <table>
        {caption && <caption>{caption}</caption>}
        <thead>
          <tr>{columns.map((c) => <th key={c.key} className={c.align === 'right' ? 'num' : undefined}>{c.label}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={rowKey ? rowKey(row) : i} onClick={onRowClick ? () => onRowClick(row) : undefined}
                className={onRowClick ? 'clickable' : undefined}>
              {columns.map((c) => (
                <td key={c.key} className={c.align === 'right' ? 'num' : undefined}>
                  {c.format ? c.format(row[c.key], row) : row[c.key] ?? '–'}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function Notes({ title = 'Interpretation', items, tone }) {
  return (
    <div className={`notes ${tone || ''}`}>
      <h3>{title}</h3>
      <ul>{items.filter(Boolean).map((item, i) => <li key={i}>{item}</li>)}</ul>
    </div>
  )
}

export function Details({ summary, children }) {
  return (
    <details className="details">
      <summary>{summary}</summary>
      {children}
    </details>
  )
}
