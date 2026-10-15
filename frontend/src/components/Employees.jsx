// Employee lists (the notebook's per-question tables, for every employee) and the employee profile.
import { useEffect, useState } from 'react'
import { fetchEmployee, fetchEmployees } from '../services/api.js'
import { int, label, num } from './format.js'
import { Table } from './ui.jsx'

const COLUMNS = {
  name: { key: 'name', label: 'Employee' },
  department: { key: 'department', label: 'Department' },
  job_level: { key: 'job_level', label: 'Level' },
  latest_valid_rating: { key: 'latest_valid_rating', label: 'Latest rating', align: 'right', format: (v) => num(v, 0) },
  rating_trend: { key: 'rating_trend', label: 'Trend', format: label },
  critical_gaps: { key: 'critical_gaps', label: 'Critical gaps', align: 'right' },
  critical_competencies: { key: 'critical_competencies', label: 'Critical competencies', format: (v) => (v?.length ? v.join(', ') : '–') },
  courses_completed: { key: 'courses_completed', label: 'Courses', align: 'right' },
  certifications_earned: { key: 'certifications_earned', label: 'Certifications', align: 'right' },
  risk_band: { key: 'risk_band', label: 'Risk band', format: (v) => <span className={`badge band-${v}`}>{v}</span> },
  risk_points: { key: 'risk_points', label: 'Points', align: 'right' },
  hr_attrition_risk: { key: 'hr_attrition_risk', label: 'HR risk' },
  is_promotion_ready: { key: 'is_promotion_ready', label: 'Promotion-ready', format: (v) => (v ? 'Yes' : 'No') },
}
const pick = (...keys) => keys.map((k) => COLUMNS[k])

const LIST_COLUMNS = {
  all: pick('name', 'department', 'job_level', 'latest_valid_rating', 'rating_trend', 'critical_gaps', 'courses_completed',
    'is_promotion_ready', 'risk_band'),
  critical_gap: pick('name', 'department', 'job_level', 'critical_gaps', 'critical_competencies'),
  promotion_ready: pick('name', 'department', 'job_level', 'latest_valid_rating', 'rating_trend', 'critical_gaps', 'courses_completed'),
  high_risk: pick('name', 'department', 'job_level', 'risk_points', 'hr_attrition_risk', 'rating_trend', 'latest_valid_rating'),
  top_learners: pick('name', 'department', 'courses_completed', 'certifications_earned', 'latest_valid_rating'),
}

/** A searchable, paged employee list from the API. Clicking a row opens the profile. */
/** columns: optional override of the list's columns; trends: optional trend filter options. */
export function EmployeeList({ list, onSelect, pageSize = 10, columns, trends }) {
  const [query, setQuery] = useState('')
  const [department, setDepartment] = useState('')
  const [trend, setTrend] = useState('')
  const [page, setPage] = useState(1)
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)

  useEffect(() => {
    let current = true
    const timer = setTimeout(() => {
      fetchEmployees({ list, q: query, department, trend, page, size: pageSize })
        .then((body) => { if (current) { setData(body); setError(null) } })
        .catch((e) => { if (current) setError(e.message) })
    }, query ? 250 : 0)
    return () => { current = false; clearTimeout(timer) }
  }, [list, query, department, trend, page, pageSize])

  const pages = data ? Math.max(1, Math.ceil(data.total / pageSize)) : 1
  return (
    <div className="employee-list">
      <div className="filters">
        <input type="search" placeholder="Search by name" value={query} aria-label="Search employees by name"
               onChange={(e) => { setQuery(e.target.value); setPage(1) }} />
        <select value={department} aria-label="Filter by department" onChange={(e) => { setDepartment(e.target.value); setPage(1) }}>
          <option value="">All departments</option>
          {data?.departments.map((d) => <option key={d} value={d}>{d}</option>)}
        </select>
        {trends && (
          <select value={trend} aria-label="Filter by rating trend" onChange={(e) => { setTrend(e.target.value); setPage(1) }}>
            <option value="">All trends</option>
            {trends.map((t) => <option key={t} value={t}>{label(t)}</option>)}
          </select>
        )}
        <span className="muted">{data ? `${int(data.total)} employees` : 'Loading…'}</span>
      </div>
      {error && <p className="error">Could not load the list: {error}</p>}
      {data && (
        <>
          <Table columns={columns || LIST_COLUMNS[list]} rows={data.rows} rowKey={(r) => r.employee_id} onRowClick={(r) => onSelect(r.employee_id)} />
          <div className="pager">
            <button type="button" disabled={page <= 1} onClick={() => setPage(page - 1)}>← Previous</button>
            <span className="muted">Page {page} of {pages}</span>
            <button type="button" disabled={page >= pages} onClick={() => setPage(page + 1)}>Next →</button>
          </div>
        </>
      )}
    </div>
  )
}

/** Side panel with one employee's full record. */
/** rules: the business rules published with the summary (thresholds shown in the signal list). */
export function EmployeeProfile({ employeeId, onClose, rules }) {
  const [state, setState] = useState({ id: null, data: null, error: null })

  useEffect(() => {
    let current = true
    fetchEmployee(employeeId)
      .then((data) => { if (current) setState({ id: employeeId, data, error: null }) })
      .catch((e) => { if (current) setState({ id: employeeId, data: null, error: e.message }) })
    return () => { current = false }
  }, [employeeId])

  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])

  const e = state.id === employeeId ? state.data : null
  const error = state.id === employeeId ? state.error : null
  return (
    <div className="drawer-backdrop" onClick={onClose}>
      <aside className="drawer" onClick={(ev) => ev.stopPropagation()} aria-label="Employee profile">
        <button type="button" className="close" onClick={onClose} aria-label="Close">×</button>
        {error && <p className="error">Could not load the employee: {error}</p>}
        {!e && !error && <p className="muted">Loading…</p>}
        {e && (
          <>
            <h2>{e.name}</h2>
            <p className="muted">#{e.employee_id} · {e.job_level} · {e.department} · {e.team} · hired {e.hire_date} ({num(e.tenure_years, 1)} years)</p>
            <div className="profile-grid">
              <Fact label="Latest valid rating" value={num(e.performance.latest_valid_rating, 0)} note={e.performance.latest_valid_period} />
              <Fact label="Rating trend" value={label(e.performance.rating_trend)} />
              <Fact label="Promotion-ready" value={e.promotion.is_promotion_ready ? 'Yes' : 'No'}
                    note={[e.promotion.potential_rating && `${e.promotion.potential_rating} potential`, e.promotion.promotion_readiness].filter(Boolean).join(' · ')} />
              <Fact label="Attrition risk band" value={e.attrition.risk_band} note={`${points(e.attrition.risk_points)} · HR: ${e.attrition.hr_attrition_risk ?? '–'}`} />
            </div>

            <h3>Review history</h3>
            <Table rows={e.review_history} columns={[
              { key: 'review_period', label: 'Period' },
              { key: 'rating', label: 'Rating', align: 'right' },
              { key: 'is_valid', label: 'Counts', format: (v, r) => (v ? 'Yes' : r.is_pre_hire ? 'No (pre-hire)' : 'No') },
              { key: 'rating_change', label: 'Change', align: 'right', format: (v) => (v == null ? '–' : (v > 0 ? `+${v}` : `${v}`)) },
            ]} />

            <h3>Skill assessments</h3>
            {e.skill_gaps.length ? (
              <Table rows={e.skill_gaps} columns={[
                { key: 'competency_name', label: 'Competency' },
                { key: 'competency_category', label: 'Category' },
                { key: 'proficiency_level', label: 'Actual', align: 'right' },
                { key: 'required_level', label: 'Required', align: 'right' },
                { key: 'is_critical_gap', label: 'Critical gap', format: (v) => (v ? <span className="badge band-High">Critical</span> : 'No') },
              ]} />
            ) : <p className="muted">No assessments.</p>}

            <h3>Learning</h3>
            <div className="profile-grid">
              <Fact label="Courses completed" value={int(e.learning.courses_completed)} />
              <Fact label="Certifications" value={int(e.learning.certifications_earned)} />
              <Fact label="Average quiz score" value={num(e.learning.avg_quiz_score, 1)} />
              <Fact label="Last activity" value={e.learning.last_activity_date ?? '–'}
                    note={`${e.learning.is_learning_active_12m ? 'active' : 'no activity'} in the ${rules.learning_window_days} days before the as-of date`} />
            </div>

            <h3>Attrition signals</h3>
            <ul className="signals">
              <li>HR attrition risk: <strong>{e.attrition.hr_attrition_risk ?? '–'}</strong> ({points(e.attrition.hr_points)})</li>
              <li>Declining rating: <strong>{e.attrition.is_rating_declining ? `yes (+${points(rules.attrition.signal_points)})` : 'no'}</strong></li>
              <li>Latest valid rating {rules.low_performer_max_rating} or lower: <strong>{e.attrition.is_low_latest_rating ? `yes (+${points(rules.attrition.signal_points)})` : 'no'}</strong></li>
              <li>No learning in the {rules.learning_window_days} days before the as-of date: <strong>{e.attrition.is_learning_inactive_12m ? `yes (+${points(rules.attrition.signal_points)})` : 'no'}</strong></li>
            </ul>
          </>
        )}
      </aside>
    </div>
  )
}

const points = (n) => `${n ?? 0} point${n === 1 ? '' : 's'}`

function Fact({ label: name, value, note }) {
  return (
    <div className="fact">
      <div className="fact-label">{name}</div>
      <div className="fact-value">{value ?? '–'}</div>
      {note && <div className="fact-note">{note}</div>}
    </div>
  )
}
