// ACME Talent Insights: the notebook's seven answers, live from the AWS data lake.
// Every number comes from the API, which serves what the pipeline published for the current Gold version.
import { useEffect, useState } from 'react'
import { BarChart, Scatter } from '../components/charts.jsx'
import { EmployeeList, EmployeeProfile } from '../components/Employees.jsx'
import LakeExplorer from '../components/LakeExplorer.jsx'
import CorrelationExplainer from '../components/CorrelationExplainer.jsx'
import { pValue, strengthWord } from '../components/stats.js'
import { int, label, num, pct } from '../components/format.js'
import { Details, Notes, Section, Stats, Table } from '../components/ui.jsx'
import { fetchSummary } from '../services/api.js'

const QUESTIONS = {
  1: 'What are the performance ratings and review history for each employee?',
  2: 'Which employees have critical skill gaps in key competencies?',
  3: 'Who are the high-potential employees ready for promotion?',
  4: 'What training and development activities have been completed by each employee?',
  5: 'Which employees are at risk of attrition based on performance trends?',
  6: 'How does employee performance correlate with team and project outcomes?',
  7: 'What is the distribution of skills across the organization?',
}
const NAV = [['lake', '0', 'Data lake'], ['quality', 'DQ', 'Data & quality'], ['q1', '1', 'Performance'], ['q2', '2', 'Skill gaps'], ['q3', '3', 'Promotion'],
  ['q4', '4', 'Training'], ['q5', '5', 'Attrition'], ['q6', '6', 'Teams & projects'], ['q7', '7', 'Skills'],
  ['directory', '8', 'Employees'], ['summary', '9', 'Summary']]

export default function Dashboard() {
  const [summary, setSummary] = useState(null)
  const [error, setError] = useState(null)
  const [selected, setSelected] = useState(null)

  useEffect(() => {
    fetchSummary().then(setSummary).catch((e) => setError(e.message))
  }, [])

  if (error) return <><TopBar /><div className="page"><Hero /><p className="error">Could not load the dashboard data: {error}</p></div></>
  if (!summary) return <><TopBar /><div className="page"><Hero /><p className="muted">Loading the latest data from the data lake…</p></div></>
  const open = (id) => setSelected(id)
  return (
    <>
    <TopBar ready />
    <div className="page">
      <Hero summary={summary} />
      <main>
        <Section id="lake" number="0" title="Data lake: Bronze, Silver and Gold">
          <p>Every table of the data lake as it is stored, all rows and columns. Click a layer, then a table. Bronze is the raw
            copy of the sources, Silver the validated data (with the rejected records in the <code>.quarantine</code> tables), Gold the
            curated business tables used below. Silver and Gold are the published versions; Bronze is the latest ingest.</p>
          <LakeExplorer />
        </Section>
        <Quality s={summary} />
        <Q1 s={summary} open={open} />
        <Q2 s={summary} open={open} />
        <Q3 s={summary} open={open} />
        <Q4 s={summary} open={open} />
        <Q5 s={summary} open={open} />
        <Q6 s={summary} />
        <Q7 s={summary} />
        <Section id="directory" number="8" title="Employee directory">
          <p>Every employee's full record: review history, skill assessments, learning, promotion and attrition signals. Search, filter, then click a row.</p>
          <EmployeeList list="all" onSelect={open} />
        </Section>
        <Summary s={summary} />
      </main>
      <footer className="footer">
        Built from Gold version {summary.freshness.gold_run_id} (Silver {summary.freshness.silver_run_id}) in {summary.serving.bucket}.
        Pipeline: PostgreSQL + S3 → Bronze → Silver → Gold → serving, on Amazon EKS.
      </footer>
      {selected != null && <EmployeeProfile employeeId={selected} rules={summary.rules} onClose={() => setSelected(null)} />}
    </div>
    </>
  )
}

/** Sticky top bar: brand, section navigation (the section in view is highlighted) and the walkthrough link. */
function TopBar({ ready = false }) {
  const [active, setActive] = useState(null)
  useEffect(() => {
    if (!ready) return undefined   // the sections exist only once the data has loaded
    const observer = new IntersectionObserver((entries) => {
      const visible = entries.filter((e) => e.isIntersecting).sort((a, b) => a.boundingClientRect.top - b.boundingClientRect.top)
      if (visible[0]) setActive(visible[0].target.id)
    }, { rootMargin: '-35% 0px -60% 0px' })
    NAV.forEach(([id]) => { const el = document.getElementById(id); if (el) observer.observe(el) })
    return () => observer.disconnect()
  }, [ready])
  return (
    <header className="topbar">
      <div className="topbar-inner">
        <a className="brand" href="#top" aria-label="ACME Talent Insights"><span className="brand-mark">A</span><span className="brand-name">ACME Talent Insights</span></a>
        <nav className="toc" aria-label="Sections">
          {NAV.map(([id, n, name]) => <a key={id} href={`#${id}`} className={active === id ? 'active' : undefined}><span>{n}</span>{name}</a>)}
        </nav>
        <a className="topbar-link" href="/presentation">Project walkthrough →</a>
      </div>
    </header>
  )
}

function Hero({ summary }) {
  const f = summary?.freshness
  const cloud = f?.environment === 'cloud (AWS)'
  const high = summary?.q5.bands.find((b) => b.band === 'High')
  return (
    <section id="top" className="hero-wrap">
      <div className="dash-hero">
        <div>
          <div className="eyebrow">Talent analytics</div>
          <h1>ACME Talent Insights</h1>
          <p className="subtitle">Answers to ACME’s seven business questions on performance, skills, promotion, learning and attrition — from
            the curated Gold layer of the talent data lake.</p>
          <div className="hero-links"><a href="#q1">Start with the answers ↓</a><a href="/presentation">How it was built →</a></div>
        </div>
        {f && (
          <div className={`source ${cloud ? 'cloud' : 'local'}`}>
            <div className="source-title"><span className="dot" />{cloud ? 'Live from AWS' : 'Local data (LocalStack)'}</div>
            <dl>
              <dt>Data as of</dt><dd>{f.as_of}</dd>
              <dt>Gold version</dt><dd><code>{f.gold_run_id}</code></dd>
              <dt>Published</dt><dd>{f.gold_published_at.slice(0, 16).replace('T', ' ')} UTC</dd>
              <dt>Lake bucket</dt><dd><code>{f.lake_bucket}</code></dd>
            </dl>
          </div>
        )}
      </div>
      {summary && (
        <div className="kpi-strip">
          <a className="dash-kpi" href="#q1"><div className="kpi-q">Q1 · Performance</div><div className="kpi-value">{int(summary.q1.employees)}</div>
            <div className="kpi-label">employees · {int(summary.q1.high_performers)} high performers</div></a>
          <a className="dash-kpi" href="#q2"><div className="kpi-q">Q2 · Skill gaps</div><div className="kpi-value">{int(summary.q2.critical_gaps)}</div>
            <div className="kpi-label">critical skill gaps</div></a>
          <a className="dash-kpi" href="#q3"><div className="kpi-q">Q3 · Promotion</div><div className="kpi-value">{int(summary.q3.promotion_ready)}</div>
            <div className="kpi-label">promotion-ready</div></a>
          <a className="dash-kpi" href="#q5"><div className="kpi-q">Q5 · Attrition</div><div className="kpi-value">{int(high?.employees)}</div>
            <div className="kpi-label">in the high-risk band</div></a>
        </div>
      )}
    </section>
  )
}

function Quality({ s }) {
  const checks = s.gold_checks.reduce((a, t) => a + t.checks, 0)
  const passed = s.gold_checks.reduce((a, t) => a + t.passed, 0)
  const read = s.quality.reduce((a, e) => a + e.read, 0)
  const quarantined = s.quality.reduce((a, e) => a + e.quarantined, 0)
  return (
    <Section id="quality" number="DQ" title="Data freshness and data quality">
      <p>Every answer describes the Gold version shown above. <strong>“As of” is the date of the source snapshot</strong>; time-relative
        measures such as “the last {s.rules.learning_window_days} days” of learning are computed relative to it, not to today.</p>
      <Stats items={[
        { label: 'Source records validated', value: int(read) },
        { label: 'Quarantined (rejected with a reason)', value: int(quarantined), note: pct(quarantined / read, 2) },
        { label: 'Gold checks passed', value: `${passed} / ${checks}`, note: `${s.gold_checks.length} tables` },
      ]} />
      <Table caption="Silver validation per source entity" rowKey={(r) => r.entity} rows={s.quality} columns={[
        { key: 'entity', label: 'Entity', format: label },
        { key: 'read', label: 'Read', align: 'right', format: int },
        { key: 'accepted', label: 'Accepted', align: 'right', format: int },
        { key: 'accepted_with_warnings', label: 'With warnings', align: 'right', format: int },
        { key: 'quarantined', label: 'Quarantined', align: 'right', format: int },
        { key: 'deduplicated', label: 'Duplicates', align: 'right', format: int },
        { key: 'quarantine_rate', label: 'Quarantine rate', align: 'right', format: (v) => pct(v, 2) },
      ]} />
      <Details summary="Learning-history sequence metrics (informational)">
        <p>How often a completion has no earlier enrollment, and a certification no earlier completion. Both sit in the range expected if
          event types were recorded in random order, so the dashboard shows <strong>no funnel or conversion rates</strong> (A23).</p>
        <Table rowKey={(r) => r.rule_id} rows={s.history_metrics} columns={[
          { key: 'metric', label: 'Metric', format: label },
          { key: 'events', label: 'Events', align: 'right', format: int },
          { key: 'flagged', label: 'Flagged', align: 'right', format: int },
          { key: 'rate', label: 'Rate', align: 'right', format: (v) => pct(v) },
          { key: 'random_baseline_rate', label: 'Random-order rate', align: 'right', format: (v) => pct(v) },
          { key: 'vs_random', label: 'Verdict' },
        ]} />
      </Details>
      <Details summary="Gold validation checks per table">
        <Table rowKey={(r) => r.table} rows={s.gold_checks} columns={[
          { key: 'table', label: 'Table' }, { key: 'rows', label: 'Rows', align: 'right', format: int },
          { key: 'built_by_run', label: 'Built by run' }, { key: 'checks', label: 'Checks', align: 'right' },
          { key: 'passed', label: 'Passed', align: 'right' },
        ]} />
      </Details>
    </Section>
  )
}

function Q1({ s, open }) {
  const q = s.q1, r = s.rules
  const [lo, hi] = r.rating_scale
  const trend = Object.fromEntries(q.trend_distribution.map((t) => [t.trend, t.employees]))
  const periodColumns = q.review_periods.map((period) => ({
    key: period, label: period, align: 'right',
    format: (_, row) => {
      const review = row.review_history.find((h) => h.review_period === period)
      if (!review) return <span className="muted">no review</span>
      return review.is_valid ? review.rating
        : <span className="muted" title="Review for a period before the hire date: kept in the history, excluded from ratings (A7)">
            {review.rating} (pre-hire, excluded)</span>
    },
  }))
  const columns = [
    { key: 'name', label: 'Employee' }, { key: 'department', label: 'Department' }, { key: 'job_level', label: 'Level' },
    ...periodColumns,
    { key: 'latest_valid_rating', label: 'Latest valid rating', align: 'right', format: (v) => num(v, 0) },
    { key: 'change', label: 'Change', align: 'right', format: (_, row) => {
      if (row.latest_valid_rating == null || row.previous_valid_rating == null || row.rating_trend === 'insufficient_history') return '–'
      const d = row.latest_valid_rating - row.previous_valid_rating
      return d > 0 ? `+${d}` : `${d}`
    } },
    { key: 'rating_trend', label: 'Trend', format: (v) => <span className={`badge trend-${v}`}>{label(v)}</span> },
  ]
  return (
    <Section id="q1" number="1" title="Performance ratings and review history" question={QUESTIONS[1]}>
      <p><strong>Valid review:</strong> a review for a period that ended on or after the hire date. Earlier ones are kept in the history but
        excluded from ratings (A7). <strong>Trend:</strong> the change from the preceding half-year’s valid rating.</p>
      <Stats items={[
        { label: 'Employees with a valid review', value: int(q.rated_employees), note: `of ${int(q.employees)}` },
        { label: `High performers (${r.high_performer_min_rating}–${hi})`, value: int(q.high_performers) },
        { label: `Low performers (${lo}–${r.low_performer_max_rating})`, value: int(q.low_performers) },
        { label: 'Pre-hire reviews excluded', value: int(q.pre_hire_reviews), note: `of ${int(q.reviews)} reviews` },
      ]} />
      <div className="grid-2">
        <BarChart title="Latest valid rating" xLabel={`Rating (${lo} = lowest, ${hi} = highest) · employees`}
                  data={q.rating_distribution.map((r) => ({ label: String(r.rating), value: r.employees, detail: pct(r.share) }))} />
        <BarChart title={`Rating trend (${q.review_periods.join(' → ')})`} xLabel="Employees"
                  data={q.trend_distribution.map((t) => ({ label: label(t.trend), value: t.employees, detail: pct(t.share) }))} />
      </div>
      <h3>Each employee’s ratings and review history</h3>
      <p className="muted">One row per employee: the rating for each review period (pre-hire reviews are shown but do not count), the latest
        valid rating, the change from the preceding period and the trend. Search by name or filter; click a row for the full record.</p>
      <EmployeeList list="performance" onSelect={open} columns={columns} pageSize={15}
                    trends={q.trend_distribution.map((t) => t.trend)} />
      <Notes items={[
        `Of the ${int(q.rated_employees)} employees with a valid review, ${int(q.high_performers)} have a latest rating of ${r.high_performer_min_rating} or higher and ${int(q.low_performers)} have ${r.low_performer_max_rating} or lower.`,
        `Of the ${int(q.employees_with_trend)} employees with a measurable trend, ${int(trend.improving)} improved, ${int(trend.stable)} were stable and ${int(trend.declining)} declined.`,
        `${int(trend.insufficient_history)} have no trend: ${int(q.employees_without_valid_review)} have no valid review at all, the rest only one.`,
        'Every employee’s ratings and review history are in the table above; click a row for the complete record.',
      ]} />
      <Notes title="Assumptions and limitations" tone="caveat" items={[
        `Only ${q.review_periods.length} review periods exist, so a trend is a single change, not a long-term trajectory.`,
        `${int(q.pre_hire_reviews)} reviews are dated before the employee’s hire date; they are excluded from ratings (proposed A7).`,
      ]} />
    </Section>
  )
}

function Q2({ s, open }) {
  const q = s.q2, ratio = s.rules.critical_gap_ratio
  const share = ratio === 0.5 ? 'half' : pct(ratio, 0)
  const example = (required, actual) => `required ${required}, actual ${actual} → ${(required - actual) / required >= ratio ? 'critical' : 'not critical'}`
  const top = q.competencies.slice(0, 10)
  const rates = q.competencies.map((c) => c.critical_gap_rate)
  return (
    <Section id="q2" number="2" title="Critical skill gaps" question={QUESTIONS[2]}>
      <p><strong>Definition (approved, A1):</strong> a gap is critical when the employee is missing at least {share} of the required level:
        (required − actual) ÷ required ≥ {ratio}. For example: {example(2, 1)}; {example(4, 2)}; {example(3, 2)}.</p>
      <Stats items={[
        { label: 'Critical gaps', value: int(q.critical_gaps), note: `${pct(q.critical_gap_rate)} of ${int(q.assessments)} assessments` },
        { label: 'Employees with a critical gap', value: int(q.employees_with_critical_gap), note: `${int(q.employees_with_two_or_more)} with two` },
        { label: 'Competencies ranked', value: int(q.competencies.length) },
      ]} />
      <BarChart horizontal title="Competencies with the highest critical-gap rate" format={(v) => `${v.toFixed(1)}%`}
                xLabel="Share of assessed employees with a critical gap"
                data={top.map((c) => ({ label: c.competency_name, value: c.critical_gap_rate * 100,
                  detail: `${c.critical_gap_count} of ${c.employees_assessed} assessed · ${c.competency_category}` }))} />
      <Table caption="Critical gaps by competency category" rowKey={(r) => r.competency_category} rows={q.by_category} columns={[
        { key: 'competency_category', label: 'Category' }, { key: 'assessments', label: 'Assessments', align: 'right', format: int },
        { key: 'critical_gaps', label: 'Critical gaps', align: 'right', format: int },
        { key: 'critical_gap_rate', label: 'Rate', align: 'right', format: (v) => pct(v) },
      ]} />
      <Details summary={`All ${q.competencies.length} competencies`}>
        <Table rowKey={(r) => r.competency_name} rows={q.competencies} columns={[
          { key: 'competency_name', label: 'Competency' }, { key: 'competency_category', label: 'Category' },
          { key: 'employees_assessed', label: 'Assessed', align: 'right' },
          { key: 'avg_proficiency', label: 'Avg actual', align: 'right', format: (v) => num(v) },
          { key: 'avg_required', label: 'Avg required', align: 'right', format: (v) => num(v) },
          { key: 'critical_gap_count', label: 'Critical', align: 'right' },
          { key: 'critical_gap_rate', label: 'Rate', align: 'right', format: (v) => pct(v) },
        ]} />
      </Details>
      <h3>Which employees?</h3>
      <p className="muted">Most critical gaps first, then by name. Click a row for the full record.</p>
      <EmployeeList list="critical_gap" onSelect={open} />
      <Notes items={[
        `${int(q.critical_gaps)} assessments (${pct(q.critical_gap_rate, 2)}) show a critical gap, held by ${int(q.employees_with_critical_gap)} employees: ${int(q.employees_with_one)} with one and ${int(q.employees_with_two_or_more)} with two.`,
        `Rates range from ${pct(Math.min(...rates))} to ${pct(Math.max(...rates))} across competencies; ${top[0].competency_name} is highest. The top of the chart is a shortlist for organisation-wide training.`,
        `Categories: ${q.by_category.map((c) => `${c.competency_category} ${pct(c.critical_gap_rate)}`).join(', ')}.`,
      ]} />
      <Notes title="Assumptions and limitations" tone="caveat" items={[
        '“Key competencies” is not defined in any source, so all competencies are ranked alike.',
        `Each employee is assessed on ${sampleText(s)}, so a competency’s rate is a sample estimate.`,
        `${int(q.pre_hire_assessments)} assessments are dated before the hire date; they are included (proposed A5).`,
      ]} />
    </Section>
  )
}

function Q3({ s, open }) {
  const q = s.q3
  return (
    <Section id="q3" number="3" title="High-potential employees ready for promotion" question={QUESTIONS[3]}>
      <p><strong>Rule (proposed, A19 — not yet approved by HR):</strong> potential <strong>{q.rule.potential}</strong>, readiness{' '}
        <strong>{q.rule.readiness}</strong>, and latest <strong>valid</strong> rating ≥ <strong>{q.rule.min_rating}</strong>.
        Critical skill gaps are shown alongside but exclude no one.</p>
      <BarChart horizontal title="How the proposed promotion rule narrows the group" xLabel="Employees"
                data={q.stages.map((x) => ({ label: x.stage.replace('(promotion-ready)', '').trim(), value: x.employees }))} />
      <Stats items={[
        { label: 'Promotion-ready', value: int(q.promotion_ready) },
        { label: 'High & Ready Now, rating below 4', value: int(q.ready_now_failing_rating) },
        { label: 'High & Ready Now, no valid rating', value: int(q.ready_now_without_valid_rating) },
        { label: 'Ready with a critical skill gap', value: int(q.ready_with_critical_gap) },
      ]} />
      <BarChart horizontal title="Promotion-ready employees by department" xLabel="Employees"
                data={q.by_department.map((d) => ({ label: d.department, value: d.employees }))} />
      <h3>The shortlist</h3>
      <p className="muted">Highest rating first, then fewest critical gaps. Click a row for the evidence.</p>
      <EmployeeList list="promotion_ready" onSelect={open} />
      <Notes items={[
        `${int(q.promotion_ready)} employees meet the proposed rule, out of ${int(q.high_and_ready_now)} whom HR rated ${q.rule.potential} potential and ${q.rule.readiness}.`,
        `${int(q.ready_now_failing_rating)} of those have a latest valid rating below ${q.rule.min_rating}; ${int(q.ready_now_without_valid_rating)} have no valid rating at all.`,
        `${int(q.ready_with_critical_gap)} of the ${int(q.promotion_ready)} have at least one critical skill gap — pair those promotions with a development plan.`,
      ]} />
      <Notes title="Assumptions and limitations" tone="caveat" items={[
        'A rule-based shortlist, not an approved HR policy (A19).',
        `${int(q.pre_hire_talent_reviews)} talent reviews are dated before the hire date; they are included and labelled (proposed A6).`,
      ]} />
    </Section>
  )
}

function Q4({ s, open }) {
  const q = s.q4, t = q.totals
  const dist = Object.fromEntries(q.completion_distribution.map((d) => [d.courses, d.employees]))
  return (
    <Section id="q4" number="4" title="Training and development completed" question={QUESTIONS[4]}>
      <p><strong>Course completed (approved, A13):</strong> an explicit completion event counts, even without a recorded enrollment; repeats
        of a course count once. Certifications are counted the same way. Pre-hire activity is excluded (proposed A4).</p>
      <Stats items={[
        { label: 'Distinct course completions', value: int(t.distinct_course_completions) },
        { label: 'Certifications earned', value: int(t.certifications_earned) },
        { label: 'Employees with a completed course', value: int(t.employees_with_completion) },
        { label: `No learning in the last ${q.window_days} days`, value: int(t.employees_inactive_12m) },
      ]} />
      <BarChart title="Employees by number of distinct courses completed" xLabel="Courses completed · employees"
                data={q.completion_distribution.map((d) => ({ label: d.courses, value: d.employees }))} />
      <h3>Most active learners</h3>
      <p className="muted">Click any employee for their full learning record.</p>
      <EmployeeList list="top_learners" onSelect={open} />
      <Notes items={[
        `${int(t.employees_with_completion)} employees completed at least one course: ${int(t.distinct_course_completions)} distinct completions and ${int(t.certifications_earned)} certifications. ${int(dist['0'])} completed none.`,
        `Learning is broad but shallow: ${int((dist['1'] || 0) + (dist['2'] || 0))} of the ${int(t.employees_with_completion)} learners completed one or two courses.`,
        `Only ${int(t.employees_inactive_12m)} employees recorded no learning in the ${q.window_days} days before the as-of date.`,
      ]} />
      <Notes title="Assumptions and limitations" tone="caveat" items={[
        'No funnel or conversion rates: learning events have no reliable sequence (A23).',
        'Training is not linked to skill gaps: no source maps courses to competencies.',
        `${int(t.pre_hire_events_excluded)} pre-hire learning events are excluded (proposed A4).`,
      ]} />
    </Section>
  )
}

function Q5({ s, open }) {
  const q = s.q5, a = s.rules.attrition, sp = a.signal_points
  const hrPoints = Object.entries(a.hr_points).filter(([, p]) => p > 0).map(([k, p]) => `${k} = ${p}`).join(', ')
  return (
    <Section id="q5" number="5" title="Attrition risk based on performance trends" question={QUESTIONS[5]}>
      <p><strong>Method (heuristic, A20 — not a validated model):</strong> HR’s own risk rating ({hrPoints}), +{sp} for a declining
        trend, +{sp} for a latest valid rating of {s.rules.low_performer_max_rating} or lower, +{sp} for no learning in the{' '}
        {s.rules.learning_window_days} days before the as-of date. Bands: {a.high_band_min_points}+ High,{' '}
        {a.medium_band_min_points}{a.high_band_min_points - a.medium_band_min_points > 1 ? `–${a.high_band_min_points - 1}` : ''} Medium,
        0–{a.medium_band_min_points - 1} Low.</p>
      <div className="grid-2">
        <BarChart title="Employees by heuristic attrition-risk band" xLabel="Risk band · employees"
                  data={q.bands.map((b) => ({ label: b.band, value: b.employees }))} />
        <Table caption="Rating trend × risk band" rowKey={(r) => r.trend} rows={q.trend_by_band} columns={[
          { key: 'trend', label: 'Trend', format: label },
          { key: 'High', label: 'High', align: 'right', format: int }, { key: 'Medium', label: 'Medium', align: 'right', format: int },
          { key: 'Low', label: 'Low', align: 'right', format: int },
        ]} />
      </div>
      <h3>Who is at risk?</h3>
      <p className="muted">The High band, highest points first. Click a row to see the signals behind the score.</p>
      <EmployeeList list="high_risk" onSelect={open} />
      <Notes items={[
        `${int(q.declining)} employees have a declining performance trend — the direct answer to “based on performance trends”. ${int(q.declining_hr_high)} of them are also rated High risk by HR.`,
        `Combining the signals puts ${int(q.bands[0].employees)} employees in the High band and ${int(q.bands[1].employees)} in Medium; ${int(q.declining_in_high_band)} of the High band have a declining trend.`,
        'The list is a starting point for conversations, not a verdict: every flag traces to the signals shown.',
      ]} />
      <Notes title="Assumptions and limitations" tone="caveat" items={[
        'A heuristic, not a prediction model: there is no data on who actually left, so it cannot be validated (A20).',
        'The trend is a single change between two half-years.',
      ]} />
    </Section>
  )
}

function Q6({ s }) {
  const q = s.q6
  const xs = q.teams.map((t) => t.avg_latest_valid_rating), ys = q.teams.map((t) => t.avg_project_outcome_score)
  const pad = (lo, hi, p) => [Math.floor((lo - p) * 2) / 2, Math.ceil((hi + p) * 2) / 2]
  const ds = q.delivery_status
  const dsChance = Math.round(ds.p_value * 100)
  const dsVerdict = ds.p_value < 0.05 ? 'yes' : ds.p_value < 0.2 ? 'hint' : 'no'
  const strength = strengthWord(q.pearson_r)
  const chance = Math.round(pValue(q.t_statistic, q.teams_with_projects - 2) * 100)
  const better = q.pearson_r >= 0 ? 'better' : 'worse'
  return (
    <Section id="q6" number="6" title="Performance vs team and project outcomes" question={QUESTIONS[6]}>
      <div className={`verdict ${q.significant_5pct ? 'yes' : 'no'}`}>
        <div className="verdict-title">{q.significant_5pct ? `A ${strength} link: higher-rated teams tend to deliver ${better} projects`
          : 'No reliable link between team performance and project results'}</div>
        <p>{q.significant_5pct
          ? `Across the ${q.teams_with_projects} teams that have projects, teams whose people have higher ratings tend to deliver ${better} project outcomes, and the pattern is too clear to be chance.`
          : `Across the ${q.teams_with_projects} teams that have projects, teams whose people have higher ratings do not clearly deliver better project outcomes. The pattern is ${strength}, and a pattern like it would appear by chance about ${chance} times in 100.`}</p>
      </div>
      <Stats items={[
        { label: 'Teams compared', value: `${q.teams_with_projects} of ${q.teams_total}`, note: 'only teams with projects can be compared' },
        { label: 'Projects', value: int(q.projects) },
        { label: 'Strength of the link', value: label(strength), note: 'scale: negligible · weak · moderate · strong' },
        { label: 'Reliable?', value: q.significant_5pct ? 'Yes' : 'No',
          note: q.significant_5pct ? 'too clear to be chance' : `could be chance (about ${chance} in 100)` },
      ]} />
      <Scatter title="Each dot is one team: its people’s average rating against its projects’ average outcome"
               xLabel="Team average latest valid rating (1–5)" yLabel="Average project outcome score"
               note="If better-rated teams delivered better projects, the dots would rise from left to right. They don’t: they form a loose cloud."
               xDomain={pad(Math.min(...xs), Math.max(...xs), 0.1)} yDomain={[Math.floor(Math.min(...ys) / 10) * 10, Math.ceil(Math.max(...ys) / 10) * 10]}
               points={q.teams.map((t) => ({ x: t.avg_latest_valid_rating, y: t.avg_project_outcome_score, label: t.team_name,
                 detail: [`${t.department} · ${t.headcount} people`, `Rating ${num(t.avg_latest_valid_rating)} · outcome ${num(t.avg_project_outcome_score, 1)}`,
                   `${t.project_count} project(s), ${pct(t.on_time_project_share, 0)} on time`] }))} />
      <h3>On-time delivery: do higher-rated teams deliver on time more often?</h3>
      <div className={`verdict ${dsVerdict}`}>
        <div className="verdict-title">{dsVerdict === 'yes' ? 'Yes: teams whose projects were all on time are rated higher'
          : dsVerdict === 'hint' ? 'A weak hint, not proof: teams whose projects were all on time are rated a little higher'
            : 'No clear difference in ratings between on-time and late teams'}</div>
        <p>{ds.projects_on_time} projects were on time and {ds.projects_not_on_time} were not. The {ds.n_in} teams whose projects were
          all on time average a rating of {num(ds.mean_in)}, against {num(ds.mean_out)} for the {ds.n_out} teams with a project not on
          time ({ds.difference >= 0 ? '+' : ''}{num(ds.difference)}). A difference this large would appear by chance about{' '}
          {dsChance} times in 100.{dsVerdict === 'hint' ? ' Worth tracking as more projects come in.' : ''}</p>
      </div>
      <BarChart horizontal title="Average team rating by project delivery status" format={(v) => num(v)} xLabel="Average latest valid rating (1–5)"
                data={[{ label: `All projects on time (${ds.n_in} teams)`, value: ds.mean_in },
                  { label: `A project not on time (${ds.n_out} teams)`, value: ds.mean_out }]} />
      <div>
        <Table caption="Performance by department" rowKey={(r) => r.department} rows={q.departments} columns={[
          { key: 'department', label: 'Department' }, { key: 'rated_employees', label: 'Rated', align: 'right', format: int },
          { key: 'avg_latest_rating', label: 'Avg rating', align: 'right', format: (v) => num(v) },
          { key: 'high_performer_share', label: 'High performers', align: 'right', format: (v) => pct(v) },
        ]} />
      </div>
      <CorrelationExplainer q6={q} />
      <Notes items={[
        `The link between team ratings and project outcomes is ${strength} and ${q.significant_5pct ? 'reliable' : 'not reliable'} across ${q.teams_with_projects} teams (the statistics behind this are one click away above).`,
        `On-time delivery ${dsVerdict === 'yes' ? 'is clearly linked to' : dsVerdict === 'hint' ? 'gives a weak hint of a link with' : 'shows no clear link with'} team ratings (${num(ds.mean_in)} vs ${num(ds.mean_out)}; chance about ${dsChance} in 100). This is a stronger signal than outcome scores give, but only ${q.teams_with_projects} teams can be compared.`,
        'Do not use individual ratings as a proxy for project success, or the other way round.',
      ]} />
      <Notes title="Assumptions and limitations" tone="caveat" items={[
        'Projects are linked to teams, not employees, so the comparison is only possible at team level.',
        `Only ${q.teams_with_projects} of ${q.teams_total} teams have projects (${q.projects} projects): a small sample. Correlation is not causation.`,
      ]} />
    </Section>
  )
}

function Q7({ s }) {
  const q = s.q7
  // Readiness rows come from the pipeline sorted by meets_required (high → low), then name: reuse that order.
  const rows = q.readiness
  const assessments = rows.reduce((a, r) => a + r.assessed, 0)
  const weighted = (key) => rows.reduce((a, r) => a + r[key] * r.assessed, 0) / assessments   // exact overall share
  const meetsAll = weighted('meets_required'), expertsAll = weighted('expert_share')
  const attention = rows.slice(-3).reverse(), strongest = rows.slice(0, 3)
  const priority = attention[0]
  const dv = q.department_variation
  const levels = [1, 2, 3, 4, 5]
  const mix = q.categories.map((cat) => ({ category: cat, ...Object.fromEntries(levels.map((l) => [l,
    q.level_mix.find((m) => m.competency_category === cat && m.proficiency_level === l)?.share])) }))
  const deptTable = q.departments.map((d) => ({ department: d, ...Object.fromEntries(q.categories.map((c) => [c,
    q.heatmap.find((h) => h.department === d && h.competency_category === c)?.avg_proficiency])) }))
  const compCols = [
    { key: 'competency_name', label: 'Competency' }, { key: 'competency_category', label: 'Category' },
    { key: 'employees_assessed', label: 'Assessed', align: 'right' },
    { key: 'avg_proficiency', label: 'Avg proficiency', align: 'right', format: (v) => num(v) },
    { key: 'avg_required', label: 'Avg required', align: 'right', format: (v) => num(v) },
  ]
  return (
    <Section id="q7" number="7" title="Skills Overview" question={QUESTIONS[7]}>
      <p className="skills-subtitle">How many assessed employees meet the proficiency their role requires, and which skills need attention.</p>
      <div className="skills-cards">
        <SkillCard value={pct(meetsAll, 0)} label="of assessments meet the required proficiency"
                   note={`${int(assessments)} assessments across ${rows.length} competencies`} />
        <SkillCard value={pct(expertsAll, 0)} label={`of assessments are at expert level (${q.expert_min_level}–5)`}
                   note="on the 1–5 proficiency scale" />
      </div>
      <div className="takeaway" role="note">
        <span className="takeaway-label">Key takeaway</span>
        <p><strong>{priority.competency_name}</strong> is the biggest improvement priority: {pct(priority.meets_required, 0)} of assessed
          employees meet the required level, against {pct(meetsAll, 0)} across all skills.</p>
      </div>
      <div className="skills-lists">
        <SkillList title="Skills needing attention" tone="attention" rows={attention} />
        <SkillList title="Strongest skills" tone="strong" rows={strongest} />
      </div>
      <p className="skills-note">Each employee is assessed on {sampleText(s)}, so these figures describe the assessed sample, not every
        employee’s full skill set.</p>
      <Details summary="View detailed analysis">
        <div className="skills-details">
          <h3>Proficiency distribution by competency</h3>
          <Readiness rows={rows} expertMin={q.expert_min_level} />
          <h3>Departments</h3>
          <p className="muted">Average proficiency (1–5) per department and competency category. Shuffle test across{' '}
            {int(dv.permutations)} random reassignments of departments: p = {num(dv.p_value)} —{' '}
            {dv.p_value < 0.05 ? 'departments differ more than chance would explain.' : 'the differences are within normal random variation.'}</p>
          <Table rowKey={(r) => r.department} rows={deptTable} columns={[
            { key: 'department', label: 'Department' },
            ...q.categories.map((c) => ({ key: c, label: c, align: 'right', format: (v) => num(v) })),
          ]} />
          <h3>Job levels</h3>
          <Table rowKey={(r) => r.job_level} rows={q.by_job_level} columns={[
            { key: 'job_level', label: 'Job level' }, { key: 'assessments', label: 'Assessments', align: 'right', format: int },
            { key: 'avg_proficiency', label: 'Avg proficiency', align: 'right', format: (v) => num(v) },
            { key: 'meets_required', label: 'Meet required level', align: 'right', format: (v) => pct(v) },
          ]} />
          <h3>Proficiency level mix by category</h3>
          <Table rowKey={(r) => r.category} rows={mix} columns={[
            { key: 'category', label: 'Category' },
            ...levels.map((l) => ({ key: String(l), label: `Level ${l}`, align: 'right', format: (v) => pct(v) })),
          ]} />
          <div className="grid-2">
            <Table caption="Lowest average proficiency" rowKey={(r) => r.competency_name} rows={q.lowest_competencies} columns={compCols} />
            <Table caption="Highest average proficiency" rowKey={(r) => r.competency_name} rows={q.highest_competencies} columns={compCols} />
          </div>
        </div>
      </Details>
    </Section>
  )
}

function SkillCard({ value, label, note }) {
  return (
    <div className="skill-card">
      <div className="skill-card-value">{value}</div>
      <div className="skill-card-label">{label}</div>
      <div className="skill-card-note">{note}</div>
    </div>
  )
}

/** A short list of competencies with their share meeting the required level as a progress bar. */
function SkillList({ title, tone, rows }) {
  return (
    <div className={`skill-list ${tone}`}>
      <h3>{title}</h3>
      <ul>
        {rows.map((r) => (
          <li key={r.competency_name}>
            <div className="skill-row-head"><span>{r.competency_name}</span><strong>{pct(r.meets_required, 0)}</strong></div>
            <div className="skill-bar" role="progressbar" aria-label={`${r.competency_name}: share meeting the required level`}
                 aria-valuemin={0} aria-valuemax={100} aria-valuenow={Math.round(r.meets_required * 100)}>
              <span style={{ width: `${r.meets_required * 100}%` }} />
            </div>
            <div className="skill-row-note">{r.competency_category} · {int(r.assessed)} assessed</div>
          </li>
        ))}
      </ul>
    </div>
  )
}

function Summary({ s }) {
  const t = Object.fromEntries(s.q1.trend_distribution.map((x) => [x.trend, x.employees]))
  const heat = s.q7.heatmap.map((c) => c.avg_proficiency)
  const [lo, hi] = s.rules.rating_scale
  const rows = [
    [1, 'Performance ratings and review history', `${int(s.q1.high_performers)} high and ${int(s.q1.low_performers)} low performers; ${int(t.improving)} improving, ${int(t.stable)} stable, ${int(t.declining)} declining.`, `High, but only ${s.q1.review_periods.length} periods`],
    [2, 'Critical skill gaps', `${int(s.q2.critical_gaps)} critical gaps (${pct(s.q2.critical_gap_rate, 0)}) held by ${int(s.q2.employees_with_critical_gap)} employees.`, 'High for the assessed sample'],
    [3, 'Promotion-ready high potentials', `${int(s.q3.promotion_ready)} employees under the proposed rule; ${int(s.q3.ready_with_critical_gap)} have a critical gap.`, 'Rule is a proposal (A19)'],
    [4, 'Training completed', `${int(s.q4.totals.distinct_course_completions)} completions and ${int(s.q4.totals.certifications_earned)} certifications; ${int(s.q4.totals.employees_with_completion)} employees completed a course.`, 'High for counts; no funnel rates'],
    [5, 'Attrition risk from performance trends', `${int(s.q5.declining)} declining; ${int(s.q5.bands[0].employees)} in the heuristic High band.`, 'Heuristic, not validated (A20)'],
    [6, 'Performance vs team and project outcomes', `${s.q6.significant_5pct ? `A ${strengthWord(s.q6.pearson_r)} but reliable link` : `No reliable link with outcome scores (the pattern is ${strengthWord(s.q6.pearson_r)})`} across ${s.q6.teams_with_projects} teams; on-time teams rate ${num(s.q6.delivery_status.mean_in)} vs ${num(s.q6.delivery_status.mean_out)} (chance about ${Math.round(s.q6.delivery_status.p_value * 100)} in 100).`, 'Low (small sample)'],
    [7, 'Distribution of skills', `Differences sit at competency level (${pct(Math.min(...s.q7.readiness.map((r) => r.meets_required)), 0)}–${pct(Math.max(...s.q7.readiness.map((r) => r.meets_required)), 0)} meet the required level); departments do not differ beyond chance (averages ${num(Math.min(...heat))}–${num(Math.max(...heat))} on the ${lo}–${hi} scale).`, 'Medium (sampled assessments)'],
  ].map(([n, question, answer, confidence]) => ({ n, question, answer, confidence }))
  return (
    <Section id="summary" number="9" title="Summary, assumptions and limitations">
      <Table rowKey={(r) => r.n} rows={rows} columns={[
        { key: 'n', label: '#' }, { key: 'question', label: 'Question' }, { key: 'answer', label: `Short answer (as of ${s.freshness.as_of})` },
        { key: 'confidence', label: 'Confidence' },
      ]} />
      <Notes title="Decisions behind these numbers" items={[
        `Approved: A1 critical gap = missing at least ${pct(s.rules.critical_gap_ratio, 0)} of the required level; A13 an explicit completion counts.`,
        'Proposed, awaiting review: A4–A7 pre-hire treatment, A19 promotion rule, A20 attrition heuristic, A21 12-month window, A22 “as of” = snapshot date, A23 no funnel rates, A24 performer thresholds.',
      ]} />
      <Notes title="Data limitations to raise with the data owner" tone="caveat" items={[
        `Many HR records are dated before the hire date (${int(s.q1.pre_hire_reviews)} reviews, ${int(s.q2.pre_hire_assessments)} assessments, ${int(s.q3.pre_hire_talent_reviews)} talent reviews, ${int(s.q4.totals.pre_hire_events_excluded)} learning events).`,
        'Learning events show no reliable sequence; there is no record of actual leavers; projects link to teams only; no course-to-competency mapping.',
      ]} />
    </Section>
  )
}

/** "2 of 40 competencies", from the assessments in the data. */
function sampleText(s) {
  const perEmployee = Math.round(s.q2.assessments / Math.max(1, s.q2.employees_assessed))
  return `${perEmployee} of ${s.q2.competencies.length} competencies`
}

/** Skill readiness: one 100% stacked bar per competency (levels 1–5), sorted by the share meeting the required level. */
function Readiness({ rows, expertMin }) {
  const shades = ['#cde2fb', '#9ec5f4', '#3987e5', '#256abf', '#0d366b']
  return (
    <figure className="chart readiness">
      <figcaption>Skill readiness by competency</figcaption>
      <div className="readiness-legend">
        {shades.map((c, i) => <span key={c}><i style={{ background: c }} />Level {i + 1}</span>)}
        <span className="muted">· right: share meeting the required level · experts = level {expertMin}–5</span>
      </div>
      <div className="readiness-rows">
        {rows.map((r) => (
          <div key={r.competency_name} className="readiness-row"
               title={`${r.competency_name} (${r.competency_category}) · ${r.assessed} assessed · ${pct(r.meets_required)} meet the required level · ${pct(r.expert_share)} experts · average ${num(r.avg_proficiency)}`}>
            <span className="readiness-name">{r.competency_name}</span>
            <span className="readiness-bar">
              {shades.map((c, i) => <span key={c} style={{ width: `${r[`level_${i + 1}`] * 100}%`, background: c }} />)}
            </span>
            <span className="readiness-meets">{pct(r.meets_required, 0)}</span>
          </div>
        ))}
      </div>
    </figure>
  )
}
