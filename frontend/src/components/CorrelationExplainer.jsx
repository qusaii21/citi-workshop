// Q6 explanation: how r, rho, t and the p-value are calculated, with every step computed live from the
// published team data (the same numbers the pipeline publishes), and what they mean for ACME.
import { useEffect, useRef, useState } from 'react'
import { num, pct } from './format.js'
import { mean, pearson, pValue, ranks, strengthWord as strength, tStatistic } from './stats.js'

export default function CorrelationExplainer({ q6 }) {
  const [open, setOpen] = useState(false)
  return (
    <>
      <button type="button" className="explain-button" onClick={() => setOpen(true)}>
        Show the statistics behind this (r, ρ, t, p-value, permutation test) — formulas and a step-by-step explanation
      </button>
      {open && <Modal title="How the team correlation is calculated" onClose={() => setOpen(false)}><Explanation q6={q6} /></Modal>}
    </>
  )
}

function Modal({ title, onClose, children }) {
  const panel = useRef(null)
  useEffect(() => {
    const onKey = (e) => { if (e.key === 'Escape') onClose() }
    window.addEventListener('keydown', onKey)
    panel.current?.focus()
    return () => window.removeEventListener('keydown', onKey)
  }, [onClose])
  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal" role="dialog" aria-modal="true" aria-label={title} tabIndex={-1} ref={panel}
           onClick={(e) => e.stopPropagation()}>
        <div className="modal-head">
          <h2>{title}</h2>
          <button type="button" className="close" onClick={onClose} aria-label="Close">×</button>
        </div>
        <div className="modal-body">{children}</div>
      </div>
    </div>
  )
}

function Explanation({ q6 }) {
  const teams = q6.teams
  const xs = teams.map((t) => t.avg_latest_valid_rating), ys = teams.map((t) => t.avg_project_outcome_score)
  const n = teams.length, df = n - 2
  const p = pearson(xs, ys)
  const rx = ranks(xs), ry = ranks(ys)
  const sp = pearson(rx, ry)
  const t = tStatistic(p.r, n), pv = pValue(t, df), tc = q6.t_critical_5pct
  const significant = Math.abs(t) >= tc
  const rows = teams.map((team, i) => ({ team, x: xs[i], y: ys[i], dx: xs[i] - p.mx, dy: ys[i] - p.my, rx: rx[i], ry: ry[i] }))
  const chance = Math.round(pv * 100)
  const ds = q6.delivery_status

  return (
    <div className="explain">
      <section>
        <h3>The question in one sentence</h3>
        <p>Do teams whose people have <strong>higher performance ratings</strong> also deliver <strong>better project outcomes</strong>?
          We compare the two for the <strong>{n} teams that have projects</strong> (projects are linked to teams, not to people, so this is
          only possible at team level).</p>
        <ul>
          <li><strong>x</strong> = a team’s average latest valid rating (1–5) — average x̄ = <strong>{num(p.mx, 3)}</strong></li>
          <li><strong>y</strong> = the average outcome score of the team’s projects — average ȳ = <strong>{num(p.my, 2)}</strong></li>
        </ul>
      </section>

      <section>
        <h3>1. Pearson’s r — do the two move together in a straight line?</h3>
        <div className="formula">
          r = Σ (x − x̄)(y − ȳ) / √( Σ (x − x̄)² × Σ (y − ȳ)² )
        </div>
        <p><strong>In plain words:</strong> for each team, check whether it is above or below average on rating (x − x̄) and on outcome
          (y − ȳ). If teams that are above average on one are usually above average on the other, the products are mostly positive and r is
          positive. Dividing by the spreads scales r to lie between −1 and +1.</p>
        <div className="formula worked">
          Σ (x − x̄)(y − ȳ) = {num(p.sxy, 3)}<br />
          Σ (x − x̄)² = {num(p.sxx, 3)} &nbsp;·&nbsp; Σ (y − ȳ)² = {num(p.syy, 1)}<br />
          r = {num(p.sxy, 3)} / √({num(p.sxx, 3)} × {num(p.syy, 1)}) = <strong>{num(p.r, 3)}</strong>
        </div>
        <Scale value={p.r} />
        <p><strong>What it means here:</strong> r = {num(p.r, 2)} is a <strong>{strength(p.r)} {p.r >= 0 ? 'positive' : 'negative'}</strong> relationship.
          r² = {pct(p.r * p.r)}: team ratings “explain” only about {pct(p.r * p.r, 0)} of the differences in project outcomes; the other{' '}
          {pct(1 - p.r * p.r, 0)} comes from other things.</p>
      </section>

      <section>
        <h3>2. Spearman’s ρ (rho) — the same idea, using ranks</h3>
        <div className="formula">ρ = Pearson’s r computed on the ranks of x and the ranks of y</div>
        <p><strong>In plain words:</strong> rank the teams from lowest to highest rating (1 … {n}), and separately from lowest to highest
          outcome. Teams with exactly the same value share the average of their ranks. Then compute r on those ranks. This only asks “do
          higher-rated teams tend to rank higher on outcomes?” — it does not need a straight line and is not pulled around by one extreme
          team.</p>
        <div className="formula worked">ρ = r(rank x, rank y) = <strong>{num(sp.r, 3)}</strong></div>
        <p><strong>What it means here:</strong> ρ = {num(sp.r, 2)} is close to r = {num(p.r, 2)}, so the weak result is not caused by an
          outlier or a curved pattern: the ordering of teams barely lines up either.</p>
      </section>

      <section>
        <h3>3. The t statistic — could this just be chance?</h3>
        <div className="formula">t = r × √( (n − 2) / (1 − r²) )</div>
        <div className="formula worked">
          t = {num(p.r, 4)} × √( {df} / (1 − {num(p.r, 4)}²) ) = <strong>{num(t, 3)}</strong>
        </div>
        <p><strong>In plain words:</strong> even if there were <em>no</em> link at all, {n} random teams would rarely give exactly r = 0. The
          t statistic measures how big our r is compared with the wobble you expect from chance with this many teams (n − 2 = {df} “degrees
          of freedom”).</p>
        <p>The usual rule: the result is <strong>statistically significant at 5%</strong> if |t| is at least <strong>{num(tc, 3)}</strong>{' '}
          (Student’s t table, {df} degrees of freedom). Here |t| = {num(Math.abs(t), 2)} is {significant ? 'above' : 'well below'} that.</p>
      </section>

      <section>
        <h3>4. The p-value — how often would chance alone do this?</h3>
        <div className="formula">p = probability of seeing |t| ≥ {num(Math.abs(t), 2)} if there were no real relationship (two-sided, {df} degrees of freedom)</div>
        <div className="formula worked">p = <strong>{num(pv, 3)}</strong> &nbsp;(significant if p &lt; 0.05)</div>
        <p><strong>In plain words:</strong> if team ratings and project outcomes were completely unrelated, a correlation at least this strong
          would still show up by chance about <strong>{chance} times in 100</strong>. {pv < 0.05
          ? 'That is rare enough to treat the link as real.'
          : 'That is far too common to treat the link as real: the data cannot tell this apart from no relationship.'}</p>
      </section>

      <section>
        <h3>5. On-time delivery — the permutation test</h3>
        <p>The same {n} teams can also be split by delivery: {ds.n_in} teams whose projects were <strong>all on time</strong>, and{' '}
          {ds.n_out} teams with <strong>a project not on time</strong>.</p>
        <div className="formula">difference = average rating of the all-on-time teams − average rating of the other teams</div>
        <div className="formula worked">difference = {num(ds.mean_in, 3)} − {num(ds.mean_out, 3)} = <strong>{ds.difference >= 0 ? '+' : ''}{num(ds.difference, 3)}</strong></div>
        <p><strong>In plain words:</strong> is that gap real, or luck? The test pretends delivery status has nothing to do with ratings. It
          randomly re-labels which {ds.n_in} of the {n} teams are “all on time”, recomputes the difference, and repeats this{' '}
          <strong>{Number(ds.permutations).toLocaleString('en-US')} times</strong> (with a fixed random seed, so the result is reproducible).</p>
        <div className="formula">p = share of shuffles whose difference is at least {num(Math.abs(ds.difference), 3)} (in either direction)</div>
        <div className="formula worked">p = <strong>{num(ds.p_value, 3)}</strong> &nbsp;(significant if p &lt; 0.05)</div>
        <p><strong>What it means here:</strong> a gap this big shows up by chance about {Math.round(ds.p_value * 100)} times in 100.
          {ds.p_value < 0.05 ? ' That is rare enough to call the link real.' : ' That is a hint — more signal than the outcome scores give — but not proof with this few teams.'}{' '}
          A permutation test is used here because it makes no assumption about the shape of the data, which matters with groups of
          {' '}{ds.n_in} and {ds.n_out} teams.</p>
      </section>

      <section className="explain-conclusion">
        <h3>What this means for ACME</h3>
        <ul>
          <li><strong>{significant ? 'There is evidence' : 'There is no evidence'}</strong> that teams with higher-rated people deliver better
            project outcomes (r = {num(p.r, 2)}, ρ = {num(sp.r, 2)}, p = {num(pv, 2)}, n = {n}).</li>
          <li>On-time delivery gives {ds.p_value < 0.05 ? 'a clear' : 'only a weak'} signal: all-on-time teams rate {num(ds.mean_in)} vs{' '}
            {num(ds.mean_out)} (p = {num(ds.p_value, 2)}). {ds.p_value < 0.05 ? '' : 'Worth tracking as more projects come in.'}</li>
          <li>Do not use individual ratings to predict project success, or project results to judge individuals.</li>
          <li>The sample is small: only {n} of {q6.teams_total} teams have projects ({q6.projects} projects). A real but modest link could be
            hidden by that. Linking projects to the people who worked on them would allow a much stronger analysis.</li>
          <li>Correlation is not causation: even a strong r would not show that ratings <em>cause</em> outcomes.</li>
        </ul>
        <p className="muted">These values are computed in your browser from the {n} teams below and match the pipeline’s published values
          (r = {num(q6.pearson_r, 3)}, ρ = {num(q6.spearman_rho, 3)}, t = {num(q6.t_statistic, 3)}).</p>
      </section>

      <details className="details">
        <summary>Show the {n} teams and every step</summary>
        <div className="table-wrap">
          <table>
            <thead>
              <tr><th>Team</th><th className="num">x rating</th><th className="num">y outcome</th><th className="num">x − x̄</th>
                <th className="num">y − ȳ</th><th className="num">(x − x̄)(y − ȳ)</th><th className="num">rank x</th><th className="num">rank y</th></tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.team.team_id}>
                  <td>{row.team.team_name}</td><td className="num">{num(row.x, 3)}</td><td className="num">{num(row.y, 1)}</td>
                  <td className="num">{num(row.dx, 3)}</td><td className="num">{num(row.dy, 2)}</td>
                  <td className="num">{num(row.dx * row.dy, 3)}</td><td className="num">{num(row.rx, 1)}</td><td className="num">{num(row.ry, 1)}</td>
                </tr>
              ))}
              <tr className="total">
                <td>Total / average</td><td className="num">x̄ = {num(mean(xs), 3)}</td><td className="num">ȳ = {num(mean(ys), 2)}</td>
                <td className="num">0</td><td className="num">0</td><td className="num">{num(p.sxy, 3)}</td><td></td><td></td>
              </tr>
            </tbody>
          </table>
        </div>
      </details>
    </div>
  )
}

/** Where r sits on the −1 … +1 scale. */
function Scale({ value }) {
  const left = ((value + 1) / 2) * 100
  return (
    <div className="r-scale" aria-label={`r = ${num(value, 2)} on a scale from −1 to +1`}>
      <div className="r-track"><span className="r-marker" style={{ left: `${left}%` }} /></div>
      <div className="r-labels"><span>−1 perfect negative</span><span>0 no relationship</span><span>+1 perfect positive</span></div>
    </div>
  )
}
