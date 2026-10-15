// Statistics for the Q6 explanation, computed in the browser from the published team data.
// They follow the pipeline (pandas): Pearson r, Spearman rho = Pearson on average ranks,
// t = r * sqrt((n - 2) / (1 - r^2)), and the two-sided p-value of t with n - 2 degrees of freedom.

/** Plain-language size of a correlation. */
export const strengthWord = (r) => {
  const a = Math.abs(r)
  return a < 0.1 ? 'negligible' : a < 0.3 ? 'weak' : a < 0.5 ? 'moderate' : 'strong'
}

export const mean = (xs) => xs.reduce((a, x) => a + x, 0) / xs.length

/** Pearson correlation with its intermediate sums (for the worked example). */
export function pearson(xs, ys) {
  const mx = mean(xs), my = mean(ys)
  let sxy = 0, sxx = 0, syy = 0
  xs.forEach((x, i) => {
    const dx = x - mx, dy = ys[i] - my
    sxy += dx * dy; sxx += dx * dx; syy += dy * dy
  })
  return { r: sxy / Math.sqrt(sxx * syy), mx, my, sxy, sxx, syy }
}

/** Ranks 1..n, ties get the average of the ranks they span (pandas' default "average"). */
export function ranks(xs) {
  const order = xs.map((x, i) => [x, i]).sort((a, b) => a[0] - b[0])
  const out = new Array(xs.length)
  for (let i = 0; i < order.length;) {
    let j = i
    while (j + 1 < order.length && order[j + 1][0] === order[i][0]) j++
    for (let k = i; k <= j; k++) out[order[k][1]] = (i + j) / 2 + 1
    i = j + 1
  }
  return out
}

export const tStatistic = (r, n) => r * Math.sqrt((n - 2) / (1 - r * r))

/** Two-sided p-value of a t statistic with df degrees of freedom: I_{df/(df+t^2)}(df/2, 1/2). */
export function pValue(t, df) {
  return incompleteBeta(df / (df + t * t), df / 2, 0.5)
}

function lnGamma(z) {  // Lanczos approximation
  const g = [76.18009172947146, -86.50532032941677, 24.01409824083091, -1.231739572450155,
    0.1208650973866179e-2, -0.5395239384953e-5]
  let x = z, y = z, tmp = x + 5.5
  tmp -= (x + 0.5) * Math.log(tmp)
  let ser = 1.000000000190015
  for (const c of g) ser += c / ++y
  return -tmp + Math.log(2.5066282746310005 * ser / x)
}

/** Regularized incomplete beta I_x(a, b) (continued fraction, Numerical Recipes). */
function incompleteBeta(x, a, b) {
  if (x <= 0) return 0
  if (x >= 1) return 1
  const front = Math.exp(lnGamma(a + b) - lnGamma(a) - lnGamma(b) + a * Math.log(x) + b * Math.log(1 - x))
  return x < (a + 1) / (a + b + 2) ? front * betaFraction(x, a, b) / a : 1 - front * betaFraction(1 - x, b, a) / b
}

function betaFraction(x, a, b) {
  const tiny = 1e-30
  let c = 1, d = 1 - (a + b) * x / (a + 1)
  d = Math.abs(d) < tiny ? tiny : d
  d = 1 / d
  let h = d
  for (let m = 1; m <= 200; m++) {
    const m2 = 2 * m
    let aa = m * (b - m) * x / ((a + m2 - 1) * (a + m2))
    d = 1 + aa * d; d = Math.abs(d) < tiny ? tiny : d
    c = 1 + aa / c; c = Math.abs(c) < tiny ? tiny : c
    d = 1 / d; h *= d * c
    aa = -(a + m) * (a + b + m) * x / ((a + m2) * (a + m2 + 1))
    d = 1 + aa * d; d = Math.abs(d) < tiny ? tiny : d
    c = 1 + aa / c; c = Math.abs(c) < tiny ? tiny : c
    d = 1 / d
    const delta = d * c
    h *= delta
    if (Math.abs(delta - 1) < 1e-12) break
  }
  return h
}
