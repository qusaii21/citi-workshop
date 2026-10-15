// Client for the talent dashboard API (backend/talent-dashboard), served by CloudFront under /api.
// VITE_API_URL is set by bin/deploy-frontend.sh; without it the page calls its own origin.
const BASE = `${(import.meta.env.VITE_API_URL || '').replace(/\/$/, '')}/api/talent-dashboard`

async function get(path, params) {
  const query = params ? `?${new URLSearchParams(Object.entries(params).filter(([, v]) => v !== '' && v != null))}` : ''
  const response = await fetch(`${BASE}${path}${query}`)
  const body = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(body.error || `request failed (${response.status})`)
  return body
}

export const fetchSummary = () => get('/summary')
export const fetchEmployees = (params) => get('/employees', params)
export const fetchEmployee = (id) => get(`/employees/${id}`)
export const fetchLake = () => get('/lake')
export const fetchLakeTable = (layer, table, params) => get(`/lake/${layer}/${encodeURIComponent(table)}`, params)
