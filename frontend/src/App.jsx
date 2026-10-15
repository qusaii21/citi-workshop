import Dashboard from './pages/Dashboard.jsx'
import Presentation from './pages/Presentation.jsx'

// Two pages: the dashboard (/) and the project walkthrough (/presentation).
export default function App() {
  const path = window.location.pathname.replace(/\/+$/, '')
  return path === '/presentation' ? <Presentation /> : <Dashboard />
}
