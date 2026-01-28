import { useState, useEffect } from 'react'
import { Layout } from './components/Layout'
import { Dashboard } from './pages/Dashboard'
import { Topics } from './pages/Topics'
import { Groups } from './pages/Groups'
import { Pending } from './pages/Pending'
import { Actions } from './pages/Actions'
import { api } from './api/client'

function App() {
  const [currentPage, setCurrentPage] = useState('dashboard')
  const [pageData, setPageData] = useState<unknown>(null)
  const [stats, setStats] = useState({ topics: 0, groups: 0, pending: 0 })
  const [connected, setConnected] = useState(false)

  useEffect(() => {
    checkConnection()
    loadStats()

    const interval = setInterval(() => {
      checkConnection()
      loadStats()
    }, 5000)

    return () => clearInterval(interval)
  }, [])

  async function checkConnection() {
    const ok = await api.checkHealth()
    setConnected(ok)
  }

  async function loadStats() {
    try {
      const s = await api.getStats()
      setStats(s)
    } catch {
      // Server might be down
    }
  }

  function handleNavigate(page: string, data?: unknown) {
    setCurrentPage(page)
    setPageData(data)
  }

  function renderPage() {
    switch (currentPage) {
      case 'dashboard':
        return <Dashboard onNavigate={handleNavigate} />
      case 'topics':
        return <Topics initialTopic={(pageData as { topic?: string })?.topic} />
      case 'groups':
        return <Groups initialGroup={(pageData as { group?: string })?.group} />
      case 'pending':
        return <Pending />
      case 'actions':
        return <Actions />
      default:
        return <Dashboard onNavigate={handleNavigate} />
    }
  }

  return (
    <Layout
      currentPage={currentPage}
      onNavigate={handleNavigate}
      stats={stats}
      connected={connected}
    >
      {renderPage()}
    </Layout>
  )
}

export default App
