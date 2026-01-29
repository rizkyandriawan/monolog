import { useState, useEffect } from 'react'
import { Routes, Route, useNavigate, useLocation } from 'react-router-dom'
import { Layout } from './components/Layout'
import { Dashboard } from './pages/Dashboard'
import { Topics } from './pages/Topics'
import { Groups } from './pages/Groups'
import { Pending } from './pages/Pending'
import { Actions } from './pages/Actions'
import { api } from './api/client'

function App() {
  const [stats, setStats] = useState({ topics: 0, groups: 0, pending: 0 })
  const [connected, setConnected] = useState(false)
  const navigate = useNavigate()
  const location = useLocation()

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

  function handleNavigate(page: string, data?: { topic?: string; group?: string }) {
    switch (page) {
      case 'dashboard':
        navigate('/')
        break
      case 'topics':
        if (data?.topic) {
          navigate(`/topics/${data.topic}`)
        } else {
          navigate('/topics')
        }
        break
      case 'groups':
        if (data?.group) {
          navigate(`/groups/${data.group}`)
        } else {
          navigate('/groups')
        }
        break
      case 'pending':
        navigate('/pending')
        break
      case 'actions':
        navigate('/actions')
        break
      default:
        navigate('/')
    }
  }

  // Determine current page from location
  const getCurrentPage = () => {
    const path = location.pathname
    if (path.startsWith('/topics')) return 'topics'
    if (path.startsWith('/groups')) return 'groups'
    if (path === '/pending') return 'pending'
    if (path === '/actions') return 'actions'
    return 'dashboard'
  }

  return (
    <Layout
      currentPage={getCurrentPage()}
      onNavigate={handleNavigate}
      stats={stats}
      connected={connected}
    >
      <Routes>
        <Route path="/" element={<Dashboard onNavigate={handleNavigate} />} />
        <Route path="/topics" element={<Topics />} />
        <Route path="/topics/:topicName" element={<Topics />} />
        <Route path="/topics/:topicName/:offset" element={<Topics />} />
        <Route path="/groups" element={<Groups />} />
        <Route path="/groups/:groupId" element={<Groups />} />
        <Route path="/pending" element={<Pending />} />
        <Route path="/actions" element={<Actions />} />
      </Routes>
    </Layout>
  )
}

export default App
