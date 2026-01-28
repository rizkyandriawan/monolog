const API_BASE = '/api'

export interface Topic {
  name: string
  latest_offset: number
  earliest_offset?: number
  created_at?: string
}

export interface Message {
  offset: number
  timestamp: number
  key: string
  value: string
  codec: number
}

export interface Group {
  id: string
  state: string
  generation: number
  members: number
  offsets?: Record<string, number>
}

export interface Stats {
  topics: number
  groups: number
  pending: number
}

export interface PendingRequest {
  topic: string
  partition: number
  offset: number
  deadline: string
  correlation_id: number
}

class ApiClient {
  private token?: string

  setToken(token: string) {
    this.token = token
  }

  private headers(): HeadersInit {
    const h: HeadersInit = {
      'Content-Type': 'application/json',
    }
    if (this.token) {
      h['Authorization'] = `Bearer ${this.token}`
    }
    return h
  }

  async getStats(): Promise<Stats> {
    const res = await fetch(`${API_BASE}/stats`, { headers: this.headers() })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json()
  }

  async getTopics(): Promise<Topic[]> {
    const res = await fetch(`${API_BASE}/topics`, { headers: this.headers() })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json() ?? []
  }

  async getTopic(name: string): Promise<Topic> {
    const res = await fetch(`${API_BASE}/topics/${name}`, { headers: this.headers() })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json()
  }

  async createTopic(name: string): Promise<void> {
    const res = await fetch(`${API_BASE}/topics`, {
      method: 'POST',
      headers: this.headers(),
      body: JSON.stringify({ name }),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
  }

  async deleteTopic(name: string): Promise<void> {
    const res = await fetch(`${API_BASE}/topics/${name}`, {
      method: 'DELETE',
      headers: this.headers(),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
  }

  async getMessages(topic: string, offset = 0, limit = 50): Promise<Message[]> {
    const res = await fetch(
      `${API_BASE}/topics/${topic}/messages?offset=${offset}&limit=${limit}`,
      { headers: this.headers() }
    )
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json() ?? []
  }

  async produceMessage(topic: string, key: string, value: string): Promise<{ offset: number }> {
    const res = await fetch(`${API_BASE}/topics/${topic}/messages`, {
      method: 'POST',
      headers: this.headers(),
      body: JSON.stringify({ key, value }),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json()
  }

  async getGroups(): Promise<Group[]> {
    const res = await fetch(`${API_BASE}/groups`, { headers: this.headers() })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json() ?? []
  }

  async getGroup(id: string): Promise<Group> {
    const res = await fetch(`${API_BASE}/groups/${id}`, { headers: this.headers() })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json()
  }

  async deleteGroup(id: string): Promise<void> {
    const res = await fetch(`${API_BASE}/groups/${id}`, {
      method: 'DELETE',
      headers: this.headers(),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
  }

  async getGroupOffset(groupId: string, topic: string): Promise<number> {
    const res = await fetch(`${API_BASE}/groups/${groupId}/offsets/${topic}`, {
      headers: this.headers(),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    const data = await res.json()
    return data.offset
  }

  async setGroupOffset(groupId: string, topic: string, offset: number): Promise<void> {
    const res = await fetch(`${API_BASE}/groups/${groupId}/offsets/${topic}`, {
      method: 'POST',
      headers: this.headers(),
      body: JSON.stringify({ offset }),
    })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
  }

  async getPending(): Promise<PendingRequest[]> {
    const res = await fetch(`${API_BASE}/pending`, { headers: this.headers() })
    if (!res.ok) throw new Error(`HTTP ${res.status}`)
    return res.json() ?? []
  }

  async checkHealth(): Promise<boolean> {
    try {
      const res = await fetch('/health')
      return res.ok
    } catch {
      return false
    }
  }
}

export const api = new ApiClient()
