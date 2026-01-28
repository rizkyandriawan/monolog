import { useEffect, useState } from 'react'
import {
  Heading,
  VStack,
  HStack,
  Card,
  CardBody,
  Table,
  Thead,
  Tbody,
  Tr,
  Th,
  Td,
  Badge,
  Text,
  Code,
  useColorModeValue,
  Progress,
} from '@chakra-ui/react'
import type { PendingRequest } from '../api/client'
import { api } from '../api/client'

export function Pending() {
  const [pending, setPending] = useState<PendingRequest[]>([])
  const [loading, setLoading] = useState(true)
  const cardBg = useColorModeValue('white', 'gray.800')

  useEffect(() => {
    loadPending()
    const interval = setInterval(loadPending, 1000)
    return () => clearInterval(interval)
  }, [])

  async function loadPending() {
    try {
      const p = await api.getPending()
      setPending(p)
    } catch (err) {
      console.error('Failed to load pending:', err)
    } finally {
      setLoading(false)
    }
  }

  function getTimeRemaining(deadline: string): { ms: number; text: string; pct: number } {
    const deadlineTime = new Date(deadline).getTime()
    const now = Date.now()
    const remaining = deadlineTime - now
    const totalMs = 30000 // assume 30s timeout

    if (remaining <= 0) {
      return { ms: 0, text: 'Expired', pct: 100 }
    }

    const seconds = Math.floor(remaining / 1000)
    const ms = remaining % 1000
    const pct = Math.max(0, Math.min(100, ((totalMs - remaining) / totalMs) * 100))

    return {
      ms: remaining,
      text: `${seconds}.${Math.floor(ms / 100)}s`,
      pct,
    }
  }

  return (
    <VStack spacing={6} align="stretch">
      <HStack justify="space-between">
        <Heading size="lg">Pending Fetch Requests</Heading>
        <Badge colorScheme={pending.length > 0 ? 'blue' : 'gray'} fontSize="md" px={3} py={1}>
          {pending.length} waiting
        </Badge>
      </HStack>

      <Text color="gray.500">
        These are long-poll fetch requests waiting for new data. When a producer sends a message,
        waiting consumers will be notified.
      </Text>

      <Card bg={cardBg}>
        <CardBody>
          {loading ? (
            <Text color="gray.500" py={8} textAlign="center">
              Loading...
            </Text>
          ) : pending.length === 0 ? (
            <VStack py={12} spacing={4}>
              <Text fontSize="4xl">⏳</Text>
              <Text color="gray.500">No pending fetch requests</Text>
              <Text color="gray.500" fontSize="sm">
                When consumers connect and wait for data, they'll appear here
              </Text>
            </VStack>
          ) : (
            <Table size="sm">
              <Thead>
                <Tr>
                  <Th>Topic</Th>
                  <Th>Partition</Th>
                  <Th isNumeric>Offset</Th>
                  <Th isNumeric>Correlation ID</Th>
                  <Th w="200px">Time Remaining</Th>
                </Tr>
              </Thead>
              <Tbody>
                {pending.map((req, idx) => {
                  const time = getTimeRemaining(req.deadline)
                  return (
                    <Tr key={idx}>
                      <Td>
                        <Code fontSize="sm">{req.topic}</Code>
                      </Td>
                      <Td>
                        <Badge>{req.partition}</Badge>
                      </Td>
                      <Td isNumeric>{req.offset}</Td>
                      <Td isNumeric>
                        <Code fontSize="xs">{req.correlation_id}</Code>
                      </Td>
                      <Td>
                        <VStack align="stretch" spacing={1}>
                          <HStack justify="space-between">
                            <Text fontSize="xs" color={time.ms < 5000 ? 'orange.400' : 'gray.500'}>
                              {time.text}
                            </Text>
                          </HStack>
                          <Progress
                            value={time.pct}
                            size="xs"
                            colorScheme={time.ms < 5000 ? 'orange' : 'blue'}
                            borderRadius="full"
                          />
                        </VStack>
                      </Td>
                    </Tr>
                  )
                })}
              </Tbody>
            </Table>
          )}
        </CardBody>
      </Card>
    </VStack>
  )
}
