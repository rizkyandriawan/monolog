import { useEffect, useState } from 'react'
import {
  Heading,
  SimpleGrid,
  Stat,
  StatLabel,
  StatNumber,
  StatHelpText,
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
  VStack,
  HStack,
  useColorModeValue,
} from '@chakra-ui/react'
import type { Topic, Group } from '../api/client'
import { api } from '../api/client'

interface DashboardProps {
  onNavigate: (page: string, data?: unknown) => void
}

export function Dashboard({ onNavigate }: DashboardProps) {
  const [topics, setTopics] = useState<Topic[]>([])
  const [groups, setGroups] = useState<Group[]>([])
  const [pendingCount, setPendingCount] = useState(0)
  const cardBg = useColorModeValue('white', 'gray.800')
  const hoverBg = useColorModeValue('gray.50', 'gray.700')

  useEffect(() => {
    loadData()
    const interval = setInterval(loadData, 5000)
    return () => clearInterval(interval)
  }, [])

  async function loadData() {
    try {
      const [t, g, stats] = await Promise.all([
        api.getTopics(),
        api.getGroups(),
        api.getStats(),
      ])
      setTopics(t)
      setGroups(g)
      setPendingCount(stats.pending)
    } catch (err) {
      console.error('Failed to load dashboard data:', err)
    }
  }

  const totalMessages = topics.reduce((sum, t) => sum + t.latest_offset, 0)

  return (
    <VStack spacing={6} align="stretch">
      <Heading size="lg">Dashboard</Heading>

      {/* Stats Cards */}
      <SimpleGrid columns={{ base: 1, md: 2, lg: 4 }} spacing={4}>
        <Card bg={cardBg}>
          <CardBody>
            <Stat>
              <StatLabel>Topics</StatLabel>
              <StatNumber>{topics.length}</StatNumber>
              <StatHelpText>Active topics</StatHelpText>
            </Stat>
          </CardBody>
        </Card>

        <Card bg={cardBg}>
          <CardBody>
            <Stat>
              <StatLabel>Total Messages</StatLabel>
              <StatNumber>{totalMessages.toLocaleString()}</StatNumber>
              <StatHelpText>Across all topics</StatHelpText>
            </Stat>
          </CardBody>
        </Card>

        <Card bg={cardBg}>
          <CardBody>
            <Stat>
              <StatLabel>Consumer Groups</StatLabel>
              <StatNumber>{groups.length}</StatNumber>
              <StatHelpText>
                {groups.filter(g => g.state === 'Stable').length} stable
              </StatHelpText>
            </Stat>
          </CardBody>
        </Card>

        <Card bg={cardBg}>
          <CardBody>
            <Stat>
              <StatLabel>Pending Fetches</StatLabel>
              <StatNumber>{pendingCount}</StatNumber>
              <StatHelpText>Waiting for data</StatHelpText>
            </Stat>
          </CardBody>
        </Card>
      </SimpleGrid>

      {/* Recent Topics */}
      <Card bg={cardBg}>
        <CardBody>
          <HStack justify="space-between" mb={4}>
            <Heading size="md">Topics</Heading>
            <Text
              color="blue.400"
              cursor="pointer"
              fontSize="sm"
              onClick={() => onNavigate('topics')}
            >
              View all →
            </Text>
          </HStack>

          {topics.length === 0 ? (
            <Text color="gray.500">No topics yet. Create one to get started.</Text>
          ) : (
            <Table size="sm">
              <Thead>
                <Tr>
                  <Th>Name</Th>
                  <Th isNumeric>Messages</Th>
                  <Th>Created</Th>
                </Tr>
              </Thead>
              <Tbody>
                {topics.slice(0, 5).map(topic => (
                  <Tr
                    key={topic.name}
                    cursor="pointer"
                    _hover={{ bg: hoverBg }}
                    onClick={() => onNavigate('topics', { topic: topic.name })}
                  >
                    <Td fontWeight="medium">{topic.name}</Td>
                    <Td isNumeric>{topic.latest_offset.toLocaleString()}</Td>
                    <Td fontSize="sm" color="gray.500">
                      {topic.created_at
                        ? new Date(topic.created_at).toLocaleDateString()
                        : '-'}
                    </Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )}
        </CardBody>
      </Card>

      {/* Consumer Groups */}
      <Card bg={cardBg}>
        <CardBody>
          <HStack justify="space-between" mb={4}>
            <Heading size="md">Consumer Groups</Heading>
            <Text
              color="blue.400"
              cursor="pointer"
              fontSize="sm"
              onClick={() => onNavigate('groups')}
            >
              View all →
            </Text>
          </HStack>

          {groups.length === 0 ? (
            <Text color="gray.500">No consumer groups. Connect a consumer to see groups.</Text>
          ) : (
            <Table size="sm">
              <Thead>
                <Tr>
                  <Th>Group ID</Th>
                  <Th>State</Th>
                  <Th isNumeric>Members</Th>
                  <Th isNumeric>Generation</Th>
                </Tr>
              </Thead>
              <Tbody>
                {groups.slice(0, 5).map(group => (
                  <Tr
                    key={group.id}
                    cursor="pointer"
                    _hover={{ bg: hoverBg }}
                    onClick={() => onNavigate('groups', { group: group.id })}
                  >
                    <Td fontWeight="medium">{group.id}</Td>
                    <Td>
                      <Badge
                        colorScheme={
                          group.state === 'Stable'
                            ? 'green'
                            : group.state === 'Empty'
                            ? 'gray'
                            : 'yellow'
                        }
                      >
                        {group.state}
                      </Badge>
                    </Td>
                    <Td isNumeric>{group.members}</Td>
                    <Td isNumeric>{group.generation}</Td>
                  </Tr>
                ))}
              </Tbody>
            </Table>
          )}
        </CardBody>
      </Card>
    </VStack>
  )
}
