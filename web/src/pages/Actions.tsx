import { useState } from 'react'
import {
  Box,
  Heading,
  VStack,
  HStack,
  Card,
  CardBody,
  Button,
  Input,
  Textarea,
  FormControl,
  FormLabel,
  SimpleGrid,
  Text,
  Code,
  useToast,
  useColorModeValue,
  Divider,
  Badge,
} from '@chakra-ui/react'
import { api } from '../api/client'

export function Actions() {
  const [quickTopic, setQuickTopic] = useState('')
  const [quickMessage, setQuickMessage] = useState('')
  const [bulkTopic, setBulkTopic] = useState('')
  const [bulkCount, setBulkCount] = useState('100')
  const [bulkPrefix, setBulkPrefix] = useState('test-message')
  const [loading, setLoading] = useState<string | null>(null)

  const toast = useToast()
  const cardBg = useColorModeValue('white', 'gray.800')

  async function handleQuickProduce() {
    if (!quickTopic.trim()) {
      toast({ title: 'Topic name required', status: 'warning' })
      return
    }

    setLoading('quick')
    try {
      // Create topic if it doesn't exist
      try {
        await api.createTopic(quickTopic.trim())
      } catch {
        // Topic might already exist, that's fine
      }

      const result = await api.produceMessage(
        quickTopic.trim(),
        '',
        quickMessage || `Hello from Monolog UI at ${new Date().toISOString()}`
      )
      toast({
        title: 'Message produced',
        description: `Offset: ${result.offset}`,
        status: 'success',
      })
    } catch {
      toast({ title: 'Failed to produce', status: 'error' })
    } finally {
      setLoading(null)
    }
  }

  async function handleBulkProduce() {
    if (!bulkTopic.trim()) {
      toast({ title: 'Topic name required', status: 'warning' })
      return
    }

    const count = parseInt(bulkCount, 10)
    if (isNaN(count) || count < 1 || count > 10000) {
      toast({ title: 'Count must be 1-10000', status: 'warning' })
      return
    }

    setLoading('bulk')
    try {
      // Create topic if it doesn't exist
      try {
        await api.createTopic(bulkTopic.trim())
      } catch {
        // Topic might already exist
      }

      let lastOffset = 0
      for (let i = 0; i < count; i++) {
        const result = await api.produceMessage(
          bulkTopic.trim(),
          `key-${i}`,
          JSON.stringify({
            index: i,
            prefix: bulkPrefix,
            timestamp: Date.now(),
            message: `${bulkPrefix}-${i}`,
          })
        )
        lastOffset = result.offset
      }

      toast({
        title: `${count} messages produced`,
        description: `Last offset: ${lastOffset}`,
        status: 'success',
      })
    } catch {
      toast({ title: 'Failed to produce bulk messages', status: 'error' })
    } finally {
      setLoading(null)
    }
  }

  async function handleCreateTestData() {
    setLoading('testdata')
    try {
      // Create some test topics
      const testTopics = ['orders', 'users', 'events', 'logs']

      for (const topic of testTopics) {
        try {
          await api.createTopic(topic)
        } catch {
          // Topic might already exist
        }

        // Add some sample messages
        for (let i = 0; i < 10; i++) {
          await api.produceMessage(
            topic,
            `${topic}-key-${i}`,
            JSON.stringify({
              id: `${topic}-${i}`,
              type: topic,
              timestamp: Date.now(),
              data: { sample: true, index: i },
            })
          )
        }
      }

      toast({
        title: 'Test data created',
        description: `Created ${testTopics.length} topics with sample messages`,
        status: 'success',
      })
    } catch {
      toast({ title: 'Failed to create test data', status: 'error' })
    } finally {
      setLoading(null)
    }
  }

  return (
    <VStack spacing={6} align="stretch">
      <Heading size="lg">Quick Actions</Heading>

      <SimpleGrid columns={{ base: 1, lg: 2 }} spacing={6}>
        {/* Quick Produce */}
        <Card bg={cardBg}>
          <CardBody>
            <VStack spacing={4} align="stretch">
              <HStack>
                <Text fontSize="2xl">⚡</Text>
                <Heading size="md">Quick Produce</Heading>
              </HStack>
              <Text color="gray.500" fontSize="sm">
                Send a single message to a topic (creates topic if needed)
              </Text>

              <FormControl>
                <FormLabel>Topic</FormLabel>
                <Input
                  placeholder="my-topic"
                  value={quickTopic}
                  onChange={e => setQuickTopic(e.target.value)}
                />
              </FormControl>

              <FormControl>
                <FormLabel>Message (optional)</FormLabel>
                <Textarea
                  placeholder="Leave empty for auto-generated message"
                  value={quickMessage}
                  onChange={e => setQuickMessage(e.target.value)}
                  rows={3}
                />
              </FormControl>

              <Button
                colorScheme="blue"
                onClick={handleQuickProduce}
                isLoading={loading === 'quick'}
              >
                Produce Message
              </Button>
            </VStack>
          </CardBody>
        </Card>

        {/* Bulk Produce */}
        <Card bg={cardBg}>
          <CardBody>
            <VStack spacing={4} align="stretch">
              <HStack>
                <Text fontSize="2xl">📦</Text>
                <Heading size="md">Bulk Produce</Heading>
              </HStack>
              <Text color="gray.500" fontSize="sm">
                Generate multiple test messages for load testing
              </Text>

              <FormControl>
                <FormLabel>Topic</FormLabel>
                <Input
                  placeholder="test-topic"
                  value={bulkTopic}
                  onChange={e => setBulkTopic(e.target.value)}
                />
              </FormControl>

              <HStack>
                <FormControl>
                  <FormLabel>Count</FormLabel>
                  <Input
                    type="number"
                    value={bulkCount}
                    onChange={e => setBulkCount(e.target.value)}
                    max={10000}
                    min={1}
                  />
                </FormControl>

                <FormControl>
                  <FormLabel>Prefix</FormLabel>
                  <Input
                    value={bulkPrefix}
                    onChange={e => setBulkPrefix(e.target.value)}
                  />
                </FormControl>
              </HStack>

              <Button
                colorScheme="blue"
                onClick={handleBulkProduce}
                isLoading={loading === 'bulk'}
              >
                Produce {bulkCount} Messages
              </Button>
            </VStack>
          </CardBody>
        </Card>

        {/* Test Data */}
        <Card bg={cardBg}>
          <CardBody>
            <VStack spacing={4} align="stretch">
              <HStack>
                <Text fontSize="2xl">🧪</Text>
                <Heading size="md">Create Test Data</Heading>
              </HStack>
              <Text color="gray.500" fontSize="sm">
                Create sample topics with test messages for exploration
              </Text>

              <Box>
                <Text fontSize="sm" mb={2}>
                  Will create:
                </Text>
                <HStack flexWrap="wrap" spacing={2}>
                  {['orders', 'users', 'events', 'logs'].map(t => (
                    <Badge key={t} colorScheme="blue">
                      {t}
                    </Badge>
                  ))}
                </HStack>
                <Text fontSize="xs" color="gray.500" mt={2}>
                  Each topic gets 10 sample messages
                </Text>
              </Box>

              <Button
                colorScheme="green"
                onClick={handleCreateTestData}
                isLoading={loading === 'testdata'}
              >
                Create Test Data
              </Button>
            </VStack>
          </CardBody>
        </Card>

        {/* Connection Info */}
        <Card bg={cardBg}>
          <CardBody>
            <VStack spacing={4} align="stretch">
              <HStack>
                <Text fontSize="2xl">🔌</Text>
                <Heading size="md">Connection Info</Heading>
              </HStack>
              <Text color="gray.500" fontSize="sm">
                Use these settings to connect your Kafka clients
              </Text>

              <Box>
                <Text fontWeight="semibold" mb={2}>
                  Kafka Bootstrap Server
                </Text>
                <Code display="block" p={3} borderRadius="md">
                  localhost:9092
                </Code>
              </Box>

              <Divider />

              <Box>
                <Text fontWeight="semibold" mb={2}>
                  Example: kafka-go
                </Text>
                <Code display="block" p={3} borderRadius="md" fontSize="xs" whiteSpace="pre">
{`conn, err := kafka.DialLeader(context.Background(),
    "tcp", "localhost:9092", "my-topic", 0)`}
                </Code>
              </Box>

              <Box>
                <Text fontWeight="semibold" mb={2}>
                  Example: kafkajs
                </Text>
                <Code display="block" p={3} borderRadius="md" fontSize="xs" whiteSpace="pre">
{`const kafka = new Kafka({
  brokers: ['localhost:9092']
})`}
                </Code>
              </Box>

              <Box>
                <Text fontWeight="semibold" mb={2}>
                  Example: kafka-python
                </Text>
                <Code display="block" p={3} borderRadius="md" fontSize="xs" whiteSpace="pre">
{`from kafka import KafkaProducer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092'
)`}
                </Code>
              </Box>
            </VStack>
          </CardBody>
        </Card>
      </SimpleGrid>
    </VStack>
  )
}
