import { useEffect, useState } from 'react'
import {
  Box,
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
  Button,
  Input,
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalBody,
  ModalFooter,
  ModalCloseButton,
  FormControl,
  FormLabel,
  Textarea,
  useDisclosure,
  useToast,
  Badge,
  Text,
  Code,
  useColorModeValue,
  NumberInput,
  NumberInputField,
  NumberInputStepper,
  NumberIncrementStepper,
  NumberDecrementStepper,
} from '@chakra-ui/react'
import type { Topic, Message } from '../api/client'
import { api } from '../api/client'

interface TopicsProps {
  initialTopic?: string
}

export function Topics({ initialTopic }: TopicsProps) {
  const [topics, setTopics] = useState<Topic[]>([])
  const [selectedTopic, setSelectedTopic] = useState<string | null>(initialTopic ?? null)
  const [messages, setMessages] = useState<Message[]>([])
  const [offset, setOffset] = useState(0)
  const [loading, setLoading] = useState(false)
  const [newTopicName, setNewTopicName] = useState('')
  const [produceKey, setProduceKey] = useState('')
  const [produceValue, setProduceValue] = useState('')

  const { isOpen: isCreateOpen, onOpen: onCreateOpen, onClose: onCreateClose } = useDisclosure()
  const { isOpen: isProduceOpen, onOpen: onProduceOpen, onClose: onProduceClose } = useDisclosure()
  const toast = useToast()
  const cardBg = useColorModeValue('white', 'gray.800')
  const codeBg = useColorModeValue('gray.100', 'gray.900')

  useEffect(() => {
    loadTopics()
  }, [])

  useEffect(() => {
    if (selectedTopic) {
      loadMessages(selectedTopic, 0)
    }
  }, [selectedTopic])

  async function loadTopics() {
    try {
      const t = await api.getTopics()
      setTopics(t)
    } catch {
      toast({ title: 'Failed to load topics', status: 'error' })
    }
  }

  async function loadMessages(topic: string, startOffset: number) {
    setLoading(true)
    try {
      const m = await api.getMessages(topic, startOffset, 50)
      setMessages(m)
      setOffset(startOffset)
    } catch {
      toast({ title: 'Failed to load messages', status: 'error' })
    } finally {
      setLoading(false)
    }
  }

  async function handleCreateTopic() {
    if (!newTopicName.trim()) return
    try {
      await api.createTopic(newTopicName.trim())
      toast({ title: 'Topic created', status: 'success' })
      setNewTopicName('')
      onCreateClose()
      loadTopics()
    } catch {
      toast({ title: 'Failed to create topic', status: 'error' })
    }
  }

  async function handleDeleteTopic(name: string) {
    if (!confirm(`Delete topic "${name}"? This cannot be undone.`)) return
    try {
      await api.deleteTopic(name)
      toast({ title: 'Topic deleted', status: 'success' })
      if (selectedTopic === name) {
        setSelectedTopic(null)
        setMessages([])
      }
      loadTopics()
    } catch {
      toast({ title: 'Failed to delete topic', status: 'error' })
    }
  }

  async function handleProduce() {
    if (!selectedTopic || !produceValue.trim()) return
    try {
      const result = await api.produceMessage(selectedTopic, produceKey, produceValue)
      toast({ title: `Message produced at offset ${result.offset}`, status: 'success' })
      setProduceKey('')
      setProduceValue('')
      onProduceClose()
      loadMessages(selectedTopic, offset)
    } catch {
      toast({ title: 'Failed to produce message', status: 'error' })
    }
  }

  const currentTopicMeta = topics.find(t => t.name === selectedTopic)

  return (
    <VStack spacing={6} align="stretch">
      <HStack justify="space-between">
        <Heading size="lg">Topics</Heading>
        <Button colorScheme="blue" onClick={onCreateOpen}>
          + Create Topic
        </Button>
      </HStack>

      <HStack spacing={6} align="start">
        {/* Topic List */}
        <Card bg={cardBg} minW="280px" maxW="280px">
          <CardBody>
            <VStack spacing={2} align="stretch">
              {topics.length === 0 ? (
                <Text color="gray.500" fontSize="sm">
                  No topics yet
                </Text>
              ) : (
                topics.map(topic => (
                  <Box
                    key={topic.name}
                    p={3}
                    borderRadius="md"
                    cursor="pointer"
                    bg={selectedTopic === topic.name ? 'blue.500' : 'transparent'}
                    color={selectedTopic === topic.name ? 'white' : 'inherit'}
                    _hover={{
                      bg: selectedTopic === topic.name ? 'blue.500' : 'gray.700',
                    }}
                    onClick={() => setSelectedTopic(topic.name)}
                  >
                    <HStack justify="space-between">
                      <Text fontWeight="medium" noOfLines={1}>
                        {topic.name}
                      </Text>
                      <Badge
                        colorScheme={selectedTopic === topic.name ? 'whiteAlpha' : 'blue'}
                      >
                        {topic.latest_offset}
                      </Badge>
                    </HStack>
                  </Box>
                ))
              )}
            </VStack>
          </CardBody>
        </Card>

        {/* Message Browser */}
        <Card bg={cardBg} flex={1}>
          <CardBody>
            {!selectedTopic ? (
              <VStack py={12} spacing={4}>
                <Text fontSize="4xl">📁</Text>
                <Text color="gray.500">Select a topic to browse messages</Text>
              </VStack>
            ) : (
              <VStack spacing={4} align="stretch">
                <HStack justify="space-between">
                  <VStack align="start" spacing={0}>
                    <Heading size="md">{selectedTopic}</Heading>
                    <Text fontSize="sm" color="gray.500">
                      {currentTopicMeta?.latest_offset ?? 0} messages
                    </Text>
                  </VStack>
                  <HStack>
                    <Button size="sm" onClick={onProduceOpen}>
                      + Produce
                    </Button>
                    <Button
                      size="sm"
                      colorScheme="red"
                      variant="ghost"
                      onClick={() => handleDeleteTopic(selectedTopic)}
                    >
                      Delete
                    </Button>
                  </HStack>
                </HStack>

                {/* Offset Navigator */}
                <HStack>
                  <Text fontSize="sm" color="gray.500">
                    Offset:
                  </Text>
                  <NumberInput
                    size="sm"
                    maxW="120px"
                    value={offset}
                    min={0}
                    onChange={(_: string, val: number) => !isNaN(val) && setOffset(val)}
                  >
                    <NumberInputField />
                    <NumberInputStepper>
                      <NumberIncrementStepper />
                      <NumberDecrementStepper />
                    </NumberInputStepper>
                  </NumberInput>
                  <Button
                    size="sm"
                    onClick={() => loadMessages(selectedTopic, offset)}
                    isLoading={loading}
                  >
                    Go
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => loadMessages(selectedTopic, 0)}
                  >
                    Start
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    onClick={() => {
                      const latest = currentTopicMeta?.latest_offset ?? 0
                      const start = Math.max(0, latest - 50)
                      loadMessages(selectedTopic, start)
                    }}
                  >
                    Latest
                  </Button>
                </HStack>

                {/* Messages Table */}
                {messages.length === 0 ? (
                  <Text color="gray.500" py={8} textAlign="center">
                    No messages at this offset
                  </Text>
                ) : (
                  <Box overflowX="auto">
                    <Table size="sm">
                      <Thead>
                        <Tr>
                          <Th w="80px">Offset</Th>
                          <Th w="120px">Key</Th>
                          <Th>Value</Th>
                          <Th w="180px">Timestamp</Th>
                        </Tr>
                      </Thead>
                      <Tbody>
                        {messages.map(msg => (
                          <Tr key={msg.offset}>
                            <Td>
                              <Code fontSize="xs">{msg.offset}</Code>
                            </Td>
                            <Td>
                              <Code fontSize="xs" bg={codeBg} p={1}>
                                {msg.key || '(null)'}
                              </Code>
                            </Td>
                            <Td>
                              <Code
                                fontSize="xs"
                                bg={codeBg}
                                p={1}
                                display="block"
                                whiteSpace="pre-wrap"
                                maxH="100px"
                                overflowY="auto"
                              >
                                {formatValue(msg.value)}
                              </Code>
                            </Td>
                            <Td fontSize="xs" color="gray.500">
                              {new Date(msg.timestamp).toLocaleString()}
                            </Td>
                          </Tr>
                        ))}
                      </Tbody>
                    </Table>
                  </Box>
                )}

                {/* Pagination */}
                {messages.length > 0 && (
                  <HStack justify="center" pt={2}>
                    <Button
                      size="sm"
                      variant="ghost"
                      isDisabled={offset === 0}
                      onClick={() => loadMessages(selectedTopic, Math.max(0, offset - 50))}
                    >
                      ← Prev 50
                    </Button>
                    <Text fontSize="sm" color="gray.500">
                      {offset} - {offset + messages.length - 1}
                    </Text>
                    <Button
                      size="sm"
                      variant="ghost"
                      isDisabled={messages.length < 50}
                      onClick={() => loadMessages(selectedTopic, offset + 50)}
                    >
                      Next 50 →
                    </Button>
                  </HStack>
                )}
              </VStack>
            )}
          </CardBody>
        </Card>
      </HStack>

      {/* Create Topic Modal */}
      <Modal isOpen={isCreateOpen} onClose={onCreateClose}>
        <ModalOverlay />
        <ModalContent bg={cardBg}>
          <ModalHeader>Create Topic</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <FormControl>
              <FormLabel>Topic Name</FormLabel>
              <Input
                placeholder="my-topic"
                value={newTopicName}
                onChange={e => setNewTopicName(e.target.value)}
                onKeyDown={e => e.key === 'Enter' && handleCreateTopic()}
              />
            </FormControl>
          </ModalBody>
          <ModalFooter>
            <Button variant="ghost" mr={3} onClick={onCreateClose}>
              Cancel
            </Button>
            <Button colorScheme="blue" onClick={handleCreateTopic}>
              Create
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>

      {/* Produce Message Modal */}
      <Modal isOpen={isProduceOpen} onClose={onProduceClose} size="lg">
        <ModalOverlay />
        <ModalContent bg={cardBg}>
          <ModalHeader>Produce Message to {selectedTopic}</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <VStack spacing={4}>
              <FormControl>
                <FormLabel>Key (optional)</FormLabel>
                <Input
                  placeholder="message-key"
                  value={produceKey}
                  onChange={e => setProduceKey(e.target.value)}
                />
              </FormControl>
              <FormControl>
                <FormLabel>Value</FormLabel>
                <Textarea
                  placeholder='{"hello": "world"}'
                  value={produceValue}
                  onChange={e => setProduceValue(e.target.value)}
                  rows={6}
                  fontFamily="mono"
                />
              </FormControl>
            </VStack>
          </ModalBody>
          <ModalFooter>
            <Button variant="ghost" mr={3} onClick={onProduceClose}>
              Cancel
            </Button>
            <Button colorScheme="blue" onClick={handleProduce}>
              Produce
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </VStack>
  )
}

function formatValue(value: string): string {
  try {
    const parsed = JSON.parse(value)
    return JSON.stringify(parsed, null, 2)
  } catch {
    return value
  }
}
