import { useEffect, useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
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
  Tabs,
  TabList,
  TabPanels,
  Tab,
  TabPanel,
  IconButton,
  Tooltip,
} from '@chakra-ui/react'
import type { Topic, Message } from '../api/client'
import { api } from '../api/client'

export function Topics() {
  const { topicName, offset: urlOffset } = useParams()
  const navigate = useNavigate()

  const [topics, setTopics] = useState<Topic[]>([])
  const [selectedTopic, setSelectedTopic] = useState<string | null>(topicName ?? null)
  const [messages, setMessages] = useState<Message[]>([])
  const [offset, setOffset] = useState(urlOffset ? parseInt(urlOffset) : 0)
  const [loading, setLoading] = useState(false)
  const [newTopicName, setNewTopicName] = useState('')
  const [produceKey, setProduceKey] = useState('')
  const [produceValue, setProduceValue] = useState('')
  const [selectedMessage, setSelectedMessage] = useState<Message | null>(null)

  const { isOpen: isCreateOpen, onOpen: onCreateOpen, onClose: onCreateClose } = useDisclosure()
  const { isOpen: isProduceOpen, onOpen: onProduceOpen, onClose: onProduceClose } = useDisclosure()
  const { isOpen: isDetailOpen, onOpen: onDetailOpen, onClose: onDetailClose } = useDisclosure()
  const toast = useToast()
  const cardBg = useColorModeValue('white', 'gray.800')
  const codeBg = useColorModeValue('gray.100', 'gray.900')

  // Sync URL params to state
  useEffect(() => {
    if (topicName && topicName !== selectedTopic) {
      setSelectedTopic(topicName)
    }
    if (urlOffset) {
      const parsed = parseInt(urlOffset)
      if (!isNaN(parsed) && parsed !== offset) {
        setOffset(parsed)
      }
    }
  }, [topicName, urlOffset])

  useEffect(() => {
    loadTopics()
  }, [])

  useEffect(() => {
    if (selectedTopic) {
      loadMessages(selectedTopic, offset)
    }
  }, [selectedTopic])

  // Update URL when topic/offset changes
  function updateUrl(topic: string | null, newOffset?: number) {
    if (topic) {
      if (newOffset !== undefined && newOffset > 0) {
        navigate(`/topics/${topic}/${newOffset}`, { replace: true })
      } else {
        navigate(`/topics/${topic}`, { replace: true })
      }
    } else {
      navigate('/topics', { replace: true })
    }
  }

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
      updateUrl(topic, startOffset)
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
        updateUrl(null)
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

  function handleSelectTopic(name: string) {
    setSelectedTopic(name)
    setOffset(0)
    updateUrl(name)
  }

  function handleOpenDetail(msg: Message) {
    setSelectedMessage(msg)
    onDetailOpen()
  }

  function handleMoveToDLQ() {
    if (!selectedMessage || !selectedTopic) return
    // TODO: Implement actual DLQ move
    toast({
      title: 'Move to DLQ',
      description: `Message ${selectedMessage.offset} from ${selectedTopic} will be moved to __dlq (not implemented yet)`,
      status: 'info',
      duration: 3000,
    })
    onDetailClose()
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
                    onClick={() => handleSelectTopic(topic.name)}
                  >
                    <HStack justify="space-between">
                      <Text fontWeight="medium" noOfLines={1}>
                        {topic.name}
                      </Text>
                      <Badge
                        colorScheme={selectedTopic === topic.name ? 'whiteAlpha' : 'blue'}
                      >
                        {topic.latest_offset + 1}
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
                      {(currentTopicMeta?.latest_offset ?? -1) + 1} messages
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
                          <Th w="60px"></Th>
                        </Tr>
                      </Thead>
                      <Tbody>
                        {messages.map(msg => (
                          <Tr
                            key={msg.offset}
                            _hover={{ bg: 'gray.700' }}
                            cursor="pointer"
                            onClick={() => handleOpenDetail(msg)}
                          >
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
                                whiteSpace="nowrap"
                                overflow="hidden"
                                textOverflow="ellipsis"
                                maxW="400px"
                              >
                                {msg.value}
                              </Code>
                            </Td>
                            <Td>
                              <Tooltip label="View details">
                                <IconButton
                                  aria-label="View"
                                  icon={<Text>👁</Text>}
                                  size="xs"
                                  variant="ghost"
                                  onClick={(e) => {
                                    e.stopPropagation()
                                    handleOpenDetail(msg)
                                  }}
                                />
                              </Tooltip>
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

      {/* Message Detail Modal */}
      <Modal isOpen={isDetailOpen} onClose={onDetailClose} size="xl">
        <ModalOverlay />
        <ModalContent bg={cardBg} maxW="800px">
          <ModalHeader>
            <HStack justify="space-between" pr={8}>
              <Text>Message Detail</Text>
              <Badge colorScheme="blue">Offset {selectedMessage?.offset}</Badge>
            </HStack>
          </ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            {selectedMessage && (
              <VStack spacing={4} align="stretch">
                {/* Metadata */}
                <HStack spacing={8} wrap="wrap">
                  <Box>
                    <Text fontSize="xs" color="gray.500" textTransform="uppercase">Topic</Text>
                    <Text fontWeight="medium">{selectedTopic}</Text>
                  </Box>
                  <Box>
                    <Text fontSize="xs" color="gray.500" textTransform="uppercase">Offset</Text>
                    <Text fontWeight="medium">{selectedMessage.offset}</Text>
                  </Box>
                  <Box>
                    <Text fontSize="xs" color="gray.500" textTransform="uppercase">Timestamp</Text>
                    <Text fontWeight="medium">{new Date(selectedMessage.timestamp).toLocaleString()}</Text>
                  </Box>
                  <Box>
                    <Text fontSize="xs" color="gray.500" textTransform="uppercase">Key</Text>
                    <Code>{selectedMessage.key || '(null)'}</Code>
                  </Box>
                  {selectedMessage.codec !== 0 && (
                    <Box>
                      <Text fontSize="xs" color="gray.500" textTransform="uppercase">Codec</Text>
                      <Badge colorScheme="purple">{getCodecName(selectedMessage.codec)}</Badge>
                    </Box>
                  )}
                </HStack>

                {/* Value with tabs */}
                <Tabs variant="enclosed" size="sm">
                  <TabList>
                    <Tab>Raw</Tab>
                    <Tab>Formatted</Tab>
                    <Tab>Hex</Tab>
                  </TabList>
                  <TabPanels>
                    <TabPanel p={0} pt={3}>
                      <Code
                        display="block"
                        whiteSpace="pre-wrap"
                        p={4}
                        bg={codeBg}
                        borderRadius="md"
                        maxH="400px"
                        overflowY="auto"
                        fontSize="sm"
                      >
                        {selectedMessage.value}
                      </Code>
                    </TabPanel>
                    <TabPanel p={0} pt={3}>
                      <Code
                        display="block"
                        whiteSpace="pre-wrap"
                        p={4}
                        bg={codeBg}
                        borderRadius="md"
                        maxH="400px"
                        overflowY="auto"
                        fontSize="sm"
                      >
                        {formatValue(selectedMessage.value)}
                      </Code>
                    </TabPanel>
                    <TabPanel p={0} pt={3}>
                      <Code
                        display="block"
                        whiteSpace="pre-wrap"
                        p={4}
                        bg={codeBg}
                        borderRadius="md"
                        maxH="400px"
                        overflowY="auto"
                        fontSize="xs"
                        fontFamily="mono"
                      >
                        {toHex(selectedMessage.value)}
                      </Code>
                    </TabPanel>
                  </TabPanels>
                </Tabs>
              </VStack>
            )}
          </ModalBody>
          <ModalFooter>
            <HStack spacing={3}>
              <Button
                colorScheme="orange"
                variant="outline"
                onClick={handleMoveToDLQ}
                leftIcon={<Text>🗑</Text>}
              >
                Move to DLQ
              </Button>
              <Button variant="ghost" onClick={onDetailClose}>
                Close
              </Button>
            </HStack>
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
    // Try base64 decode
    try {
      const decoded = atob(value)
      // Check if it looks like JSON after decoding
      const parsed = JSON.parse(decoded)
      return `[Base64 Decoded]\n${JSON.stringify(parsed, null, 2)}`
    } catch {
      // Try to detect if it's gzip/snappy compressed
      if (value.startsWith('H4sI')) {
        return `[Gzip Compressed - Base64]\n${value}`
      }
      return value
    }
  }
}

function toHex(str: string): string {
  const lines: string[] = []
  const bytes = new TextEncoder().encode(str)

  for (let i = 0; i < bytes.length; i += 16) {
    const chunk = bytes.slice(i, i + 16)
    const hex = Array.from(chunk).map(b => b.toString(16).padStart(2, '0')).join(' ')
    const ascii = Array.from(chunk).map(b => (b >= 32 && b < 127) ? String.fromCharCode(b) : '.').join('')
    lines.push(`${i.toString(16).padStart(8, '0')}  ${hex.padEnd(48)}  ${ascii}`)
  }

  return lines.join('\n')
}

function getCodecName(codec: number): string {
  switch (codec) {
    case 0: return 'None'
    case 1: return 'Gzip'
    case 2: return 'Snappy'
    case 3: return 'LZ4'
    case 4: return 'Zstd'
    default: return `Unknown (${codec})`
  }
}
