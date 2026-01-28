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
  Badge,
  Text,
  Code,
  useToast,
  useColorModeValue,
  Accordion,
  AccordionItem,
  AccordionButton,
  AccordionPanel,
  AccordionIcon,
  Modal,
  ModalOverlay,
  ModalContent,
  ModalHeader,
  ModalBody,
  ModalFooter,
  ModalCloseButton,
  FormControl,
  FormLabel,
  NumberInput,
  NumberInputField,
  Select,
  useDisclosure,
} from '@chakra-ui/react'
import type { Group, Topic } from '../api/client'
import { api } from '../api/client'

interface GroupsProps {
  initialGroup?: string
}

interface GroupDetail {
  State: string
  Generation: number
  Members: Record<string, MemberDetail>
  Offsets: Record<string, number>
}

interface MemberDetail {
  ClientID?: string
  ClientHost?: string
  ProtocolType?: string
  Assignment?: unknown
}

export function Groups({ initialGroup }: GroupsProps) {
  const [groups, setGroups] = useState<Group[]>([])
  const [topics, setTopics] = useState<Topic[]>([])
  const [selectedGroup, setSelectedGroup] = useState<string | null>(initialGroup ?? null)
  const [groupDetail, setGroupDetail] = useState<GroupDetail | null>(null)
  const [offsetTopic, setOffsetTopic] = useState('')
  const [offsetValue, setOffsetValue] = useState(0)

  const { isOpen, onOpen, onClose } = useDisclosure()
  const toast = useToast()
  const cardBg = useColorModeValue('white', 'gray.800')

  useEffect(() => {
    loadData()
    const interval = setInterval(loadData, 5000)
    return () => clearInterval(interval)
  }, [])

  useEffect(() => {
    if (selectedGroup) {
      loadGroupDetail(selectedGroup)
    }
  }, [selectedGroup])

  async function loadData() {
    try {
      const [g, t] = await Promise.all([api.getGroups(), api.getTopics()])
      setGroups(g)
      setTopics(t)
    } catch (err) {
      console.error('Failed to load data:', err)
    }
  }

  async function loadGroupDetail(groupId: string) {
    try {
      const detail = await api.getGroup(groupId)
      setGroupDetail(detail as unknown as GroupDetail)
    } catch {
      toast({ title: 'Failed to load group detail', status: 'error' })
    }
  }

  async function handleDeleteGroup(groupId: string) {
    if (!confirm(`Delete consumer group "${groupId}"? This will remove all committed offsets.`)) {
      return
    }
    try {
      await api.deleteGroup(groupId)
      toast({ title: 'Group deleted', status: 'success' })
      if (selectedGroup === groupId) {
        setSelectedGroup(null)
        setGroupDetail(null)
      }
      loadData()
    } catch {
      toast({ title: 'Failed to delete group', status: 'error' })
    }
  }

  async function handleSetOffset() {
    if (!selectedGroup || !offsetTopic) return
    try {
      await api.setGroupOffset(selectedGroup, offsetTopic, offsetValue)
      toast({ title: 'Offset updated', status: 'success' })
      onClose()
      loadGroupDetail(selectedGroup)
    } catch {
      toast({ title: 'Failed to set offset', status: 'error' })
    }
  }

  function getStateColor(state: string): string {
    switch (state) {
      case 'Stable':
        return 'green'
      case 'PreparingRebalance':
      case 'CompletingRebalance':
        return 'yellow'
      case 'Empty':
        return 'gray'
      case 'Dead':
        return 'red'
      default:
        return 'gray'
    }
  }

  return (
    <VStack spacing={6} align="stretch">
      <Heading size="lg">Consumer Groups</Heading>

      <HStack spacing={6} align="start">
        {/* Group List */}
        <Card bg={cardBg} minW="300px" maxW="300px">
          <CardBody>
            <VStack spacing={2} align="stretch">
              {groups.length === 0 ? (
                <Text color="gray.500" fontSize="sm">
                  No consumer groups yet.
                  <br />
                  Connect a consumer to create one.
                </Text>
              ) : (
                groups.map(group => (
                  <Box
                    key={group.id}
                    p={3}
                    borderRadius="md"
                    cursor="pointer"
                    bg={selectedGroup === group.id ? 'blue.500' : 'transparent'}
                    color={selectedGroup === group.id ? 'white' : 'inherit'}
                    _hover={{
                      bg: selectedGroup === group.id ? 'blue.500' : 'gray.700',
                    }}
                    onClick={() => setSelectedGroup(group.id)}
                  >
                    <HStack justify="space-between">
                      <VStack align="start" spacing={0}>
                        <Text fontWeight="medium" noOfLines={1} maxW="180px">
                          {group.id}
                        </Text>
                        <Text fontSize="xs" opacity={0.7}>
                          {group.members} member{group.members !== 1 ? 's' : ''}
                        </Text>
                      </VStack>
                      <Badge
                        colorScheme={
                          selectedGroup === group.id
                            ? 'whiteAlpha'
                            : getStateColor(group.state)
                        }
                        fontSize="xs"
                      >
                        {group.state}
                      </Badge>
                    </HStack>
                  </Box>
                ))
              )}
            </VStack>
          </CardBody>
        </Card>

        {/* Group Detail */}
        <Card bg={cardBg} flex={1}>
          <CardBody>
            {!selectedGroup ? (
              <VStack py={12} spacing={4}>
                <Text fontSize="4xl">👥</Text>
                <Text color="gray.500">Select a consumer group to view details</Text>
              </VStack>
            ) : groupDetail ? (
              <VStack spacing={4} align="stretch">
                <HStack justify="space-between">
                  <VStack align="start" spacing={0}>
                    <Heading size="md">{selectedGroup}</Heading>
                    <HStack>
                      <Badge colorScheme={getStateColor(groupDetail.State)}>
                        {groupDetail.State}
                      </Badge>
                      <Text fontSize="sm" color="gray.500">
                        Generation {groupDetail.Generation}
                      </Text>
                    </HStack>
                  </VStack>
                  <HStack>
                    <Button size="sm" onClick={onOpen}>
                      Set Offset
                    </Button>
                    <Button
                      size="sm"
                      colorScheme="red"
                      variant="ghost"
                      onClick={() => handleDeleteGroup(selectedGroup)}
                    >
                      Delete
                    </Button>
                  </HStack>
                </HStack>

                {/* Members */}
                <Box>
                  <Text fontWeight="semibold" mb={2}>
                    Members ({Object.keys(groupDetail.Members || {}).length})
                  </Text>
                  {Object.keys(groupDetail.Members || {}).length === 0 ? (
                    <Text color="gray.500" fontSize="sm">
                      No active members
                    </Text>
                  ) : (
                    <Accordion allowMultiple>
                      {Object.entries(groupDetail.Members || {}).map(
                        ([memberId, member]) => (
                          <AccordionItem key={memberId} border="none">
                            <AccordionButton px={2} py={2} borderRadius="md" _hover={{ bg: 'gray.700' }}>
                              <Box flex="1" textAlign="left">
                                <HStack>
                                  <Code fontSize="sm">{memberId}</Code>
                                  {member.ClientID && (
                                    <Text fontSize="xs" color="gray.500">
                                      ({member.ClientID})
                                    </Text>
                                  )}
                                </HStack>
                              </Box>
                              <AccordionIcon />
                            </AccordionButton>
                            <AccordionPanel pb={4}>
                              <VStack align="start" spacing={2} fontSize="sm">
                                <HStack>
                                  <Text color="gray.500">Client Host:</Text>
                                  <Code>{member.ClientHost || 'N/A'}</Code>
                                </HStack>
                                <HStack>
                                  <Text color="gray.500">Protocol Type:</Text>
                                  <Code>{member.ProtocolType || 'consumer'}</Code>
                                </HStack>
                                {member.Assignment !== undefined && member.Assignment !== null && (
                                  <Box>
                                    <Text color="gray.500">Assigned Partitions:</Text>
                                    <Code
                                      display="block"
                                      whiteSpace="pre"
                                      p={2}
                                      mt={1}
                                      fontSize="xs"
                                    >
                                      {JSON.stringify(member.Assignment, null, 2)}
                                    </Code>
                                  </Box>
                                )}
                              </VStack>
                            </AccordionPanel>
                          </AccordionItem>
                        )
                      )}
                    </Accordion>
                  )}
                </Box>

                {/* Committed Offsets */}
                <Box>
                  <Text fontWeight="semibold" mb={2}>
                    Committed Offsets
                  </Text>
                  {Object.keys(groupDetail.Offsets || {}).length === 0 ? (
                    <Text color="gray.500" fontSize="sm">
                      No committed offsets
                    </Text>
                  ) : (
                    <Table size="sm">
                      <Thead>
                        <Tr>
                          <Th>Topic</Th>
                          <Th isNumeric>Offset</Th>
                          <Th isNumeric>Lag</Th>
                        </Tr>
                      </Thead>
                      <Tbody>
                        {Object.entries(groupDetail.Offsets || {}).map(
                          ([topic, offset]) => {
                            const topicMeta = topics.find(t => t.name === topic)
                            const latest = topicMeta?.latest_offset ?? 0
                            const lag = Math.max(0, latest - offset)
                            return (
                              <Tr key={topic}>
                                <Td>
                                  <Code fontSize="sm">{topic}</Code>
                                </Td>
                                <Td isNumeric>{offset}</Td>
                                <Td isNumeric>
                                  <Badge
                                    colorScheme={
                                      lag === 0 ? 'green' : lag < 100 ? 'yellow' : 'red'
                                    }
                                  >
                                    {lag}
                                  </Badge>
                                </Td>
                              </Tr>
                            )
                          }
                        )}
                      </Tbody>
                    </Table>
                  )}
                </Box>

                {/* Raw Data */}
                <Box>
                  <Text fontWeight="semibold" mb={2}>
                    Raw Data
                  </Text>
                  <Code
                    display="block"
                    whiteSpace="pre"
                    p={3}
                    borderRadius="md"
                    fontSize="xs"
                    maxH="200px"
                    overflowY="auto"
                  >
                    {JSON.stringify(groupDetail, null, 2)}
                  </Code>
                </Box>
              </VStack>
            ) : (
              <Text color="gray.500">Loading...</Text>
            )}
          </CardBody>
        </Card>
      </HStack>

      {/* Set Offset Modal */}
      <Modal isOpen={isOpen} onClose={onClose}>
        <ModalOverlay />
        <ModalContent bg={cardBg}>
          <ModalHeader>Set Offset for {selectedGroup}</ModalHeader>
          <ModalCloseButton />
          <ModalBody>
            <VStack spacing={4}>
              <FormControl>
                <FormLabel>Topic</FormLabel>
                <Select
                  placeholder="Select topic"
                  value={offsetTopic}
                  onChange={e => setOffsetTopic(e.target.value)}
                >
                  {topics.map(t => (
                    <option key={t.name} value={t.name}>
                      {t.name} (latest: {t.latest_offset})
                    </option>
                  ))}
                </Select>
              </FormControl>
              <FormControl>
                <FormLabel>Offset</FormLabel>
                <NumberInput
                  value={offsetValue}
                  min={0}
                  onChange={(_: string, val: number) => !isNaN(val) && setOffsetValue(val)}
                >
                  <NumberInputField />
                </NumberInput>
              </FormControl>
            </VStack>
          </ModalBody>
          <ModalFooter>
            <Button variant="ghost" mr={3} onClick={onClose}>
              Cancel
            </Button>
            <Button colorScheme="blue" onClick={handleSetOffset}>
              Set Offset
            </Button>
          </ModalFooter>
        </ModalContent>
      </Modal>
    </VStack>
  )
}
