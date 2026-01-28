import type { ReactNode } from 'react'
import {
  Box,
  Flex,
  VStack,
  HStack,
  Text,
  Badge,
  Divider,
  useColorModeValue,
} from '@chakra-ui/react'

interface NavItemProps {
  icon: string
  label: string
  active?: boolean
  badge?: number
  onClick: () => void
}

function NavItem({ icon, label, active, badge, onClick }: NavItemProps) {
  const activeBg = useColorModeValue('blue.50', 'blue.900')
  const activeColor = useColorModeValue('blue.600', 'blue.200')
  const hoverBg = useColorModeValue('gray.100', 'gray.700')

  return (
    <Box
      as="button"
      w="full"
      py={2}
      px={3}
      borderRadius="md"
      bg={active ? activeBg : 'transparent'}
      color={active ? activeColor : 'inherit'}
      _hover={{ bg: active ? activeBg : hoverBg }}
      onClick={onClick}
      textAlign="left"
    >
      <HStack justify="space-between">
        <HStack spacing={3}>
          <Text fontSize="lg">{icon}</Text>
          <Text fontWeight={active ? 'semibold' : 'normal'}>{label}</Text>
        </HStack>
        {badge !== undefined && badge > 0 && (
          <Badge colorScheme="blue" borderRadius="full">
            {badge}
          </Badge>
        )}
      </HStack>
    </Box>
  )
}

interface LayoutProps {
  children: ReactNode
  currentPage: string
  onNavigate: (page: string) => void
  stats: { topics: number; groups: number; pending: number }
  connected: boolean
}

export function Layout({ children, currentPage, onNavigate, stats, connected }: LayoutProps) {
  const sidebarBg = useColorModeValue('gray.50', 'gray.800')
  const borderColor = useColorModeValue('gray.200', 'gray.700')

  return (
    <Flex h="100vh" overflow="hidden">
      {/* Sidebar */}
      <Box
        w="240px"
        bg={sidebarBg}
        borderRight="1px"
        borderColor={borderColor}
        p={4}
        display="flex"
        flexDirection="column"
      >
        {/* Logo */}
        <HStack spacing={3} mb={6}>
          <Text fontSize="2xl">📝</Text>
          <Box>
            <Text fontSize="xl" fontWeight="bold">
              Monolog
            </Text>
            <HStack spacing={1}>
              <Box
                w={2}
                h={2}
                borderRadius="full"
                bg={connected ? 'green.400' : 'red.400'}
              />
              <Text fontSize="xs" color="gray.500">
                {connected ? 'Connected' : 'Disconnected'}
              </Text>
            </HStack>
          </Box>
        </HStack>

        {/* Navigation */}
        <VStack spacing={1} align="stretch" flex={1}>
          <NavItem
            icon="📊"
            label="Dashboard"
            active={currentPage === 'dashboard'}
            onClick={() => onNavigate('dashboard')}
          />
          <NavItem
            icon="📁"
            label="Topics"
            active={currentPage === 'topics'}
            badge={stats.topics}
            onClick={() => onNavigate('topics')}
          />
          <NavItem
            icon="👥"
            label="Consumer Groups"
            active={currentPage === 'groups'}
            badge={stats.groups}
            onClick={() => onNavigate('groups')}
          />
          <NavItem
            icon="⏳"
            label="Pending Fetches"
            active={currentPage === 'pending'}
            badge={stats.pending}
            onClick={() => onNavigate('pending')}
          />

          <Divider my={4} />

          <NavItem
            icon="⚡"
            label="Quick Actions"
            active={currentPage === 'actions'}
            onClick={() => onNavigate('actions')}
          />
        </VStack>

        {/* Footer */}
        <Box pt={4} borderTop="1px" borderColor={borderColor}>
          <Text fontSize="xs" color="gray.500" textAlign="center">
            Kafka-compatible broker
          </Text>
          <Text fontSize="xs" color="gray.500" textAlign="center">
            localhost:9092
          </Text>
        </Box>
      </Box>

      {/* Main Content */}
      <Box flex={1} overflow="auto" p={6}>
        {children}
      </Box>
    </Flex>
  )
}
