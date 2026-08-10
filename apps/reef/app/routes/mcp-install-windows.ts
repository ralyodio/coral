import type { Route } from './+types/mcp-install-windows'

import { mcpClientById } from '@/lib/mcp-clients'
import { mcpConnectionFromEnv } from '@/lib/mcp-connection.server'
import { mcpInstallerPowerShellScript } from '@/lib/mcp-installer-script.server'

export function loader({ params }: Route.LoaderArgs): Response {
  const client = mcpClientById(params.clientId)
  if (!client) return new Response('Unknown MCP client.\n', { status: 404 })

  return new Response(mcpInstallerPowerShellScript(client, mcpConnectionFromEnv()), {
    headers: {
      'content-disposition': `inline; filename="coral-mcp-${client.id}.ps1"`,
      'content-type': 'text/plain; charset=utf-8',
      'x-content-type-options': 'nosniff',
    },
  })
}
