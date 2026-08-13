import type { Route } from './+types/mcp-install'

import { mcpClientById } from '@/lib/mcp-clients'
import { mcpConnectionFromEnv } from '@/lib/mcp-connection.server'
import { mcpInstallerScript } from '@/lib/mcp-installer-script.server'

// A resource route intentionally serves only a bounded shell installer. It has
// no browser-to-Coral transport: all client configuration happens on the user's host.
export function loader({ params }: Route.LoaderArgs): Response {
  const client = mcpClientById(params.clientId)
  if (!client) return new Response('Unknown MCP client.\n', { status: 404 })

  return new Response(mcpInstallerScript(client, mcpConnectionFromEnv()), {
    headers: {
      'content-disposition': `inline; filename="coral-mcp-${client.id}.sh"`,
      'content-type': 'text/x-shellscript; charset=utf-8',
      'x-content-type-options': 'nosniff',
    },
  })
}
