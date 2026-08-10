import { afterEach, describe, expect, it, vi } from 'vitest'

import { loader } from './mcp-install'
import { loader as windowsLoader } from './mcp-install-windows'

describe('MCP installer resource route', () => {
  afterEach(() => vi.unstubAllEnvs())

  it('serves a shell script for an allowlisted client', async () => {
    const response = await loader({
      params: { clientId: 'codex' },
      request: new Request('http://reef.test/mcp/install/codex'),
    } as Parameters<typeof loader>[0])

    expect(response.status).toBe(200)
    expect(response.headers.get('content-type')).toContain('text/x-shellscript')
    expect(response.headers.get('content-disposition')).toContain('coral-mcp-codex.sh')
    await expect(response.text()).resolves.toContain('client="codex"')
  })

  it('serves PowerShell for the Windows route', async () => {
    const response = await windowsLoader({
      params: { clientId: 'codex' },
      request: new Request('http://reef.test/mcp/install/codex/windows'),
    } as Parameters<typeof windowsLoader>[0])

    expect(response.headers.get('content-disposition')).toContain('coral-mcp-codex.ps1')
    await expect(response.text()).resolves.toContain("$ErrorActionPreference = 'Stop'")
  })

  it('does not generate a script for unknown clients', async () => {
    const response = await loader({
      params: { clientId: 'not-a-client' },
      request: new Request('http://reef.test/mcp/install/not-a-client'),
    } as Parameters<typeof loader>[0])

    expect(response.status).toBe(404)
  })

  it('configures a remote HTTP endpoint without requiring a local Coral binary', async () => {
    vi.stubEnv('CORAL_MCP_MODE', 'remote')
    vi.stubEnv('CORAL_MCP_URL', 'https://coral.example.com/mcp')

    const response = await loader({
      params: { clientId: 'codex' },
      request: new Request('http://reef.test/mcp/install/codex'),
    } as Parameters<typeof loader>[0])

    const script = await response.text()
    expect(script).toContain('"type":"http","url":"https://coral.example.com/mcp"')
    expect(script).not.toContain('command -v coral')
  })

  it('configures the remote HTTP endpoint in the Windows installer', async () => {
    vi.stubEnv('CORAL_MCP_MODE', 'remote')
    vi.stubEnv('CORAL_MCP_URL', 'https://coral.example.com/mcp')

    const response = await windowsLoader({
      params: { clientId: 'codex' },
      request: new Request('http://reef.test/mcp/install/codex/windows'),
    } as Parameters<typeof windowsLoader>[0])

    const script = await response.text()
    expect(script).toContain('"type":"http","url":"https://coral.example.com/mcp"')
    expect(script).not.toContain('Get-Command coral')
  })
})
