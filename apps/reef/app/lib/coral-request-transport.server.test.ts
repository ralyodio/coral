import { Code, ConnectError } from '@connectrpc/connect'
import { afterEach, expect, it, vi } from 'vitest'

import { guiOnboardingClientForRequest } from './coral-request.server'
import { isCoralUnavailableError } from './coral-unavailable'

// Kept out of `coral-request.server.test.ts`: that file mocks both Connect
// transports away, and this case needs the real gRPC-Web transport so the
// fetch wrapper it is built with actually runs.
const request = new Request('http://reef.test/onboarding')

afterEach(() => {
  vi.unstubAllGlobals()
})

it('classifies fetch connection failures as unavailable', async () => {
  const networkError = new TypeError('fetch failed')
  vi.stubGlobal('fetch', vi.fn().mockRejectedValue(networkError))

  const error = await guiOnboardingClientForRequest(request, null)
    .getGuiOnboardingState({})
    .catch((caught) => caught)

  expect(error).toBeInstanceOf(ConnectError)
  expect(error).toMatchObject({ cause: networkError, code: Code.Unavailable })
  expect(isCoralUnavailableError(error)).toBe(true)
})
