import { describe, expect, it } from 'vitest'

import {
  TraceInvocationKind,
  TraceOperationKind,
  TraceStatus,
} from '@/generated/coral/v1/traces_pb'

import {
  SEARCH_EXTRA_DETAILS_DESCRIPTION,
  shouldClearTimelineInspector,
  timelineShortcutsEnabled,
  traceDetailPolicy,
} from './trace-detail-policy'
import { formatInvocation, type TraceSummaryData } from './trace-utils'

function summary(operationKind: TraceOperationKind): TraceSummaryData {
  return {
    durationNanos: '1000000',
    endTimeUnixNanos: '2000000',
    invocationKind: TraceInvocationKind.DIRECT,
    name: operationKind === TraceOperationKind.SEARCH ? 'coral.search' : 'coral.query',
    operationKind,
    operationName: operationKind === TraceOperationKind.TOOL ? 'list_tables' : '',
    query: 'needle',
    rootSpanId: 'root',
    rowCount: '0',
    rowCountRecorded: false,
    spanCount: 1,
    startTimeUnixNanos: '1000000',
    status: TraceStatus.OK,
    traceId: 'trace',
  }
}

describe('trace detail policy', () => {
  it('defaults Search to Results and gates Timeline shortcuts outside Extra details', () => {
    const search = summary(TraceOperationKind.SEARCH)

    expect(traceDetailPolicy(search, 'Results 3')).toEqual({
      defaultTab: 'results',
      primaryTabs: [
        { id: 'results', label: 'Results 3' },
        {
          description: SEARCH_EXTRA_DETAILS_DESCRIPTION,
          id: 'timeline',
          label: 'Extra details',
        },
      ],
      traceStatsInSummary: false,
    })
    expect(timelineShortcutsEnabled(search, 'results')).toBe(false)
    expect(timelineShortcutsEnabled(search, 'timeline')).toBe(true)
    expect(shouldClearTimelineInspector(search, 'results')).toBe(true)
    expect(shouldClearTimelineInspector(search, 'timeline')).toBe(false)
  })

  it.each([TraceOperationKind.QUERY, TraceOperationKind.TOOL])(
    'preserves Query and Tool detail behavior for operation kind %s',
    (operationKind) => {
      const operation = summary(operationKind)

      expect(traceDetailPolicy(operation, 'Results')).toEqual({
        defaultTab: 'timeline',
        primaryTabs: [{ id: 'timeline', label: 'Trace' }],
        traceStatsInSummary: true,
      })
      expect(timelineShortcutsEnabled(operation, 'results')).toBe(true)
      expect(shouldClearTimelineInspector(operation, 'results')).toBe(false)
    },
  )

  it('formats invocation paths without leaking an unspecified value', () => {
    expect(formatInvocation(TraceInvocationKind.DIRECT)).toBe('Direct')
    expect(formatInvocation(TraceInvocationKind.MCP)).toBe('MCP')
    expect(formatInvocation(TraceInvocationKind.UNSPECIFIED)).toBe('—')
  })
})
