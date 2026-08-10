import { isSearchOperation, type TraceSummaryData } from './trace-utils'

export const SEARCH_EXTRA_DETAILS_DESCRIPTION =
  'Only visible in Coral. These details were not included in the Search response.'

export interface PrimaryDetailTab {
  description?: string
  id: 'results' | 'timeline'
  label: string
}

export interface TraceDetailPolicy {
  defaultTab: 'results' | 'timeline'
  primaryTabs: PrimaryDetailTab[]
  traceStatsInSummary: boolean
}

export function traceDetailPolicy(
  summary: TraceSummaryData,
  resultsLabel: string,
): TraceDetailPolicy {
  if (isSearchOperation(summary)) {
    return {
      defaultTab: 'results',
      primaryTabs: [
        { id: 'results', label: resultsLabel },
        {
          description: SEARCH_EXTRA_DETAILS_DESCRIPTION,
          id: 'timeline',
          label: 'Extra details',
        },
      ],
      traceStatsInSummary: false,
    }
  }

  return {
    defaultTab: 'timeline',
    primaryTabs: [{ id: 'timeline', label: 'Trace' }],
    traceStatsInSummary: true,
  }
}

export function timelineShortcutsEnabled(summary: TraceSummaryData, activeTab: string): boolean {
  return !isSearchOperation(summary) || activeTab === 'timeline'
}

export function shouldClearTimelineInspector(summary: TraceSummaryData, nextTab: string): boolean {
  return isSearchOperation(summary) && nextTab !== 'timeline'
}
