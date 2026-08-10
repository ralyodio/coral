import { create } from '@bufbuild/protobuf'
import { describe, expect, it } from 'vitest'

import {
  SearchFieldSchema,
  SearchFieldValuesSchema,
  SearchFunctionShapeSchema,
  SearchProvider,
  SearchProviderCoverageSchema,
  SearchProviderState,
  SearchProviderStatusSchema,
  SearchResponseSchema,
  SearchResultSchema,
  SearchResultTruncationSchema,
  SearchSurfaceRefSchema,
  SearchTableShapeSchema,
} from '@/generated/coral/v1/search_pb'
import {
  TraceSearchResponseSchema,
  TraceSearchResponseTooLargeSchema,
} from '@/generated/coral/v1/traces_pb'

import {
  formatSearchSqlIdentifier,
  mapTraceSearchResponse,
  searchResultsTabLabel,
} from './search-response'

function groupedResponse() {
  const table = create(SearchResultSchema, {
    description: 'Workflow jobs and their current conclusions.',
    guide: 'Filter by owner before querying recent jobs.',
    matchingValues: [
      create(SearchFieldValuesSchema, { field: 'owner', values: ['withcoral'] }),
      create(SearchFieldValuesSchema, { field: 'conclusion', values: ['success', 'failure'] }),
    ],
    omittedMatchingFieldCount: 2,
    providers: [SearchProvider.CATALOG_METADATA, SearchProvider.OBSERVED_VALUES],
    shape: {
      case: 'table',
      value: create(SearchTableShapeSchema, {
        fields: [
          create(SearchFieldSchema, { dataType: 'Utf8', name: 'owner', required: true }),
          create(SearchFieldSchema, { dataType: 'Utf8', name: 'conclusion' }),
        ],
      }),
    },
    surface: create(SearchSurfaceRefSchema, {
      name: 'repo_action_jobs',
      schemaName: 'github',
    }),
  })
  const fn = create(SearchResultSchema, {
    providers: [SearchProvider.NATIVE_FANOUT, 99 as SearchProvider],
    shape: {
      case: 'function',
      value: create(SearchFunctionShapeSchema, {
        arguments: [
          create(SearchFieldSchema, { dataType: 'Int64', name: 'z_limit' }),
          create(SearchFieldSchema, { dataType: 'Utf8', name: 'query', required: true }),
        ],
        returns: [
          create(SearchFieldSchema, { dataType: 'Utf8', name: 'title' }),
          create(SearchFieldSchema, { dataType: 'Int64', name: 'issue_id' }),
        ],
      }),
    },
    surface: create(SearchSurfaceRefSchema, {
      catalogName: 'Warehouse-Prod',
      name: 'Search Issues',
      schemaName: 'analytics',
    }),
  })
  const missingSurface = create(SearchResultSchema, {
    providers: [SearchProvider.CATALOG_METADATA],
    shape: { case: 'table', value: create(SearchTableShapeSchema) },
  })
  const missingShape = create(SearchResultSchema, {
    surface: create(SearchSurfaceRefSchema, { name: 'orphan', schemaName: 'github' }),
  })

  return create(TraceSearchResponseSchema, {
    outcome: {
      case: 'response',
      value: create(SearchResponseSchema, {
        providerStatuses: [
          create(SearchProviderStatusSchema, {
            coverage: create(SearchProviderCoverageSchema, {
              budgetExhausted: true,
              eligibleUnits: 4,
              failedUnits: 1,
              hasMore: true,
              returnedCount: 2,
              searchedUnits: 3,
              staleIndex: true,
              timedOut: false,
            }),
            note: 'One source timed out.',
            provider: SearchProvider.NATIVE_FANOUT,
            state: SearchProviderState.PARTIAL,
          }),
          create(SearchProviderStatusSchema, {
            note: 'Catalog search completed.',
            provider: SearchProvider.CATALOG_METADATA,
            state: SearchProviderState.RESULTS_FOUND,
          }),
        ],
        results: [table, fn, missingSurface, missingShape],
        truncation: create(SearchResultTruncationSchema, {
          maxResults: 4,
          note: 'More candidates were available.',
          returnedCount: 4,
          truncated: true,
        }),
      }),
    },
  })
}

describe('Search response presentation mapping', () => {
  it('preserves result and status order while keeping grouped evidence on its surface', () => {
    const view = mapTraceSearchResponse(groupedResponse())
    if (view.state !== 'available') throw new Error('expected an available response')

    expect(view.results).toEqual([
      {
        description: 'Workflow jobs and their current conclusions.',
        fields: [
          { dataType: 'Utf8', name: 'conclusion', required: false },
          { dataType: 'Utf8', name: 'owner', required: true },
        ],
        guide: 'Filter by owner before querying recent jobs.',
        kind: 'table',
        matchingValues: [
          { field: 'conclusion', values: ['success', 'failure'] },
          { field: 'owner', values: ['withcoral'] },
        ],
        omittedMatchingFieldCount: 2,
        providers: [
          { label: 'Catalog', tone: 'catalog' },
          { label: 'Observed values', tone: 'observed' },
        ],
        rank: 1,
        sqlReference: 'github.repo_action_jobs',
      },
      {
        arguments: [
          { dataType: 'Utf8', name: 'query', required: true },
          { dataType: 'Int64', name: 'z_limit', required: false },
        ],
        description: '',
        guide: '',
        kind: 'function',
        matchingValues: [],
        omittedMatchingFieldCount: 0,
        providers: [
          { label: 'Native fanout', tone: 'neutral' },
          { label: 'Unknown provider', tone: 'neutral' },
        ],
        rank: 2,
        returns: [
          { dataType: 'Int64', name: 'issue_id', required: false },
          { dataType: 'Utf8', name: 'title', required: false },
        ],
        sqlReference: '"Warehouse-Prod".analytics."Search Issues"',
      },
      { kind: 'unknown', rank: 3 },
      { kind: 'unknown', rank: 4 },
    ])
    expect(view.providerStatuses.map((status) => status.provider.label)).toEqual([
      'Native fanout',
      'Catalog',
    ])
    expect(view.providerStatuses[0]).toEqual({
      coverage: {
        budgetExhausted: true,
        eligibleUnits: 4,
        failedUnits: 1,
        hasMore: true,
        returnedCount: 2,
        searchedUnits: 3,
        staleIndex: true,
        timedOut: false,
      },
      note: 'One source timed out.',
      provider: { label: 'Native fanout', tone: 'neutral' },
      state: 'Partial',
    })
    expect(view.truncation).toEqual({
      maxResults: 4,
      note: 'More candidates were available.',
      returnedCount: 4,
      truncated: true,
    })
    expect(searchResultsTabLabel(view)).toBe('Results 4')
  })

  it('distinguishes an available empty response from unavailable and oversized history', () => {
    const empty = mapTraceSearchResponse(
      create(TraceSearchResponseSchema, {
        outcome: { case: 'response', value: create(SearchResponseSchema) },
      }),
    )
    const unavailable = mapTraceSearchResponse()
    const tooLarge = mapTraceSearchResponse(
      create(TraceSearchResponseSchema, {
        outcome: { case: 'tooLarge', value: create(TraceSearchResponseTooLargeSchema) },
      }),
    )

    expect(empty).toEqual({ providerStatuses: [], results: [], state: 'available' })
    expect(searchResultsTabLabel(empty)).toBe('Results 0')
    expect(unavailable).toEqual({ state: 'unavailable' })
    expect(searchResultsTabLabel(unavailable)).toBe('Results')
    expect(tooLarge).toEqual({ state: 'tooLarge' })
    expect(searchResultsTabLabel(tooLarge)).toBe('Results')
  })

  it('quotes SQL identifiers exactly like the shared Rust renderer', () => {
    expect(formatSearchSqlIdentifier('simple_name_2')).toBe('simple_name_2')
    expect(formatSearchSqlIdentifier('Git"Hub')).toBe('"Git""Hub"')
    expect(formatSearchSqlIdentifier('')).toBe('""')
  })
})
