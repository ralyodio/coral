import { SearchProvider, SearchProviderState } from '@/generated/coral/v1/search_pb'

interface SearchFieldData {
  dataType: string
  name: string
  required: boolean
}

interface SearchFieldValuesData {
  field: string
  values: string[]
}

interface SearchProviderCoverageData {
  budgetExhausted: boolean
  eligibleUnits: number
  failedUnits: number
  hasMore: boolean
  returnedCount: number
  searchedUnits: number
  staleIndex: boolean
  timedOut: boolean
}

interface SearchProviderStatusData {
  coverage?: SearchProviderCoverageData
  note: string
  provider: SearchProvider
  state: SearchProviderState
}

interface SearchResultTruncationData {
  maxResults: number
  note: string
  returnedCount: number
  truncated: boolean
}

interface SearchSurfaceRefData {
  catalogName: string
  name: string
  schemaName: string
}

interface SearchResultData {
  description: string
  guide: string
  matchingValues: SearchFieldValuesData[]
  omittedMatchingFieldCount: number
  providers: SearchProvider[]
  shape:
    | { case: 'function'; value: { arguments: SearchFieldData[]; returns: SearchFieldData[] } }
    | { case: 'table'; value: { fields: SearchFieldData[] } }
    | { case: undefined; value?: undefined }
  surface?: SearchSurfaceRefData
}

interface SearchResponseData {
  providerStatuses: SearchProviderStatusData[]
  results: SearchResultData[]
  truncation?: SearchResultTruncationData
}

export interface TraceSearchResponseData {
  outcome:
    | { case: 'response'; value: SearchResponseData }
    | { case: 'tooLarge'; value: object }
    | { case: undefined; value?: undefined }
}

export interface SearchFieldView {
  dataType: string
  name: string
  required: boolean
}

export interface SearchMatchingValuesView {
  field: string
  values: string[]
}

export interface SearchProviderView {
  label: string
  tone: 'catalog' | 'neutral' | 'observed'
}

export interface SearchProviderCoverageView {
  budgetExhausted: boolean
  eligibleUnits: number
  failedUnits: number
  hasMore: boolean
  returnedCount: number
  searchedUnits: number
  staleIndex: boolean
  timedOut: boolean
}

export interface SearchProviderStatusView {
  coverage?: SearchProviderCoverageView
  note: string
  provider: SearchProviderView
  state: string
}

export interface SearchTruncationView {
  maxResults: number
  note: string
  returnedCount: number
  truncated: boolean
}

export interface SearchKnownResultViewBase {
  description: string
  guide: string
  matchingValues: SearchMatchingValuesView[]
  omittedMatchingFieldCount: number
  providers: SearchProviderView[]
  rank: number
  sqlReference: string
}

export interface SearchTableResultView extends SearchKnownResultViewBase {
  fields: SearchFieldView[]
  kind: 'table'
}

export interface SearchFunctionResultView extends SearchKnownResultViewBase {
  arguments: SearchFieldView[]
  kind: 'function'
  returns: SearchFieldView[]
}

export interface SearchUnknownResultView {
  kind: 'unknown'
  rank: number
}

export type SearchResultView =
  | SearchFunctionResultView
  | SearchTableResultView
  | SearchUnknownResultView

export type SearchResultsView =
  | {
      providerStatuses: SearchProviderStatusView[]
      results: SearchResultView[]
      state: 'available'
      truncation?: SearchTruncationView
    }
  | { state: 'tooLarge' }
  | { state: 'unavailable' }

function compareNames(left: { name: string }, right: { name: string }): number {
  if (left.name < right.name) return -1
  if (left.name > right.name) return 1
  return 0
}

function fieldMap(fields: SearchFieldData[]): SearchFieldView[] {
  const fieldsByName = new Map<string, SearchFieldView>()
  for (const field of fields) {
    fieldsByName.set(field.name, {
      dataType: field.dataType,
      name: field.name,
      required: field.required,
    })
  }
  return [...fieldsByName.values()].toSorted(compareNames)
}

function matchingValues(values: SearchFieldValuesData[]): SearchMatchingValuesView[] {
  const valuesByField = new Map<string, SearchMatchingValuesView>()
  for (const entry of values) {
    valuesByField.set(entry.field, { field: entry.field, values: [...entry.values] })
  }
  return [...valuesByField.values()].toSorted((left, right) =>
    compareNames({ name: left.field }, { name: right.field }),
  )
}

export function formatSearchSqlIdentifier(identifier: string): string {
  if (/^[a-z_][a-z0-9_]*$/.test(identifier)) return identifier
  return `"${identifier.replaceAll('"', '""')}"`
}

export function formatSearchSqlReference(surface: SearchSurfaceRefData): string {
  return [surface.catalogName || undefined, surface.schemaName, surface.name]
    .filter((part): part is string => part !== undefined)
    .map(formatSearchSqlIdentifier)
    .join('.')
}

function mapProvider(provider: SearchProvider): SearchProviderView {
  if (provider === SearchProvider.CATALOG_METADATA) {
    return { label: 'Catalog', tone: 'catalog' }
  }
  if (provider === SearchProvider.OBSERVED_VALUES) {
    return { label: 'Observed values', tone: 'observed' }
  }
  if (provider === SearchProvider.NATIVE_FANOUT) {
    return { label: 'Native fanout', tone: 'neutral' }
  }
  return { label: 'Unknown provider', tone: 'neutral' }
}

function providerStateLabel(state: SearchProviderState): string {
  if (state === SearchProviderState.RESULTS_FOUND) return 'Results found'
  if (state === SearchProviderState.EMPTY) return 'Empty'
  if (state === SearchProviderState.NOT_ENABLED) return 'Not enabled'
  if (state === SearchProviderState.SKIPPED) return 'Skipped'
  if (state === SearchProviderState.PARTIAL) return 'Partial'
  if (state === SearchProviderState.ERROR) return 'Error'
  return 'Unknown'
}

function mapCoverage(coverage: SearchProviderCoverageData): SearchProviderCoverageView {
  return {
    budgetExhausted: coverage.budgetExhausted,
    eligibleUnits: coverage.eligibleUnits,
    failedUnits: coverage.failedUnits,
    hasMore: coverage.hasMore,
    returnedCount: coverage.returnedCount,
    searchedUnits: coverage.searchedUnits,
    staleIndex: coverage.staleIndex,
    timedOut: coverage.timedOut,
  }
}

function mapProviderStatus(status: SearchProviderStatusData): SearchProviderStatusView {
  return {
    ...(status.coverage ? { coverage: mapCoverage(status.coverage) } : {}),
    note: status.note,
    provider: mapProvider(status.provider),
    state: providerStateLabel(status.state),
  }
}

function mapTruncation(truncation: SearchResultTruncationData): SearchTruncationView {
  return {
    maxResults: truncation.maxResults,
    note: truncation.note,
    returnedCount: truncation.returnedCount,
    truncated: truncation.truncated,
  }
}

function knownResultBase(result: SearchResultData, rank: number, surface: SearchSurfaceRefData) {
  return {
    description: result.description,
    guide: result.guide,
    matchingValues: matchingValues(result.matchingValues),
    omittedMatchingFieldCount: result.omittedMatchingFieldCount,
    providers: result.providers.map(mapProvider),
    rank,
    sqlReference: formatSearchSqlReference(surface),
  }
}

function mapResult(result: SearchResultData, rank: number): SearchResultView {
  if (!result.surface) return { kind: 'unknown', rank }

  if (result.shape.case === 'table') {
    return {
      ...knownResultBase(result, rank, result.surface),
      fields: fieldMap(result.shape.value.fields),
      kind: 'table',
    }
  }

  if (result.shape.case === 'function') {
    return {
      ...knownResultBase(result, rank, result.surface),
      arguments: fieldMap(result.shape.value.arguments),
      kind: 'function',
      returns: fieldMap(result.shape.value.returns),
    }
  }

  return { kind: 'unknown', rank }
}

export function mapTraceSearchResponse(
  searchResponse?: TraceSearchResponseData,
): SearchResultsView {
  if (!searchResponse) return { state: 'unavailable' }
  if (searchResponse.outcome.case === 'tooLarge') return { state: 'tooLarge' }
  if (searchResponse.outcome.case !== 'response') return { state: 'unavailable' }

  const response = searchResponse.outcome.value
  const view: Extract<SearchResultsView, { state: 'available' }> = {
    providerStatuses: response.providerStatuses.map(mapProviderStatus),
    results: response.results.map((result, index) => mapResult(result, index + 1)),
    state: 'available',
  }
  if (response.truncation) view.truncation = mapTruncation(response.truncation)
  return view
}

export function searchResultsTabLabel(view: SearchResultsView): string {
  return view.state === 'available' ? `Results ${view.results.length}` : 'Results'
}
