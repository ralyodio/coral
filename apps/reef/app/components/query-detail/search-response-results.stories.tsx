import { useEffect, useRef } from 'react'
import type { Meta, StoryObj } from '@storybook/react-vite'

import type { SearchResultsView } from './search-response'
import { SearchResponseResults } from './search-response-results'

const groupedView: SearchResultsView = {
  providerStatuses: [
    {
      coverage: {
        budgetExhausted: false,
        eligibleUnits: 3,
        failedUnits: 0,
        hasMore: false,
        returnedCount: 2,
        searchedUnits: 3,
        staleIndex: false,
        timedOut: false,
      },
      note: 'Catalog search completed.',
      provider: { label: 'Catalog', tone: 'catalog' },
      state: 'Results found',
    },
    {
      coverage: {
        budgetExhausted: false,
        eligibleUnits: 2,
        failedUnits: 0,
        hasMore: true,
        returnedCount: 1,
        searchedUnits: 2,
        staleIndex: false,
        timedOut: false,
      },
      note: 'More observed matches were available.',
      provider: { label: 'Observed values', tone: 'observed' },
      state: 'Partial',
    },
  ],
  results: [
    {
      description: 'Workflow jobs and their current conclusions.',
      fields: [
        { dataType: 'Utf8', name: 'conclusion', required: false },
        { dataType: 'Utf8', name: 'owner', required: true },
        { dataType: 'Utf8', name: 'repository', required: false },
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
        {
          dataType: 'Struct<repository_owner_namespace_with_a_very_long_nested_type_name>',
          name: 'required_argument_with_a_very_long_unbroken_name_that_wraps',
          required: true,
        },
        { dataType: 'Int64', name: 'z_limit', required: false },
      ],
      description: 'Searches issues using a provider-native function.',
      guide: '',
      kind: 'function',
      matchingValues: [],
      omittedMatchingFieldCount: 0,
      providers: [{ label: 'Native fanout', tone: 'neutral' }],
      rank: 2,
      returns: [
        { dataType: 'Int64', name: 'issue_id', required: false },
        { dataType: 'Utf8', name: 'title', required: false },
      ],
      sqlReference: '"Warehouse-Prod".analytics."Search Issues"',
    },
    { kind: 'unknown', rank: 3 },
  ],
  state: 'available',
  truncation: {
    maxResults: 3,
    note: 'More candidates were available.',
    returnedCount: 3,
    truncated: true,
  },
}

function ExpandedResults({ view }: { view: SearchResultsView }) {
  const rootRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    rootRef.current?.querySelectorAll('details').forEach((details) => {
      details.open = true
    })
  }, [])
  return (
    <div ref={rootRef}>
      <SearchResponseResults view={view} />
    </div>
  )
}

const meta = {
  args: { view: groupedView },
  component: SearchResponseResults,
  parameters: { layout: 'padded' },
  tags: ['autodocs'],
  title: 'Components/QueryDetail/SearchResponseResults',
} satisfies Meta<typeof SearchResponseResults>

export default meta
type Story = StoryObj<typeof meta>

export const Grouped: Story = {}

export const GroupedExpanded: Story = {
  render: ({ view }) => <ExpandedResults view={view} />,
}

export const Empty: Story = {
  args: { view: { providerStatuses: [], results: [], state: 'available' } },
}

export const Unavailable: Story = {
  args: { view: { state: 'unavailable' } },
}

export const TooLarge: Story = {
  args: { view: { state: 'tooLarge' } },
}
