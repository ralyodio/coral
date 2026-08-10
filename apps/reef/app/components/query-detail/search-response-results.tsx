import { Banner } from '@/wax/components/banner'
import { Icon } from '@/wax/components/icon'
import { Pill, type PillColor } from '@/wax/components/pill'
import { Tooltip } from '@/wax/components/tooltip'
import { Typography } from '@/wax/components/typography'

import type {
  SearchFieldView,
  SearchFunctionResultView,
  SearchKnownResultViewBase,
  SearchMatchingValuesView,
  SearchProviderCoverageView,
  SearchProviderStatusView,
  SearchProviderView,
  SearchResultView,
  SearchResultsView,
  SearchTableResultView,
  SearchTruncationView,
  SearchUnknownResultView,
} from './search-response'
import * as styles from './search-response-results.css'

export interface SearchResponseResultsProps {
  view: SearchResultsView
}

function providerColor(provider: SearchProviderView): PillColor {
  if (provider.tone === 'catalog') return 'blue'
  if (provider.tone === 'observed') return 'purple'
  return 'graySubtle'
}

function ProviderBadge({ provider }: { provider: SearchProviderView }) {
  return (
    <Pill as="span" color={providerColor(provider)}>
      {provider.label}
    </Pill>
  )
}

function Fields({
  fields,
  requiredLabel,
  title,
}: {
  fields: SearchFieldView[]
  requiredLabel?: string
  title: string
}) {
  if (fields.length === 0) return null
  return (
    <section className={styles.section}>
      <Typography.BodySmallStrong as="h3">{title}</Typography.BodySmallStrong>
      <dl className={styles.fieldList}>
        {fields.map((field) => (
          <div className={styles.fieldRow} key={field.name}>
            <Typography.CodeSmallInlineStrong as="dt" className={styles.fieldNameCell}>
              <span className={styles.fieldName}>{field.name}</span>
              {field.required && requiredLabel ? (
                <Tooltip content={requiredLabel} side="top">
                  <span aria-label={requiredLabel} className={styles.requiredStar} tabIndex={0}>
                    *
                  </span>
                </Tooltip>
              ) : null}
            </Typography.CodeSmallInlineStrong>
            <Typography.CodeSmallInline as="dd" className={styles.fieldTypeCell} variant="tertiary">
              {field.dataType || 'Type unavailable'}
            </Typography.CodeSmallInline>
          </div>
        ))}
      </dl>
    </section>
  )
}

function MatchingValues({ groups }: { groups: SearchMatchingValuesView[] }) {
  if (groups.length === 0) return null
  return (
    <section className={styles.section}>
      <Typography.BodySmallStrong as="h3">Matching values</Typography.BodySmallStrong>
      <dl className={styles.matchingValues}>
        {groups.map((group) => (
          <div className={styles.matchingValueRow} key={group.field}>
            <Typography.CodeSmallInlineStrong as="dt">
              {group.field}
            </Typography.CodeSmallInlineStrong>
            <Typography.BodySmall as="dd" variant="secondary">
              {group.values.length > 0 ? group.values.join(', ') : 'No values retained'}
            </Typography.BodySmall>
          </div>
        ))}
      </dl>
    </section>
  )
}

function ResultDescription({ result }: { result: SearchKnownResultViewBase }) {
  if (!result.description) return null
  return (
    <Typography.Body as="p" variant="secondary">
      {result.description}
    </Typography.Body>
  )
}

function CommonResultSections({ result }: { result: SearchKnownResultViewBase }) {
  return (
    <>
      <MatchingValues groups={result.matchingValues} />
      {result.omittedMatchingFieldCount > 0 ? (
        <Typography.BodySmall as="p" variant="tertiary">
          {result.omittedMatchingFieldCount} more matching field
          {result.omittedMatchingFieldCount === 1 ? '' : 's'} not shown.
        </Typography.BodySmall>
      ) : null}
      {result.guide ? (
        <section className={styles.section}>
          <Typography.BodySmallStrong as="h3">Guide</Typography.BodySmallStrong>
          <Typography.Body as="p" variant="secondary">
            {result.guide}
          </Typography.Body>
        </section>
      ) : null}
    </>
  )
}

function TableResultBody({ result }: { result: SearchTableResultView }) {
  return (
    <>
      <ResultDescription result={result} />
      <Fields fields={result.fields} requiredLabel="Required filter" title="Fields" />
      <CommonResultSections result={result} />
    </>
  )
}

function FunctionResultBody({ result }: { result: SearchFunctionResultView }) {
  return (
    <>
      <ResultDescription result={result} />
      <Fields fields={result.arguments} requiredLabel="Required argument" title="Arguments" />
      <Fields fields={result.returns} title="Returns" />
      <CommonResultSections result={result} />
    </>
  )
}

function ResultSummary({ result }: { result: SearchFunctionResultView | SearchTableResultView }) {
  return (
    <span className={styles.summaryLayout}>
      <Icon className={styles.disclosureIcon} name="ChevronRight" size="16" />
      <Typography.BodySmallStrong as="span" className={styles.rankCell} variant="tertiary">
        #{result.rank}
      </Typography.BodySmallStrong>
      <span className={styles.kindCell}>
        <Pill as="span" color={result.kind === 'table' ? 'green' : 'orange'}>
          {result.kind === 'table' ? 'Table' : 'Function'}
        </Pill>
      </span>
      <Typography.CodeInlineStrong
        as="code"
        className={styles.sqlReference}
        title={result.sqlReference}
      >
        {result.sqlReference}
      </Typography.CodeInlineStrong>
      {result.providers.length > 0 ? (
        <span className={styles.foundVia}>
          <Typography.BodySmall as="span" variant="tertiary">
            Found via
          </Typography.BodySmall>
          {result.providers.map((provider, index) => (
            <ProviderBadge key={`${provider.label}-${index}`} provider={provider} />
          ))}
        </span>
      ) : null}
    </span>
  )
}

function KnownResult({ result }: { result: SearchFunctionResultView | SearchTableResultView }) {
  return (
    <details className={styles.resultCard}>
      <summary className={styles.resultSummary}>
        <ResultSummary result={result} />
      </summary>
      <div className={styles.resultBody}>
        {result.kind === 'table' ? (
          <TableResultBody result={result} />
        ) : (
          <FunctionResultBody result={result} />
        )}
      </div>
    </details>
  )
}

function UnknownResult({ result }: { result: SearchUnknownResultView }) {
  return (
    <article className={styles.unknownCard}>
      <span className={styles.summaryLayout}>
        <span aria-hidden="true" className={styles.disclosureSpacer} />
        <Typography.BodySmallStrong as="span" className={styles.rankCell} variant="tertiary">
          #{result.rank}
        </Typography.BodySmallStrong>
        <span className={styles.kindCell}>
          <Pill as="span" color="graySubtle">
            Unknown
          </Pill>
        </span>
        <Typography.BodyStrong as="span" className={styles.unknownLabel}>
          Unknown result
        </Typography.BodyStrong>
      </span>
    </article>
  )
}

function ResultCard({ result }: { result: SearchResultView }) {
  return result.kind === 'unknown' ? (
    <UnknownResult result={result} />
  ) : (
    <KnownResult result={result} />
  )
}

function coverageText(coverage: SearchProviderCoverageView): string {
  const parts = [
    `Searched ${coverage.searchedUnits} of ${coverage.eligibleUnits} eligible`,
    `${coverage.returnedCount} returned`,
    `${coverage.failedUnits} failed`,
  ]
  if (coverage.hasMore) parts.push('More available')
  if (coverage.budgetExhausted) parts.push('Budget exhausted')
  if (coverage.timedOut) parts.push('Timed out')
  if (coverage.staleIndex) parts.push('Stale index')
  return parts.join(' · ')
}

function ProviderStatus({ status }: { status: SearchProviderStatusView }) {
  return (
    <li className={styles.providerStatus}>
      <div className={styles.providerStatusHeader}>
        <ProviderBadge provider={status.provider} />
        <Typography.BodySmallStrong as="span">{status.state}</Typography.BodySmallStrong>
      </div>
      <div className={styles.providerStatusCopy}>
        {status.note ? (
          <Typography.BodySmall as="p" variant="secondary">
            {status.note}
          </Typography.BodySmall>
        ) : null}
        {status.coverage ? (
          <Typography.BodySmall as="p" variant="tertiary">
            {coverageText(status.coverage)}
          </Typography.BodySmall>
        ) : null}
      </div>
    </li>
  )
}

function ProviderStatuses({ statuses }: { statuses: SearchProviderStatusView[] }) {
  return (
    <section className={styles.providerSection}>
      <Typography.HeadingXSmall as="h2">Provider status</Typography.HeadingXSmall>
      {statuses.length > 0 ? (
        <ul className={styles.providerStatusList}>
          {statuses.map((status, index) => (
            <ProviderStatus key={`${status.provider.label}-${index}`} status={status} />
          ))}
        </ul>
      ) : (
        <Typography.Body variant="tertiary">No provider status reported.</Typography.Body>
      )}
    </section>
  )
}

function Truncation({ truncation }: { truncation: SearchTruncationView }) {
  return (
    <Banner title={truncation.truncated ? 'Results truncated' : 'Result limit'}>
      <Typography.BodySmall as="p">
        Returned {truncation.returnedCount} of {truncation.maxResults} requested results
        {truncation.truncated ? ' (truncated)' : ''}.
      </Typography.BodySmall>
      {truncation.note ? (
        <Typography.BodySmall as="p" variant="tertiary">
          {truncation.note}
        </Typography.BodySmall>
      ) : null}
    </Banner>
  )
}

function ResultState({ children }: { children: string }) {
  return (
    <div className={styles.resultState}>
      <Typography.Body variant="tertiary">{children}</Typography.Body>
    </div>
  )
}

export function SearchResponseResults({ view }: SearchResponseResultsProps) {
  if (view.state === 'unavailable') {
    return (
      <div className={styles.root}>
        <ResultState>Results are unavailable for this search.</ResultState>
      </div>
    )
  }
  if (view.state === 'tooLarge') {
    return (
      <div className={styles.root}>
        <ResultState>This result response was too large to retain.</ResultState>
      </div>
    )
  }

  return (
    <div className={styles.root}>
      {view.results.length > 0 ? (
        <ol className={styles.resultList}>
          {view.results.map((result) => (
            <li className={styles.resultRow} key={result.rank}>
              <ResultCard result={result} />
            </li>
          ))}
        </ol>
      ) : (
        <ResultState>No results found for this search.</ResultState>
      )}
      {view.truncation ? <Truncation truncation={view.truncation} /> : null}
      <ProviderStatuses statuses={view.providerStatuses} />
    </div>
  )
}
