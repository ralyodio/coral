import { globalStyle, style } from '@vanilla-extract/css'

import { breakpoints } from '@/styles/theme.css'
import { fontFamily } from '@/wax/theme/font.css'
import { theme } from '@/wax/theme/theme.css'

export const root = style({
  display: 'flex',
  flex: 1,
  flexDirection: 'column',
  gap: 16,
  minHeight: 0,
  minWidth: 0,
  overflowY: 'auto',
  paddingInlineEnd: 4,
})

export const resultList = style({
  backgroundColor: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 10,
  display: 'flex',
  flexDirection: 'column',
  listStyle: 'none',
  margin: 0,
  overflow: 'hidden',
  padding: 0,
})

export const resultRow = style({ minWidth: 0 })

globalStyle(`${resultRow} + ${resultRow}`, {
  borderBlockStart: `1px solid ${theme.stroke.secondary}`,
})

export const resultCard = style({
  minWidth: 0,
  overflow: 'hidden',
})

export const unknownCard = style({
  minWidth: 0,
  paddingBlock: 12,
  paddingInline: 14,
})

export const resultSummary = style({
  cursor: 'pointer',
  listStyle: 'none',
  paddingBlock: 12,
  paddingInline: 14,
  selectors: {
    '&:focus': { outline: 'none' },
    '&:focus-visible': {
      outline: `1px solid ${theme.stroke.focused}`,
      outlineOffset: -2,
    },
    '&:hover': { backgroundColor: theme.surface.onMainContentHover },
  },
})

globalStyle(`${resultSummary}::-webkit-details-marker`, { display: 'none' })

export const summaryLayout = style({
  alignItems: 'center',
  columnGap: 10,
  display: 'grid',
  gridTemplateColumns: '16px 32px 76px minmax(0, 1fr) auto',
  minWidth: 0,
  rowGap: 8,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gridTemplateColumns: '16px 32px 76px minmax(0, 1fr)',
    },
  },
})

export const disclosureIcon = style({
  flexShrink: 0,
  selectors: {
    [`${resultCard}[open] &`]: { transform: 'rotate(90deg)' },
  },
})

export const disclosureSpacer = style({ height: 16, width: 16 })

export const rankCell = style({
  minWidth: 0,
  textAlign: 'end',
  whiteSpace: 'nowrap',
})

export const kindCell = style({
  display: 'flex',
  justifyContent: 'flex-start',
  minWidth: 0,
})

export const sqlReference = style({
  display: 'block',
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const unknownLabel = style({
  minWidth: 0,
  overflow: 'hidden',
  textOverflow: 'ellipsis',
  whiteSpace: 'nowrap',
})

export const foundVia = style({
  alignItems: 'center',
  display: 'flex',
  flexWrap: 'wrap',
  gap: 6,
  gridColumn: 5,
  justifyContent: 'flex-end',
  maxWidth: 300,
  minWidth: 0,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gridColumn: '2 / -1',
      justifyContent: 'flex-start',
      maxWidth: 'none',
    },
  },
})

export const resultBody = style({
  borderBlockStart: `1px solid ${theme.stroke.secondary}`,
  display: 'grid',
  gap: 16,
  paddingBlock: 14,
  paddingInline: 38,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      paddingInline: 14,
    },
  },
})

export const section = style({ display: 'grid', gap: 6 })

export const fieldList = style({
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  margin: 0,
  overflow: 'hidden',
})

export const fieldRow = style({
  alignItems: 'baseline',
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'minmax(0, 1fr) minmax(0, 1fr)',
  paddingBlock: 7,
  paddingInline: 10,
})

globalStyle(`${fieldRow} + ${fieldRow}`, {
  borderBlockStart: `1px solid ${theme.stroke.secondary}`,
})
globalStyle(`${fieldRow} dd`, { margin: 0 })

export const fieldNameCell = style({
  alignItems: 'baseline',
  display: 'flex',
  minWidth: 0,
})

export const fieldName = style({
  minWidth: 0,
  overflowWrap: 'anywhere',
  whiteSpace: 'normal',
})

export const fieldTypeCell = style({
  minWidth: 0,
  overflowWrap: 'anywhere',
  whiteSpace: 'normal',
})

export const requiredStar = style({
  color: theme.content.error,
  cursor: 'help',
  display: 'inline-flex',
  flexShrink: 0,
  font: 'inherit',
  marginInlineStart: 4,
  outlineOffset: 2,
})

export const matchingValues = style({
  display: 'grid',
  gap: 6,
  margin: 0,
})

export const matchingValueRow = style({
  alignItems: 'baseline',
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'minmax(120px, 1fr) minmax(0, 3fr)',
})

globalStyle(`${matchingValueRow} dd`, {
  margin: 0,
  overflowWrap: 'anywhere',
})

export const providerSection = style({
  borderBlockStart: `1px solid ${theme.stroke.primary}`,
  display: 'grid',
  gap: 10,
  paddingBlockStart: 16,
})

export const providerStatusList = style({
  backgroundColor: theme.surface.onMainContent,
  border: `1px solid ${theme.stroke.secondary}`,
  borderRadius: 8,
  display: 'grid',
  listStyle: 'none',
  margin: 0,
  overflow: 'hidden',
  padding: 0,
})

export const providerStatus = style({
  alignItems: 'start',
  display: 'grid',
  gap: 12,
  gridTemplateColumns: 'minmax(150px, auto) minmax(0, 1fr)',
  paddingBlock: 9,
  paddingInline: 12,
  '@media': {
    [`screen and (max-width: ${breakpoints.mobile})`]: {
      gap: 5,
      gridTemplateColumns: 'minmax(0, 1fr)',
    },
  },
})

globalStyle(`${providerStatus} + ${providerStatus}`, {
  borderBlockStart: `1px solid ${theme.stroke.secondary}`,
})

export const providerStatusHeader = style({
  alignItems: 'center',
  display: 'flex',
  gap: 8,
})

export const providerStatusCopy = style({ display: 'grid', gap: 3, minWidth: 0 })

globalStyle(`${providerStatus} p`, { margin: 0 })

export const resultState = style({
  alignItems: 'center',
  border: `1px dashed ${theme.stroke.primary}`,
  borderRadius: 8,
  display: 'flex',
  justifyContent: 'center',
  minHeight: 140,
  padding: 24,
  textAlign: 'center',
})

globalStyle(`${resultBody} h3, ${providerSection} h2`, { margin: 0 })
globalStyle(`${resultBody} p`, { margin: 0 })
globalStyle(`${sqlReference}, ${fieldRow} code, ${matchingValueRow} code`, {
  fontFamily: fontFamily.dmMono,
})
