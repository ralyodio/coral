export function isWindowsRequest(request: Request): boolean {
  const clientHint = request.headers.get('sec-ch-ua-platform')?.replaceAll('"', '')
  if (clientHint === 'Windows') return true

  return /Windows NT/i.test(request.headers.get('user-agent') ?? '')
}
