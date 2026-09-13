/** Shared formatting helpers. Previously copy-pasted into each page. */

/**
 * Pulls a readable message out of an axios error, preferring the backend's own
 * `{ error, message }` body over the generic HTTP status text.
 */
export function describeError(err: unknown, fallback: string): string {
  const detail = (err as { response?: { data?: { error?: string; message?: string } } })
    ?.response?.data;
  return detail?.message ?? detail?.error ?? (err as Error)?.message ?? fallback;
}

/** Renders a timestamp in local time, passing through anything unparseable. */
export function formatTime(value: string | null | undefined): string {
  if (!value) return '—';
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString();
}
