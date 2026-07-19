// Tiny presentation helpers shared by the views.

export const shortId = (id: string): string => id.slice(0, 8);

/** e.g. "1536 MiB request / 3072 MiB limit · 1500m cpu" (M2.5 resource tiers). */
export const resourceSummary = (r: {
  readonly memoryRequestMib: number;
  readonly memoryLimitMib: number;
  readonly cpuRequestMillicores: number;
}): string =>
  `${r.memoryRequestMib} MiB request / ${r.memoryLimitMib} MiB limit · ${r.cpuRequestMillicores}m cpu`;

export const timestamp = (date: Date): string => date.toISOString().replace("T", " ").slice(0, 19);

export const relativeTime = (date: Date, now: Date = new Date()): string => {
  const seconds = Math.max(0, Math.round((now.getTime() - date.getTime()) / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.round(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.round(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  return `${Math.round(hours / 24)}d ago`;
};
