// Tiny presentation helpers shared by the views.

export const shortId = (id: string): string => id.slice(0, 8);

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
