// Numbers and times as people read them.

const UNITS = ["bytes", "KiB", "MiB", "GiB", "TiB"];

export function formatBytes(n: number): string {
  if (n === 1) {
    return "1 byte";
  }
  let v = n;
  let u = 0;
  while (v >= 1024 && u < UNITS.length - 1) {
    v /= 1024;
    u++;
  }
  const digits = u === 0 || Number.isInteger(v) ? 0 : v < 10 ? 1 : 0;
  return `${v.toFixed(digits)} ${UNITS[u]}`;
}

/** Local wall-clock time, 24-hour, to the second. */
export function formatTime(unixMillis: number): string {
  return new Date(unixMillis).toLocaleTimeString("en-GB", { hour12: false });
}

/** A duration in whole seconds, minutes or hours: "42 s", "3 min", "2 h". */
export function formatDuration(ms: number): string {
  const s = Math.max(0, Math.floor(ms / 1000));
  if (s < 120) {
    return `${s} s`;
  }
  if (s < 7200) {
    return `${Math.floor(s / 60)} min`;
  }
  return `${Math.floor(s / 3600)} h`;
}
