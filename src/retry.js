// Retry policy: exponential backoff with full / equal / no jitter (AWS architecture blog).
export function backoff(attempt, { base = 0.5, cap = 30, factor = 2, jitter = "full" } = {}, rand = Math.random) {
  const exp = Math.min(cap, base * Math.pow(factor, attempt - 1));
  if (jitter === "full") return rand() * exp;
  if (jitter === "equal") return exp / 2 + rand() * (exp / 2);
  return exp;
}
