import type { PurchaseResponse, SagaStatusResponse } from "./types";

const API_BASE =
  process.env.NEXT_PUBLIC_API_BASE_URL ||
  "http://localhost:8000";

export async function buyTicket(payload: {
  user_id: string;
  event_id: string;
  tier_name: string;
  quantity: number;
}): Promise<PurchaseResponse> {
  const res = await fetch(`${API_BASE}/buy-ticket`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  if (!res.ok) {
    throw new Error(extractErrorMessage(await res.json().catch(() => ({})), `Purchase failed (${res.status})`));
  }
  return res.json();
}

export async function getSagaStatus(
  transactionId: string
): Promise<SagaStatusResponse | null> {
  const res = await fetch(`${API_BASE}/status/${transactionId}`);
  if (res.status === 404) return null;
  if (!res.ok) {
    throw new Error(extractErrorMessage(await res.json().catch(() => ({})), `Status check failed (${res.status})`));
  }
  return res.json();
}

function extractErrorMessage(body: unknown, fallback: string): string {
  if (typeof body === "string" && body) return body;
  if (body && typeof body === "object") {
    const obj = body as Record<string, unknown>;
    if (typeof obj.message === "string") return obj.message;
    if (obj.detail && typeof obj.detail === "object") {
      const inner = obj.detail as Record<string, unknown>;
      if (typeof inner.message === "string") return inner.message;
    }
    if (typeof obj.detail === "string") return obj.detail;
  }
  return fallback;
}
