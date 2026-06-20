export type TierName = "GENERAL" | "VIP" | "PLATINUM";

export type SagaStep =
  | "INITIATED"
  | "PAYMENT_PROCESSED"
  | "INVENTORY_RESERVED"
  | "SUCCESS"
  | "ROLLED_BACK"
  | "TIMED_OUT"
  | "FAILED_PERMANENTLY";

export interface PurchaseResponse {
  transaction_id: string;
  status: string;
  message: string;
}

export interface SagaStatusResponse {
  transaction_id: string;
  status: string;
  event_id?: string;
  tier_name?: string;
  quantity?: number;
  reserved_at?: string;
}

export interface TierInfo {
  name: TierName;
  label: string;
  priceCents: number;
  available: number;
  color: string;
  gradient: string;
}

export interface TelemetrySnapshot {
  totalSales: number;
  totalRollbacks: number;
  activeSagas: number;
  ticketsRemaining: Record<TierName, number>;
  totalRevenueCents: number;
}

export const TIER_CONFIG: Record<TierName, Omit<TierInfo, "available">> = {
  GENERAL: {
    name: "GENERAL",
    label: "General Admission",
    priceCents: 10000,
    color: "blue",
    gradient: "from-blue-500 to-blue-700",
  },
  VIP: {
    name: "VIP",
    label: "VIP Access",
    priceCents: 25000,
    color: "purple",
    gradient: "from-purple-500 to-purple-700",
  },
  PLATINUM: {
    name: "PLATINUM",
    label: "Platinum Experience",
    priceCents: 50000,
    color: "amber",
    gradient: "from-amber-500 to-amber-700",
  },
};
