"use client";

import { useState } from "react";
import { buyTicket } from "@/lib/api";
import { TIER_CONFIG } from "@/lib/types";
import type { PurchaseResponse, TierName } from "@/lib/types";
import SagaVisualizer from "@/components/SagaVisualizer";

const EVENT_ID = process.env.NEXT_PUBLIC_FLASH_SALE_EVENT_ID || "cafe0f00-0000-4000-a000-000000000000";
const SEED_USERS = [
  "aaaaaaaa-1111-1111-1111-111111111111",
  "bbbbbbbb-2222-2222-2222-222222222222",
  "cccccccc-3333-3333-3333-333333333333",
  "dddddddd-4444-4444-4444-444444444444",
  "eeeeeeee-5555-5555-5555-555555555555",
  "0000000f-1111-1111-1111-111111111111",
  "00000010-1111-1111-1111-111111111111",
  "00000011-1111-1111-1111-111111111111",
  "00000012-1111-1111-1111-111111111111",
  "00000013-1111-1111-1111-111111111111",
  "00000014-1111-1111-1111-111111111111",
  "00000015-1111-1111-1111-111111111111",
  "00000016-1111-1111-1111-111111111111",
  "00000017-1111-1111-1111-111111111111",
  "00000018-1111-1111-1111-111111111111",
  "00000019-1111-1111-1111-111111111111",
  "0000001a-1111-1111-1111-111111111111",
  "0000001b-1111-1111-1111-111111111111",
  "0000001c-1111-1111-1111-111111111111",
  "0000001d-1111-1111-1111-111111111111",
];
const TIERS = Object.keys(TIER_CONFIG) as TierName[];

function pickRandomUser(): string {
  return SEED_USERS[Math.floor(Math.random() * SEED_USERS.length)];
}

export default function HomePage() {
  const [selectedTier, setSelectedTier] = useState<TierName>("GENERAL");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [purchase, setPurchase] = useState<PurchaseResponse | null>(null);
  const [transactionIds, setTransactionIds] = useState<string[]>([]);

  async function handleBuy() {
    setLoading(true);
    setError(null);
    setPurchase(null);

    try {
      const result = await buyTicket({
        user_id: pickRandomUser(),
        event_id: EVENT_ID,
        tier_name: selectedTier,
        quantity: 1,
      });
      setPurchase(result);
      setTransactionIds((prev) => [result.transaction_id, ...prev]);
    } catch (e) {
      setError(e instanceof Error ? e.message : "Purchase failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-10 animate-fade-in">
      <section className="text-center">
        <div className="mx-auto mb-2 inline-flex items-center gap-2 rounded-full border border-red-500/30 bg-red-500/10 px-3 py-1 text-xs font-semibold text-danger">
          <span className="relative flex h-2 w-2">
            <span className="absolute inline-flex h-full w-full animate-ping rounded-full bg-red-400 opacity-75" />
            <span className="relative inline-flex h-2 w-2 rounded-full bg-red-500" />
          </span>
          FLASH SALE LIVE
        </div>
        <h2 className="text-3xl font-bold tracking-tight sm:text-4xl">
          Summer Music Festival 2026
        </h2>
        <p className="mt-2 text-[rgb(var(--muted))]">
          Limited tickets available. Act fast &mdash; first come, first served.
        </p>
      </section>

      <section>
        <h3 className="mb-3 text-sm font-semibold uppercase tracking-wider text-[rgb(var(--muted))]">
          Select Your Tier
        </h3>
        <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
          {TIERS.map((tier) => {
            const config = TIER_CONFIG[tier];
            const isSelected = selectedTier === tier;
            return (
              <button
                key={tier}
                onClick={() => setSelectedTier(tier)}
                className={`card cursor-pointer p-5 text-left transition-all ${
                  isSelected
                    ? "border-blue-500 ring-2 ring-blue-500/30"
                    : "hover:border-[rgb(var(--muted))]"
                }`}
              >
                <div className={`mb-3 inline-block rounded bg-gradient-to-r ${config.gradient} px-3 py-1 text-xs font-bold text-white`}>
                  {config.label}
                </div>
                <p className="text-2xl font-bold">
                  ${(config.priceCents / 100).toFixed(0)}
                </p>
                <p className="mt-1 text-xs text-[rgb(var(--muted))]">per ticket</p>
              </button>
            );
          })}
        </div>
      </section>

      <div className="flex justify-center">
        <button
          onClick={handleBuy}
          disabled={loading}
          className={`group relative inline-flex items-center gap-2 rounded-lg bg-gradient-to-r from-blue-600 to-blue-700 px-8 py-3 text-sm font-semibold text-white shadow-lg shadow-blue-500/25 transition-all hover:from-blue-500 hover:to-blue-600 disabled:cursor-not-allowed disabled:opacity-60 ${
            loading ? "animate-pulse-fast" : ""
          }`}
        >
          {loading ? (
            <>
              <svg
                className="h-4 w-4 animate-spin"
                viewBox="0 0 24 24"
                fill="none"
              >
                <circle
                  className="opacity-25"
                  cx="12" cy="12" r="10"
                  stroke="currentColor" strokeWidth="4"
                />
                <path
                  className="opacity-75"
                  fill="currentColor"
                  d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"
                />
              </svg>
              Processing&hellip;
            </>
          ) : (
            "Purchase Ticket"
          )}
        </button>
      </div>

      {error && (
        <div className="mx-auto max-w-md rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3 text-center text-sm text-danger">
          {error}
        </div>
      )}

      {purchase && (
        <div className="mx-auto max-w-lg animate-slide-up">
          <p className="mb-3 text-center text-sm font-medium text-emerald-400">
            Purchase Accepted! Transaction {purchase.transaction_id.slice(0, 8)}&hellip;
          </p>
          <SagaVisualizer transactionId={purchase.transaction_id} />
        </div>
      )}

      {transactionIds.length > 0 && !purchase && (
        <div className="mx-auto max-w-lg space-y-6">
          {transactionIds.map((id) => (
            <SagaVisualizer key={id} transactionId={id} />
          ))}
        </div>
      )}
    </div>
  );
}
