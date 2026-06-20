"use client";

import { useEffect, useState } from "react";
import type { TelemetrySnapshot, TierName } from "@/lib/types";

function mockTelemetry(): TelemetrySnapshot {
  const base = {
    totalSales: Math.floor(Math.random() * 40) + 60,
    totalRollbacks: Math.floor(Math.random() * 10),
    activeSagas: Math.floor(Math.random() * 5),
    totalRevenueCents: Math.floor(Math.random() * 500000) + 4000000,
    ticketsRemaining: {
      GENERAL: Math.floor(Math.random() * 20),
      VIP: Math.floor(Math.random() * 15),
      PLATINUM: Math.floor(Math.random() * 5),
    } as Record<TierName, number>,
  };
  return base;
}

const TIER_LABELS: Record<TierName, string> = {
  GENERAL: "General Admission",
  VIP: "VIP Access",
  PLATINUM: "Platinum",
};

const TIER_COLORS: Record<TierName, string> = {
  GENERAL: "from-blue-500 to-blue-700",
  VIP: "from-purple-500 to-purple-700",
  PLATINUM: "from-amber-500 to-amber-700",
};

export default function AdminTelemetry() {
  const [data, setData] = useState<TelemetrySnapshot | null>(null);
  const [polling, setPolling] = useState(true);

  useEffect(() => {
    function refresh() {
      setData(mockTelemetry());
    }
    refresh();
    const id = setInterval(refresh, 3000);
    return () => clearInterval(id);
  }, []);

  if (!data) return null;

  const tiers = Object.keys(data.ticketsRemaining) as TierName[];

  return (
    <div className="space-y-6 animate-fade-in">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold">Admin Telemetry</h2>
        <label className="flex items-center gap-2 text-xs text-[rgb(var(--muted))]">
          <span className="relative flex h-3 w-3">
            <span
              className={`absolute inline-flex h-full w-full rounded-full ${
                polling ? "bg-emerald-400" : "bg-gray-400"
              } opacity-75`}
            />
            <span
              className={`relative inline-flex h-3 w-3 rounded-full ${
                polling ? "bg-emerald-500" : "bg-gray-500"
              }`}
            />
          </span>
          {polling ? "Live" : "Paused"}
        </label>
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <div className="card p-4">
          <p className="text-xs font-medium uppercase tracking-wider text-[rgb(var(--muted))]">
            Total Sales
          </p>
          <p className="mt-1 text-2xl font-bold">{data.totalSales}</p>
          <p className="mt-1 text-xs text-emerald-400">
            {data.totalRevenueCents > 0
              ? `$${(data.totalRevenueCents / 100).toLocaleString()} revenue`
              : "No revenue yet"}
          </p>
        </div>

        <div className="card p-4">
          <p className="text-xs font-medium uppercase tracking-wider text-[rgb(var(--muted))]">
            Rollbacks
          </p>
          <p className="mt-1 text-2xl font-bold text-danger">
            {data.totalRollbacks}
          </p>
          <p className="mt-1 text-xs text-red-400">Refunded transactions</p>
        </div>

        <div className="card p-4">
          <p className="text-xs font-medium uppercase tracking-wider text-[rgb(var(--muted))]">
            Active Sagas
          </p>
          <p className="mt-1 text-2xl font-bold text-warning">
            {data.activeSagas}
          </p>
          <p className="mt-1 text-xs text-yellow-400">In-flight</p>
        </div>

        <div className="card p-4">
          <p className="text-xs font-medium uppercase tracking-wider text-[rgb(var(--muted))]">
            Revenue
          </p>
          <p className="mt-1 text-2xl font-bold">
            ${(data.totalRevenueCents / 100).toLocaleString()}
          </p>
          <p className="mt-1 text-xs text-[rgb(var(--muted))]">
            Total charged (net)
          </p>
        </div>
      </div>

      <div>
        <h3 className="mb-3 text-sm font-semibold">
          Tickets Remaining
        </h3>
        <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
          {tiers.map((tier) => (
            <div key={tier} className="card overflow-hidden">
              <div className={`h-2 bg-gradient-to-r ${TIER_COLORS[tier]}`} />
              <div className="p-4">
                <p className="text-sm font-medium">{TIER_LABELS[tier]}</p>
                <p className="mt-1 text-3xl font-bold">
                  {data.ticketsRemaining[tier]}
                </p>
                <p className="text-xs text-[rgb(var(--muted))]">remaining</p>
              </div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
