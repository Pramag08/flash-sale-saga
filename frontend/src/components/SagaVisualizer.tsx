"use client";

import { useEffect, useRef, useState } from "react";
import { getSagaStatus } from "@/lib/api";
import type { SagaStep, SagaStatusResponse } from "@/lib/types";

const STEPS: { step: SagaStep; label: string }[] = [
  { step: "INITIATED", label: "Saga Initiated" },
  { step: "PAYMENT_PROCESSED", label: "Payment Processed" },
  { step: "INVENTORY_RESERVED", label: "Inventory Reserved" },
  { step: "SUCCESS", label: "Success" },
];

const FAIL_STEPS: { step: SagaStep; label: string }[] = [
  { step: "ROLLED_BACK", label: "Rolled Back" },
  { step: "TIMED_OUT", label: "Timed Out" },
  { step: "FAILED_PERMANENTLY", label: "Failed Permanently" },
];

const STEP_ORDER: Record<SagaStep, number> = {
  INITIATED: 0,
  PAYMENT_PROCESSED: 1,
  INVENTORY_RESERVED: 2,
  SUCCESS: 3,
  ROLLED_BACK: -1,
  TIMED_OUT: -1,
  FAILED_PERMANENTLY: -1,
};

function statusToStep(
  pollCount: number,
  statusResp: SagaStatusResponse | null
): SagaStep {
  const backendStatus = statusResp?.status ?? "";

  if (backendStatus === "CANCELLED" || backendStatus === "ROLLED_BACK") {
    return "ROLLED_BACK";
  }
  if (backendStatus === "TIMED_OUT") return "TIMED_OUT";
  if (backendStatus === "FAILED_PERMANENTLY") return "FAILED_PERMANENTLY";
  if (backendStatus === "CONFIRMED") return "SUCCESS";
  if (backendStatus === "RESERVED") return "INVENTORY_RESERVED";

  if (pollCount >= 5) return "INVENTORY_RESERVED";
  if (pollCount >= 3) return "PAYMENT_PROCESSED";

  return "INITIATED";
}

interface Props {
  transactionId: string;
}

const INITIAL_DELAY_MS = 500;
const MAX_DELAY_MS = 8000;
const BACKOFF_FACTOR = 2;
const MAX_POLLS = 30;

export default function SagaVisualizer({ transactionId }: Props) {
  const [step, setStep] = useState<SagaStep>("INITIATED");
  const [pollCount, setPollCount] = useState(0);
  const [statusData, setStatusData] = useState<SagaStatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const timeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const cancelledRef = useRef(false);
  const pollCountRef = useRef(0);

  useEffect(() => {
    cancelledRef.current = false;
    pollCountRef.current = 0;

    async function poll(delay: number) {
      if (cancelledRef.current) return;

      try {
        const data = await getSagaStatus(transactionId);
        if (cancelledRef.current) return;

        pollCountRef.current += 1;
        const count = pollCountRef.current;
        setPollCount(count);

        if (count >= MAX_POLLS) {
          setStep("TIMED_OUT");
          return;
        }

        setStep(statusToStep(count, data));
        if (data) setStatusData(data);
        setError(null);

        if (
          data &&
          (data.status === "CONFIRMED" ||
            data.status === "CANCELLED" ||
            data.status === "TIMED_OUT")
        ) {
          return;
        }

        const nextDelay = Math.min(delay * BACKOFF_FACTOR, MAX_DELAY_MS);
        timeoutRef.current = setTimeout(() => poll(nextDelay), nextDelay);
      } catch (e) {
        if (!cancelledRef.current) {
          pollCountRef.current += 1;
          const count = pollCountRef.current;
          setPollCount(count);

          if (count >= MAX_POLLS) {
            setStep("TIMED_OUT");
            return;
          }

          setError(e instanceof Error ? e.message : "Polling failed");
          const nextDelay = Math.min(delay * BACKOFF_FACTOR, MAX_DELAY_MS);
          timeoutRef.current = setTimeout(() => poll(nextDelay), nextDelay);
        }
      }
    }

    timeoutRef.current = setTimeout(() => poll(INITIAL_DELAY_MS), INITIAL_DELAY_MS);

    return () => {
      cancelledRef.current = true;
      if (timeoutRef.current) clearTimeout(timeoutRef.current);
    };
  }, [transactionId]);

  const isFail = step === "ROLLED_BACK" || step === "TIMED_OUT" || step === "FAILED_PERMANENTLY";

  const currentStepIndex = isFail ? -1 : STEP_ORDER[step];

  return (
    <div className="card p-6 animate-fade-in">
      <div className="mb-4 flex items-center justify-between">
        <h3 className="text-sm font-semibold uppercase tracking-wider text-[rgb(var(--muted))]">
          Saga Progress
        </h3>
        <code className="rounded bg-[rgb(var(--bg))] px-2 py-0.5 text-xs text-[rgb(var(--muted))]">
          {transactionId.slice(0, 8)}&hellip;
        </code>
      </div>

      {isFail ? (
        <div className="mb-6 rounded-lg border border-red-500/30 bg-red-500/10 px-4 py-3">
          <span className="text-sm font-semibold text-danger">
            Wallet Refunded &mdash; Your payment has been reversed.
          </span>
          {step === "ROLLED_BACK" && (
            <p className="mt-1 text-xs text-red-400">
              Not enough inventory was available. Your funds have been returned.
            </p>
          )}
          {step === "TIMED_OUT" && (
            <p className="mt-1 text-xs text-red-400">
              The reservation timed out. Your funds have been returned.
            </p>
          )}
          {step === "FAILED_PERMANENTLY" && (
            <p className="mt-1 text-xs text-red-400">
              The saga encountered a permanent error. Your funds have been returned.
            </p>
          )}
        </div>
      ) : (
        <div className="mb-6 space-y-0">
          {STEPS.map((s, i) => {
            const active = i <= currentStepIndex;
            const current = i === currentStepIndex;
            return (
              <div key={s.step} className="relative flex items-center gap-4 py-2">
                {i < STEPS.length - 1 && (
                  <div
                    className={`absolute left-[11px] top-8 h-full w-0.5 transition-colors duration-500 ${
                      active && i < currentStepIndex
                        ? "bg-emerald-500"
                        : "bg-[rgb(var(--border))]"
                    }`}
                  />
                )}
                <div
                  className={`relative z-10 flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-xs font-bold transition-all duration-500 ${
                    active
                      ? "bg-emerald-500 text-white shadow-lg shadow-emerald-500/30"
                      : "bg-[rgb(var(--border))] text-[rgb(var(--muted))]"
                  } ${current ? "animate-pulse-fast" : ""}`}
                >
                  {active && !current ? "✓" : i + 1}
                </div>
                <span
                  className={`text-sm transition-colors duration-500 ${
                    active
                      ? "font-medium text-[rgb(var(--fg))]"
                      : "text-[rgb(var(--muted))]"
                  } ${current ? "animate-pulse-fast" : ""}`}
                >
                  {s.label}
                </span>
              </div>
            );
          })}
        </div>
      )}

      {error && (
        <p className="text-xs text-warning">Polling error: {error}</p>
      )}

      {statusData && !isFail && (
        <div className="mt-4 border-t border-[rgb(var(--border))] pt-3 text-xs text-[rgb(var(--muted))]">
          {statusData.tier_name && (
            <span className="mr-4">Tier: {statusData.tier_name}</span>
          )}
          {statusData.quantity && (
            <span>Qty: {statusData.quantity}</span>
          )}
        </div>
      )}
    </div>
  );
}
