"use client";

import AdminTelemetry from "@/components/AdminTelemetry";

export default function AdminPage() {
  return (
    <div className="space-y-8 animate-fade-in">
      <div>
        <h2 className="text-2xl font-bold tracking-tight">Admin Dashboard</h2>
        <p className="mt-1 text-sm text-[rgb(var(--muted))]">
          Real-time flash sale metrics, reservation status, and rollback monitoring.
        </p>
      </div>

      <AdminTelemetry />
    </div>
  );
}
