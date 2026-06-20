import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Flash Sale Saga — Live Purchase Dashboard",
  description:
    "High-concurrency flash sale ticket purchase with real-time saga status visualization and admin telemetry.",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark" suppressHydrationWarning>
      <body className="min-h-screen antialiased" suppressHydrationWarning>
        <header className="border-b border-[rgb(var(--border))] px-6 py-4">
          <div className="mx-auto flex max-w-6xl items-center justify-between">
            <h1 className="text-xl font-bold tracking-tight">
              Flash Sale Saga
            </h1>
            <nav className="flex gap-6 text-sm text-[rgb(var(--muted))]">
              <a href="/" className="hover:text-[rgb(var(--fg))] transition-colors">
                Buy Tickets
              </a>
              <a href="/admin" className="hover:text-[rgb(var(--fg))] transition-colors">
                Admin
              </a>
            </nav>
          </div>
        </header>
        <main className="mx-auto max-w-6xl px-6 py-8">{children}</main>
      </body>
    </html>
  );
}
