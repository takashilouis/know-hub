"use client";

import React, { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { useMorphik } from "@/contexts/morphik-context";

interface RecentDoc {
  external_id: string;
  filename?: string;
  content_type: string;
  system_metadata: { status?: string; created_at?: string };
  folder_path?: string;
}

interface Stats {
  totalDocs: number;
  totalFolders: number;
  processingDocs: number;
}

function formatTime(dateStr?: string) {
  if (!dateStr) return "";
  const d = new Date(dateStr);
  const now = new Date();
  const diffDays = Math.floor((now.getTime() - d.getTime()) / 86400000);
  if (diffDays === 0) return d.toLocaleTimeString("en-US", { hour: "2-digit", minute: "2-digit" });
  if (diffDays === 1) return "Yesterday";
  return d.toLocaleDateString("en-US", { month: "short", day: "numeric" });
}

function getFileIcon(contentType: string): string {
  if (contentType?.includes("pdf")) return "picture_as_pdf";
  if (contentType?.includes("image")) return "image";
  if (contentType?.includes("video")) return "videocam";
  if (contentType?.includes("csv")) return "table_view";
  if (contentType?.includes("text") || contentType?.includes("markdown")) return "article";
  return "description";
}

function getStatus(doc: RecentDoc): "SYNCED" | "PROCESSING" | "FAILED" {
  const s = doc.system_metadata?.status;
  if (s === "processing") return "PROCESSING";
  if (s === "failed" || s === "error") return "FAILED";
  return "SYNCED";
}

export default function HomePage() {
  const router = useRouter();
  const { apiBaseUrl, authToken } = useMorphik();
  const [searchQuery, setSearchQuery] = useState("");
  const [recentDocs, setRecentDocs] = useState<RecentDoc[]>([]);
  const [stats, setStats] = useState<Stats>({ totalDocs: 0, totalFolders: 0, processingDocs: 0 });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    if (!apiBaseUrl) return;
    const fetchData = async () => {
      try {
        const headers: Record<string, string> = { "Content-Type": "application/json" };
        if (authToken) headers["Authorization"] = `Bearer ${authToken}`;

        const docsRes = await fetch(`${apiBaseUrl}/documents/list_docs`, {
          method: "POST",
          headers,
          body: JSON.stringify({ limit: 6, skip: 0 }),
        });
        if (docsRes.ok) {
          const data = await docsRes.json();
          const documents: RecentDoc[] = data.documents || [];
          setRecentDocs(documents);
          setStats(prev => ({
            ...prev,
            totalDocs: data.total ?? documents.length,
            processingDocs: documents.filter((d: RecentDoc) => d.system_metadata?.status === "processing").length,
          }));
        }

        const foldersRes = await fetch(`${apiBaseUrl}/folders`, { headers });
        if (foldersRes.ok) {
          const folders = await foldersRes.json();
          setStats(prev => ({ ...prev, totalFolders: Array.isArray(folders) ? folders.length : 0 }));
        }
      } catch {
        // Silently ignore — backend may not be connected yet
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, [apiBaseUrl, authToken]);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    if (searchQuery.trim()) {
      router.push(`/search?q=${encodeURIComponent(searchQuery.trim())}`);
    }
  };

  return (
    /* Matches home_dashboard/code.html layout exactly */
    <div className="relative w-full flex-1 overflow-y-auto bg-kh-black">
      {/* Subtle gradient bleed from top */}
      <div className="pointer-events-none absolute inset-x-0 top-0 h-64 bg-gradient-to-b from-kh-accent/5 to-transparent" />

      <div className="relative z-10 mx-auto flex w-full max-w-[1200px] flex-col gap-12 p-8 lg:p-12">

        {/* ── Hero Section ── */}
        <header className="flex flex-col gap-6 pt-4">
          <div className="flex items-center gap-3">
            {/* Vertical accent bar */}
            <div className="h-8 w-1.5 bg-kh-accent" />
            <h1 className="font-display text-4xl font-semibold tracking-[-0.04em] text-white">
              System Intelligence Active
            </h1>
          </div>

          {/* ── Global Search ── */}
          <form onSubmit={handleSearch} className="group relative mt-4">
            <div className="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-6">
              <span className="material-symbols-outlined text-[28px] text-kh-muted transition-colors group-focus-within:text-kh-accent">
                search
              </span>
            </div>
            <input
              className="h-16 w-full border border-kh-border bg-kh-surface pl-[68px] pr-24 text-lg font-body text-white placeholder:text-kh-muted/70 transition-all outline-none focus:border-kh-accent focus:ring-1 focus:ring-kh-accent"
              placeholder="Search the Obsidian Void..."
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
            />
            <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-6">
              <div className="flex items-center gap-1 border border-kh-border bg-kh-black px-2 py-1">
                <span className="font-mono text-xs text-kh-muted">⌘K</span>
              </div>
            </div>
          </form>
        </header>

        {/* ── Vitals Row — 3 Stat Blocks ── */}
        <section className="grid grid-cols-1 gap-6 md:grid-cols-3">
          {/* Stat: Total Docs */}
          <div className="stat-block flex h-[120px] cursor-default flex-col justify-between border border-kh-border bg-kh-surface p-6">
            <div className="flex items-start justify-between">
              <h3 className="font-mono text-xs uppercase tracking-widest text-kh-muted">Total Indexed Files</h3>
              <span className="material-symbols-outlined text-[18px] text-kh-muted/50">description</span>
            </div>
            <div className="flex items-baseline gap-2">
              {loading ? (
                <div className="h-8 w-24 animate-pulse bg-kh-border" />
              ) : (
                <>
                  <span className="font-display text-3xl font-semibold tracking-[-0.02em] text-white">
                    {stats.totalDocs.toLocaleString()}
                  </span>
                  <span className="font-mono text-xs text-kh-accent">+{stats.processingDocs} processing</span>
                </>
              )}
            </div>
          </div>

          {/* Stat: Folders */}
          <div className="stat-block flex h-[120px] cursor-default flex-col justify-between border border-kh-border bg-kh-surface p-6">
            <div className="flex items-start justify-between">
              <h3 className="font-mono text-xs uppercase tracking-widest text-kh-muted">Knowledge Folders</h3>
              <span className="material-symbols-outlined text-[18px] text-kh-muted/50">folder</span>
            </div>
            <div className="flex items-baseline gap-2">
              {loading ? (
                <div className="h-8 w-16 animate-pulse bg-kh-border" />
              ) : (
                <span className="font-display text-3xl font-semibold tracking-[-0.02em] text-white">
                  {stats.totalFolders.toString().padStart(2, "0")}
                </span>
              )}
            </div>
          </div>

          {/* Stat: Active Pipelines */}
          <div className="stat-block flex h-[120px] cursor-default flex-col justify-between border border-kh-border bg-kh-surface p-6">
            <div className="flex items-start justify-between">
              <h3 className="font-mono text-xs uppercase tracking-widest text-kh-muted">Active Pipelines</h3>
              <span className="material-symbols-outlined text-[18px] text-kh-muted/50">account_tree</span>
            </div>
            <div className="flex items-baseline gap-2">
              {loading ? (
                <div className="h-8 w-16 animate-pulse bg-kh-border" />
              ) : (
                <>
                  <span className="font-display text-3xl font-semibold tracking-[-0.02em] text-white">
                    {stats.processingDocs.toString().padStart(2, "0")}
                  </span>
                  <div className="ml-2 mt-2 flex gap-1">
                    {[...Array(3)].map((_, i) => (
                      <div key={i} className="mt-2 h-1.5 w-1.5 bg-kh-accent" />
                    ))}
                    <div className="mt-2 h-1.5 w-1.5 bg-kh-border" />
                  </div>
                </>
              )}
            </div>
          </div>
        </section>

        {/* ── Bottom Split ── */}
        <section className="mt-4 grid grid-cols-1 gap-8 lg:grid-cols-12">

          {/* Command Actions — left (4 cols) */}
          <div className="flex flex-col gap-4 lg:col-span-4">
            <div className="flex items-center justify-between border-b border-kh-border pb-2">
              <h2 className="font-mono text-sm uppercase tracking-widest text-kh-muted">Command Actions</h2>
            </div>
            <div className="mt-2 flex flex-col gap-3">
              <Link href="/documents">
                <button className="group flex w-full items-center justify-between border border-kh-border bg-kh-surface px-4 py-3 text-white transition-colors hover:border-kh-accent">
                  <div className="flex items-center gap-3">
                    <span className="material-symbols-outlined text-[20px] text-kh-muted transition-colors group-hover:text-kh-accent">
                      upload
                    </span>
                    <span className="text-sm font-medium">Ingest Document</span>
                  </div>
                  <span className="font-mono text-[10px] text-kh-muted opacity-0 transition-opacity group-hover:opacity-100">
                    ⌘ U
                  </span>
                </button>
              </Link>
              <button
                className="group flex w-full items-center justify-between border border-kh-border bg-kh-surface px-4 py-3 text-white transition-colors hover:border-kh-accent"
                onClick={() => {
                  if (!apiBaseUrl) return;
                  const headers: Record<string, string> = {};
                  if (authToken) headers["Authorization"] = `Bearer ${authToken}`;
                  fetch(`${apiBaseUrl}/ingest/sync`, { method: "POST", headers }).catch(() => {});
                }}
              >
                <div className="flex items-center gap-3">
                  <span className="material-symbols-outlined text-[20px] text-kh-muted transition-colors group-hover:text-kh-accent">
                    sync
                  </span>
                  <span className="text-sm font-medium">Force DB Sync</span>
                </div>
              </button>
              <Link href="/connections">
                <button className="group flex w-full items-center justify-between border border-kh-border bg-kh-surface px-4 py-3 text-white transition-colors hover:border-kh-accent">
                  <div className="flex items-center gap-3">
                    <span className="material-symbols-outlined text-[20px] text-kh-muted transition-colors group-hover:text-kh-accent">
                      add_link
                    </span>
                    <span className="text-sm font-medium">New Pipeline</span>
                  </div>
                </button>
              </Link>
            </div>
          </div>

          {/* Recent Ingestions — right (8 cols) */}
          <div className="flex flex-col gap-4 lg:col-span-8">
            <div className="flex items-center justify-between border-b border-kh-border pb-2">
              <h2 className="font-mono text-sm uppercase tracking-widest text-kh-muted">Recent Ingestions</h2>
              <Link href="/documents" className="font-mono text-xs text-kh-accent hover:underline">
                View All →
              </Link>
            </div>

            <div className="flex flex-col border border-kh-border bg-kh-black">
              {loading ? (
                [...Array(4)].map((_, i) => (
                  <div key={i} className="flex items-center justify-between border-b border-kh-border px-4 py-3">
                    <div className="h-4 w-48 animate-pulse bg-kh-surface" />
                    <div className="h-4 w-20 animate-pulse bg-kh-surface" />
                  </div>
                ))
              ) : recentDocs.length === 0 ? (
                <div className="flex flex-col items-center justify-center px-4 py-12 text-center">
                  <span className="material-symbols-outlined text-[40px] text-kh-border">folder_open</span>
                  <p className="mt-3 font-mono text-xs text-kh-muted">
                    No documents indexed. Initialize intelligence pipeline.
                  </p>
                  <Link href="/documents">
                    <button className="mt-4 border border-kh-border bg-kh-surface px-4 py-2 font-mono text-xs text-kh-accent transition-colors hover:border-kh-accent">
                      UPLOAD FIRST DOCUMENT
                    </button>
                  </Link>
                </div>
              ) : (
                recentDocs.map((doc, i) => {
                  const status = getStatus(doc);
                  return (
                    <Link key={doc.external_id} href="/documents">
                      <div
                        className={`list-row flex cursor-pointer items-center justify-between px-4 py-3 ${i < recentDocs.length - 1 ? "border-b border-kh-border" : ""}`}
                      >
                        <div className="flex items-center gap-4">
                          <span
                            className={`material-symbols-outlined text-[20px] file-icon ${
                              status === "PROCESSING"
                                ? "animate-pulse text-kh-warning"
                                : status === "FAILED"
                                  ? "text-kh-danger"
                                  : "text-kh-muted"
                            }`}
                          >
                            {status === "FAILED"
                              ? "error"
                              : status === "PROCESSING"
                                ? "donut_large"
                                : getFileIcon(doc.content_type)}
                          </span>
                          <span className="max-w-[200px] truncate text-sm font-medium text-white sm:max-w-xs">
                            {doc.filename || "Untitled"}
                          </span>
                        </div>
                        <div className="flex items-center gap-6">
                          <span
                            className={`font-mono text-[10px] ${
                              status === "SYNCED"
                                ? "border border-kh-success/30 bg-kh-success/10 px-2 py-0.5 text-kh-success"
                                : status === "PROCESSING"
                                  ? "border border-kh-warning/30 bg-kh-warning/10 px-2 py-0.5 text-kh-warning"
                                  : "border border-kh-danger/30 bg-kh-danger/10 px-2 py-0.5 text-kh-danger"
                            }`}
                          >
                            {status}
                          </span>
                          <span className="w-24 text-right font-mono text-xs text-kh-muted">
                            {formatTime(doc.system_metadata?.created_at)}
                          </span>
                        </div>
                      </div>
                    </Link>
                  );
                })
              )}
            </div>
          </div>
        </section>
      </div>
    </div>
  );
}
