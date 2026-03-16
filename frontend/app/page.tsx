"use client";

import React, { useEffect, useState } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import {
  IconSearch,
  IconMessage,
  IconUpload,
  IconFiles,
  IconFolder,
  IconChevronRight,
  IconBook,
  IconClock,
} from "@tabler/icons-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
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

        // Fetch recent documents
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

        // Fetch folder count
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

  const formatDate = (dateStr?: string) => {
    if (!dateStr) return "";
    return new Date(dateStr).toLocaleDateString("en-US", { month: "short", day: "numeric" });
  };

  const getFileIcon = (contentType: string) => {
    if (contentType?.includes("pdf")) return "📄";
    if (contentType?.includes("image")) return "🖼️";
    if (contentType?.includes("video")) return "🎥";
    if (contentType?.includes("text") || contentType?.includes("markdown")) return "📝";
    return "📎";
  };

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-8 py-4">
      {/* Hero */}
      <div className="flex flex-col items-center gap-4 pt-6 text-center">
        <div className="flex h-14 w-14 items-center justify-center rounded-2xl bg-primary/10 text-primary">
          <IconBook className="h-7 w-7" />
        </div>
        <div>
          <h1 className="text-3xl font-bold tracking-tight text-foreground">Knowledge Hub</h1>
          <p className="mt-1.5 text-muted-foreground">
            Search, explore, and ask questions across your organization&apos;s documents
          </p>
        </div>

        {/* Search bar */}
        <form onSubmit={handleSearch} className="mt-2 flex w-full max-w-2xl gap-2">
          <div className="relative flex-1">
            <IconSearch className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              className="h-11 pl-10 pr-4 text-base shadow-sm"
              placeholder="Search across all documents..."
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
            />
          </div>
          <Button type="submit" size="lg" className="h-11 px-6">
            Search
          </Button>
        </form>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-3 gap-4">
        {[
          { label: "Documents", value: loading ? "—" : stats.totalDocs, icon: IconFiles, color: "text-blue-600" },
          { label: "Folders", value: loading ? "—" : stats.totalFolders, icon: IconFolder, color: "text-emerald-600" },
          {
            label: "Processing",
            value: loading ? "—" : stats.processingDocs,
            icon: IconClock,
            color: "text-amber-500",
          },
        ].map(({ label, value, icon: Icon, color }) => (
          <div key={label} className="rounded-xl border bg-card p-5 shadow-sm">
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">{label}</span>
              <Icon className={`h-4 w-4 ${color}`} />
            </div>
            <p className="mt-2 text-3xl font-semibold tracking-tight">{value}</p>
          </div>
        ))}
      </div>

      {/* Quick Actions */}
      <div>
        <h2 className="mb-3 text-sm font-medium uppercase tracking-wider text-muted-foreground">Quick Actions</h2>
        <div className="grid grid-cols-3 gap-3">
          <Link href="/documents">
            <div className="group flex cursor-pointer flex-col gap-2 rounded-xl border bg-card p-5 shadow-sm transition-all hover:border-primary/50 hover:shadow-md">
              <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-blue-50 text-blue-600 group-hover:bg-blue-100">
                <IconUpload className="h-5 w-5" />
              </div>
              <div>
                <p className="text-sm font-medium">Upload Documents</p>
                <p className="mt-0.5 text-xs text-muted-foreground">Add PDFs, specs, or code files</p>
              </div>
            </div>
          </Link>
          <Link href="/search">
            <div className="group flex cursor-pointer flex-col gap-2 rounded-xl border bg-card p-5 shadow-sm transition-all hover:border-primary/50 hover:shadow-md">
              <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-emerald-50 text-emerald-600 group-hover:bg-emerald-100">
                <IconSearch className="h-5 w-5" />
              </div>
              <div>
                <p className="text-sm font-medium">Semantic Search</p>
                <p className="mt-0.5 text-xs text-muted-foreground">Find relevant content by meaning</p>
              </div>
            </div>
          </Link>
          <Link href="/chat">
            <div className="group flex cursor-pointer flex-col gap-2 rounded-xl border bg-card p-5 shadow-sm transition-all hover:border-primary/50 hover:shadow-md">
              <div className="flex h-9 w-9 items-center justify-center rounded-lg bg-violet-50 text-violet-600 group-hover:bg-violet-100">
                <IconMessage className="h-5 w-5" />
              </div>
              <div>
                <p className="text-sm font-medium">Ask AI</p>
                <p className="mt-0.5 text-xs text-muted-foreground">Chat with your knowledge base</p>
              </div>
            </div>
          </Link>
        </div>
      </div>

      {/* Recent Documents */}
      <div>
        <div className="mb-3 flex items-center justify-between">
          <h2 className="text-sm font-medium uppercase tracking-wider text-muted-foreground">Recent Documents</h2>
          <Link href="/documents" className="flex items-center gap-1 text-xs text-primary hover:underline">
            View all <IconChevronRight className="h-3 w-3" />
          </Link>
        </div>

        {loading ? (
          <div className="grid grid-cols-2 gap-3">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="h-16 animate-pulse rounded-xl border bg-muted" />
            ))}
          </div>
        ) : recentDocs.length === 0 ? (
          <div className="rounded-xl border border-dashed bg-card p-10 text-center">
            <IconFiles className="mx-auto h-8 w-8 text-muted-foreground/50" />
            <p className="mt-2 text-sm text-muted-foreground">No documents yet</p>
            <Link href="/documents">
              <Button variant="outline" size="sm" className="mt-3">
                Upload your first document
              </Button>
            </Link>
          </div>
        ) : (
          <div className="grid grid-cols-2 gap-3">
            {recentDocs.map(doc => (
              <Link key={doc.external_id} href="/documents">
                <div className="group flex cursor-pointer items-start gap-3 rounded-xl border bg-card p-4 shadow-sm transition-all hover:border-primary/50 hover:shadow-md">
                  <span className="text-xl">{getFileIcon(doc.content_type)}</span>
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-medium">{doc.filename || "Untitled"}</p>
                    <div className="mt-0.5 flex items-center gap-2">
                      {doc.folder_path && (
                        <span className="flex items-center gap-0.5 text-[11px] text-muted-foreground">
                          <IconFolder className="h-3 w-3" />
                          {doc.folder_path.split("/").filter(Boolean).pop()}
                        </span>
                      )}
                      <span className="text-[11px] text-muted-foreground">
                        {formatDate(doc.system_metadata?.created_at)}
                      </span>
                      {doc.system_metadata?.status === "processing" && (
                        <span className="rounded-full bg-amber-100 px-1.5 py-0.5 text-[10px] font-medium text-amber-700">
                          Processing
                        </span>
                      )}
                    </div>
                  </div>
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
