"use client";

import { ConnectorCard } from "./ConnectorCard";
import { BookLock, BookOpen } from "lucide-react";
import { GitHub } from "../chat/icons";

const availableConnectors = [
  {
    connectorType: "google_drive",
    displayName: "Google Drive",
    icon: BookLock,
    description: "Access files and folders from your Google Drive.",
    materialIcon: "folder_data",
  },
  {
    connectorType: "github",
    displayName: "GitHub",
    icon: GitHub,
    description: "Access repositories and files from GitHub.",
    materialIcon: "code",
  },
  {
    connectorType: "zotero",
    displayName: "Zotero",
    icon: BookOpen,
    description: "Access your Zotero library and research papers.",
    materialIcon: "book",
  },
];

interface ConnectorListProps {
  apiBaseUrl: string;
  authToken: string | null;
}

export function ConnectorList({ apiBaseUrl, authToken }: ConnectorListProps) {
  return (
    /* Matches connections/code.html layout exactly */
    <div className="w-full bg-kh-black">
      {/* ── Header ── */}
      <div className="mb-10 flex flex-wrap items-end justify-between gap-4 border-b border-kh-border pb-6">
        <div className="flex flex-col gap-2">
          <h2 className="font-display text-4xl font-bold tracking-tight text-white">Data Connections</h2>
          <p className="font-mono text-sm uppercase tracking-widest text-kh-muted">
            Manage external knowledge pipelines
          </p>
        </div>
        <div className="flex items-center gap-3">
          <div className="relative">
            <span className="material-symbols-outlined absolute left-3 top-1/2 -translate-y-1/2 text-[18px] text-kh-muted">
              search
            </span>
            <input
              className="h-10 w-64 border border-kh-border bg-kh-surface pl-10 pr-4 text-sm font-body text-white placeholder-kh-muted outline-none transition-all focus:border-kh-accent"
              placeholder="Filter integrations..."
              type="text"
            />
          </div>
          <button className="flex h-10 items-center gap-2 bg-white px-4 font-display text-sm font-medium text-black transition-colors hover:bg-gray-200">
            <span className="material-symbols-outlined text-[18px]">add</span>
            NEW CONNECTION
          </button>
        </div>
      </div>

      {/* ── Integration Grid ── */}
      <div className="grid grid-cols-1 gap-6 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
        {availableConnectors.map(connector => (
          <ConnectorCard
            key={connector.connectorType}
            connectorType={connector.connectorType}
            displayName={connector.displayName}
            icon={connector.icon}
            apiBaseUrl={apiBaseUrl}
            authToken={authToken}
          />
        ))}

        {/* Notion — disconnected placeholder */}
        <div className="group relative flex h-[160px] max-w-[320px] w-full cursor-pointer flex-col justify-between border border-kh-border bg-kh-surface p-5 opacity-80 transition-colors hover:border-kh-muted hover:opacity-100">
          <div className="flex items-start justify-between">
            <div className="flex h-10 w-10 items-center justify-center border border-kh-border bg-kh-black">
              <span className="material-symbols-outlined text-[24px] text-kh-muted">description</span>
            </div>
            <span className="border border-kh-border bg-kh-black px-2 py-1 font-mono text-[11px] font-medium tracking-wide text-kh-muted">
              DISCONNECTED
            </span>
          </div>
          <div>
            <h3 className="font-display text-[18px] font-semibold leading-tight text-kh-muted transition-colors group-hover:text-white">
              Notion
            </h3>
            <p className="font-body text-[13px] text-kh-muted">Requires workspace auth</p>
          </div>
        </div>

        {/* Confluence — disconnected placeholder */}
        <div className="group relative flex h-[160px] max-w-[320px] w-full cursor-pointer flex-col justify-between border border-kh-border bg-kh-surface p-5 opacity-80 transition-colors hover:border-kh-muted hover:opacity-100">
          <div className="flex items-start justify-between">
            <div className="flex h-10 w-10 items-center justify-center border border-kh-border bg-kh-black">
              <span className="material-symbols-outlined text-[24px] text-kh-muted">book</span>
            </div>
            <span className="border border-kh-border bg-kh-black px-2 py-1 font-mono text-[11px] font-medium tracking-wide text-kh-muted">
              DISCONNECTED
            </span>
          </div>
          <div>
            <h3 className="font-display text-[18px] font-semibold leading-tight text-kh-muted transition-colors group-hover:text-white">
              Confluence
            </h3>
            <p className="font-body text-[13px] text-kh-muted">Provide API credentials</p>
          </div>
        </div>
      </div>
    </div>
  );
}
