"use client";

import { usePathname } from "next/navigation";
import { Separator } from "@/components/ui/separator";
import { SidebarTrigger } from "@/components/ui/sidebar-components";
import { ChevronRight } from "lucide-react";
import Link from "next/link";
import { useHeader } from "@/contexts/header-context";
import { Breadcrumb } from "@/components/types";

export interface DynamicSiteHeaderProps {
  customBreadcrumbs?: Breadcrumb[];
  rightContent?: React.ReactNode;
  userProfile?: {
    name?: string;
    email?: string;
    avatar?: string;
    tier?: string;
  };
}

export function DynamicSiteHeader({
  customBreadcrumbs: propBreadcrumbs,
  rightContent: propRightContent,
}: DynamicSiteHeaderProps = {}) {
  const pathname = usePathname();
  const { customBreadcrumbs: contextBreadcrumbs, rightContent: contextRightContent } = useHeader();

  const breadcrumbs = contextBreadcrumbs || propBreadcrumbs || generateBreadcrumbs(pathname || "");
  const rightContent = contextRightContent || propRightContent;

  return (
    <header className="group-has-data-[collapsible=icon]/sidebar-wrapper:h-[var(--header-height)] flex h-[var(--header-height)] shrink-0 items-center gap-2 border-b border-kh-border bg-kh-black transition-[width,height] ease-linear">
      <div className="flex w-full items-center gap-1 px-4 lg:gap-2 lg:px-6">
        <SidebarTrigger className="-ml-1 text-kh-muted hover:text-white" />
        <Separator orientation="vertical" className="mx-2 bg-kh-border data-[orientation=vertical]:h-4" />

        {/* Breadcrumbs — JetBrains Mono style */}
        <nav className="flex items-center space-x-1 text-sm">
          {breadcrumbs.map((crumb, index) => {
            const isLast = index === breadcrumbs.length - 1;
            const isCurrent = crumb.current || isLast;

            return (
              <div key={index} className="flex items-center">
                {index > 0 && <ChevronRight className="mx-1 h-3 w-3 text-kh-border" />}
                {!isCurrent && (crumb.href || crumb.onClick) ? (
                  crumb.onClick ? (
                    <button
                      onClick={crumb.onClick}
                      className="font-mono text-xs uppercase tracking-wider text-kh-muted transition-colors hover:text-white"
                    >
                      {crumb.label}
                    </button>
                  ) : (
                    <Link
                      href={crumb.href!}
                      className="font-mono text-xs uppercase tracking-wider text-kh-muted transition-colors hover:text-white"
                    >
                      {crumb.label}
                    </Link>
                  )
                ) : (
                  <span className="font-mono text-xs font-medium uppercase tracking-wider text-kh-text">
                    {crumb.label}
                  </span>
                )}
              </div>
            );
          })}
        </nav>

        {/* Right side actions */}
        <div className="ml-auto flex items-center gap-2">{rightContent}</div>
      </div>
    </header>
  );
}

function generateBreadcrumbs(pathname: string): Breadcrumb[] {
  const segments = pathname.split("/").filter(Boolean);
  if (segments.length === 0) return [{ label: "Home" }];
  const breadcrumbs: Breadcrumb[] = [{ label: "Home", href: "/" }];
  breadcrumbs.push({ label: getSectionLabel(segments[0]) });
  return breadcrumbs;
}

function getSectionLabel(section: string): string {
  const labels: Record<string, string> = {
    documents:   "Knowledge Base",
    search:      "Semantic Search",
    chat:        "Ask AI",
    connections: "Connections",
    settings:    "Settings",
    logs:        "Logs",
    pdf:         "PDF Viewer",
  };
  return labels[section] || section.charAt(0).toUpperCase() + section.slice(1);
}
