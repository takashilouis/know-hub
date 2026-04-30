"use client";

import * as React from "react";
import Link from "next/link";
import { useRouter, usePathname } from "next/navigation";
import { IconArrowLeft, IconPlus } from "@tabler/icons-react";

import { Button } from "@/components/ui/button";
import { ChatSidebar } from "@/components/chat/ChatSidebar";
import { SettingsSidebar } from "@/components/settings/SettingsSidebar";
import { useMorphik } from "@/contexts/morphik-context";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarGroup,
  SidebarGroupContent,
  useSidebar,
} from "@/components/ui/sidebar-components";
import { NavigationStrategy } from "@/lib/navigation-utils";

interface BaseSidebarProps extends React.ComponentProps<typeof Sidebar> {
  showChatView?: boolean;
  onChatViewChange?: (show: boolean) => void;
  activeChatId?: string;
  onChatSelect?: (id: string | undefined) => void;
  showSettingsView?: boolean;
  onSettingsViewChange?: (show: boolean) => void;
  activeSettingsTab?: string;
  onSettingsTabChange?: (tab: string) => void;
  navigation: NavigationStrategy;
}

/* ── Icon map from HTML designs ── */
const NAV_ICONS: Record<string, string> = {
  Home:        "house",
  Knowledge:   "database",
  Search:      "search",
  "Ask AI":    "chat",
  Connections: "link",
  Settings:    "settings",
};

export function BaseSidebar({
  showChatView = false,
  onChatViewChange,
  activeChatId,
  onChatSelect,
  showSettingsView = false,
  onSettingsViewChange,
  activeSettingsTab = "api-keys",
  onSettingsTabChange,
  navigation,
  ...props
}: BaseSidebarProps) {
  const { apiBaseUrl, authToken } = useMorphik();
  const { state, setOpen, toggleSidebar } = useSidebar();
  const router = useRouter();
  const pathname = usePathname() || "/";

  React.useEffect(() => {
    if (showChatView || showSettingsView) setOpen(true);
  }, [showChatView, showSettingsView, setOpen]);

  /* ── Obsidian Void branding mark ── */
  const LogoMark = () => (
    <div className="flex items-center gap-3 px-2">
      <h1 className="font-display text-xl font-bold leading-normal tracking-tight text-white group-data-[collapsible=icon]:hidden">
        KNOW-hub
      </h1>
      {/* Pulsing status dot */}
      <div className="h-1.5 w-1.5 bg-kh-accent group-data-[collapsible=icon]:hidden" style={{ borderRadius: 0 }} />
      {/* Collapsed icon */}
      <span className="material-symbols-outlined hidden text-kh-accent text-[20px] group-data-[collapsible=icon]:block">
        database
      </span>
    </div>
  );

  /* ── Material icon for a nav item title ── */
  const getIcon = (title: string) => NAV_ICONS[title] ?? "circle";

  /* ── Nav item active check ── */
  const isNavActive = (item: { title: string; url?: string }) => {
    if ("url" in item && item.url) {
      return item.url === "/" ? pathname === "/" : pathname.startsWith(item.url as string);
    }
    return false;
  };

  return (
    <Sidebar collapsible="icon" {...props}>
      {/* ── Brand header ── */}
      <SidebarHeader className="border-b border-kh-border py-5 bg-kh-black">
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton asChild size="lg">
              <Link href="/" className="flex items-center">
                <LogoMark />
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>

      <SidebarContent className="relative flex flex-col bg-kh-black">
        {showChatView ? (
          <div className="min-h-0 flex-1">
            <SidebarGroup>
              <SidebarGroupContent className="px-2 py-1">
                {state === "collapsed" ? (
                  <div className="flex w-full items-center justify-center">
                    <Button variant="ghost" size="icon" onClick={() => onChatViewChange?.(false)} title="Back">
                      <IconArrowLeft className="h-4 w-4" />
                    </Button>
                  </div>
                ) : (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="w-full justify-start gap-2 text-sm text-kh-muted hover:text-white"
                    onClick={() => onChatViewChange?.(false)}
                  >
                    <IconArrowLeft className="h-4 w-4" />
                    Back to Menu
                  </Button>
                )}
              </SidebarGroupContent>
            </SidebarGroup>
            {state === "collapsed" && (
              <SidebarGroup>
                <SidebarGroupContent className="flex items-center justify-center px-2 py-1">
                  <Button variant="ghost" size="icon" title="New chat" onClick={() => onChatSelect?.(undefined)}>
                    <IconPlus className="h-4 w-4" />
                  </Button>
                </SidebarGroupContent>
              </SidebarGroup>
            )}
            <div className="h-full [&>div>div:first-child]:!px-2 [&>div>div:first-child]:!py-1 [&>div>div:last-child]:!px-2 [&>div]:!w-full [&>div]:!border-r-0 [&>div]:!bg-transparent [&_button]:!px-2 [&_button]:!py-1.5 [&_li>div]:!px-1 [&_ul]:!p-2">
              <ChatSidebar
                apiBaseUrl={apiBaseUrl}
                authToken={authToken}
                activeChatId={activeChatId}
                onSelect={chatId => onChatSelect?.(chatId)}
                collapsed={state === "collapsed"}
                onToggle={toggleSidebar}
              />
            </div>
          </div>
        ) : showSettingsView ? (
          <div className="min-h-0 flex-1">
            <SidebarGroup>
              <SidebarGroupContent className="px-2 py-1">
                {state === "collapsed" ? (
                  <div className="flex w-full items-center justify-center">
                    <Button variant="ghost" size="icon" onClick={() => onSettingsViewChange?.(false)} title="Back">
                      <IconArrowLeft className="h-4 w-4" />
                    </Button>
                  </div>
                ) : (
                  <Button
                    variant="ghost"
                    size="sm"
                    className="w-full justify-start gap-2 text-sm text-kh-muted hover:text-white"
                    onClick={() => onSettingsViewChange?.(false)}
                  >
                    <IconArrowLeft className="h-4 w-4" />
                    Back to Menu
                  </Button>
                )}
              </SidebarGroupContent>
            </SidebarGroup>
            <div className="h-full [&>div>div:first-child]:!px-2 [&>div>div:first-child]:!py-1 [&>div>div:last-child]:!px-2 [&>div]:!w-full [&>div]:!border-r-0 [&>div]:!bg-transparent [&_button]:!px-2 [&_button]:!py-1.5 [&_li>div]:!px-1 [&_ul]:!p-2">
              <SettingsSidebar
                activeTab={activeSettingsTab}
                onTabChange={tab => onSettingsTabChange?.(tab)}
                collapsed={state === "collapsed"}
                onToggle={toggleSidebar}
              />
            </div>
          </div>
        ) : (
          <>
            {/* ── Main navigation — matches HTML exactly ── */}
            <SidebarGroup className="pt-4">
              <SidebarGroupContent className="flex flex-col gap-1 px-0">
                {navigation.mainItems.map(item => {
                  const icon = getIcon(item.title);
                  const active = isNavActive(item as { title: string; url?: string });

                  const navClass = active
                    ? "flex items-center gap-3 px-3 py-2 bg-kh-surface text-kh-accent border border-kh-border w-full"
                    : "flex items-center gap-3 px-3 py-2 text-kh-muted hover:text-white hover:bg-kh-surface transition-colors w-full";

                  const activeStyle = active ? { borderLeft: "2px solid #12d393" } : {};

                  return (
                    <div key={item.title}>
                      {navigation.type === "url" && "url" in item ? (
                        item.isSpecial && item.title === "Ask AI" ? (
                          <button
                            className={navClass}
                            style={activeStyle}
                            onClick={() => {
                              if (pathname !== "/chat") router.push("/chat");
                              setOpen(true);
                              onChatViewChange?.(true);
                            }}
                          >
                            <span
                              className="material-symbols-outlined text-[20px]"
                              style={active ? { fontVariationSettings: "'FILL' 1" } : {}}
                            >
                              {icon}
                            </span>
                            <p className="text-sm font-body leading-normal group-data-[collapsible=icon]:hidden">
                              {item.title}
                            </p>
                          </button>
                        ) : item.isSpecial && item.title === "Settings" ? (
                          <button
                            className={navClass}
                            style={activeStyle}
                            onClick={() => {
                              if (pathname !== "/settings") router.push("/settings");
                              setOpen(true);
                              onSettingsViewChange?.(true);
                            }}
                          >
                            <span
                              className="material-symbols-outlined text-[20px]"
                              style={active ? { fontVariationSettings: "'FILL' 1" } : {}}
                            >
                              {icon}
                            </span>
                            <p className="text-sm font-body leading-normal group-data-[collapsible=icon]:hidden">
                              {item.title}
                            </p>
                          </button>
                        ) : (
                          <Link href={item.url} className={navClass} style={activeStyle}>
                            <span
                              className="material-symbols-outlined text-[20px]"
                              style={active ? { fontVariationSettings: "'FILL' 1" } : {}}
                            >
                              {icon}
                            </span>
                            <p className="text-sm font-body leading-normal group-data-[collapsible=icon]:hidden">
                              {item.title}
                            </p>
                          </Link>
                        )
                      ) : (
                        <button
                          className={navClass}
                          style={activeStyle}
                          onClick={() => navigation.onItemClick(item)}
                        >
                          <span className="material-symbols-outlined text-[20px]">{icon}</span>
                          <p className="text-sm font-body leading-normal group-data-[collapsible=icon]:hidden">
                            {item.title}
                          </p>
                        </button>
                      )}
                    </div>
                  );
                })}
              </SidebarGroupContent>
            </SidebarGroup>

            {/* ── Secondary nav pinned to bottom ── */}
            <div className="mt-auto">
              <SidebarGroup>
                <SidebarGroupContent className="flex flex-col gap-1 px-0">
                  {navigation.secondaryItems.map(item => {
                    const icon = getIcon(item.title);
                    const active = isNavActive(item as { title: string; url?: string });
                    const navClass = active
                      ? "flex items-center gap-3 px-3 py-2 bg-kh-surface text-kh-accent border border-kh-border w-full"
                      : "flex items-center gap-3 px-3 py-2 text-kh-muted hover:text-white hover:bg-kh-surface transition-colors w-full";
                    const activeStyle = active ? { borderLeft: "2px solid #12d393" } : {};

                    return (
                      <div key={item.title}>
                        {navigation.type === "url" && "url" in item ? (
                          item.isSpecial && item.title === "Settings" ? (
                            <button
                              className={navClass}
                              style={activeStyle}
                              onClick={() => {
                                if (pathname !== "/settings") router.push("/settings");
                                setOpen(true);
                                onSettingsViewChange?.(true);
                              }}
                            >
                              <span className="material-symbols-outlined text-[20px]">{icon}</span>
                              <p className="text-sm font-body leading-normal group-data-[collapsible=icon]:hidden">
                                {item.title}
                              </p>
                            </button>
                          ) : (
                            <Link href={item.url} className={navClass} style={activeStyle}>
                              <span className="material-symbols-outlined text-[20px]">{icon}</span>
                              <p className="text-sm font-body leading-normal group-data-[collapsible=icon]:hidden">
                                {item.title}
                              </p>
                            </Link>
                          )
                        ) : (
                          <button className={navClass} style={activeStyle} onClick={() => navigation.onItemClick(item)}>
                            <span className="material-symbols-outlined text-[20px]">{icon}</span>
                            <p className="text-sm font-body leading-normal group-data-[collapsible=icon]:hidden">
                              {item.title}
                            </p>
                          </button>
                        )}
                      </div>
                    );
                  })}
                </SidebarGroupContent>
              </SidebarGroup>
            </div>
          </>
        )}
      </SidebarContent>

      {/* ── Footer ── */}
      <SidebarFooter className="border-t border-kh-border bg-kh-black">
        <div className="flex items-center justify-between px-3 py-2 group-data-[collapsible=icon]:hidden">
          <div className="flex items-center gap-3">
            <div className="flex h-8 w-8 items-center justify-center border border-kh-border bg-kh-surface">
              <span className="font-mono text-xs text-kh-muted">OP</span>
            </div>
            <div className="flex flex-col">
              <span className="font-mono text-xs text-kh-text">Admin</span>
              <span className="font-mono text-[10px] uppercase tracking-tighter text-kh-muted">System Active</span>
            </div>
          </div>
          <button
            className="text-kh-muted hover:text-white transition-colors"
            onClick={toggleSidebar}
            title="Collapse sidebar"
          >
            <span className="material-symbols-outlined text-xl">keyboard_double_arrow_left</span>
          </button>
        </div>
      </SidebarFooter>
    </Sidebar>
  );
}
