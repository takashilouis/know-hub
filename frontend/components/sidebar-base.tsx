"use client";

import * as React from "react";
import Link from "next/link";
import { useRouter, usePathname } from "next/navigation";
import { IconArrowLeft, IconPlus, IconBook } from "@tabler/icons-react";

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

  const LogoMark = () => (
    <div className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-blue-500 text-white">
      <IconBook className="h-4 w-4" />
    </div>
  );

  return (
    <Sidebar collapsible="icon" {...props}>
      <SidebarHeader>
        <SidebarMenu>
          <SidebarMenuItem>
            <SidebarMenuButton asChild size="lg">
              <Link href="/" className="flex items-center gap-2 group-data-[collapsible=icon]:justify-center">
                <LogoMark />
                <span className="text-sm font-semibold tracking-tight text-sidebar-foreground group-data-[collapsible=icon]:hidden">
                  Knowledge Hub
                </span>
              </Link>
            </SidebarMenuButton>
          </SidebarMenuItem>
        </SidebarMenu>
      </SidebarHeader>

      <SidebarContent className="relative flex flex-col">
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
                    className="w-full justify-start gap-2 text-sm"
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
                    className="w-full justify-start gap-2 text-sm"
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
            {/* Main navigation */}
            <SidebarGroup>
              <SidebarGroupContent className="flex flex-col gap-1">
                <SidebarMenu>
                  {navigation.mainItems.map(item => (
                    <SidebarMenuItem key={item.title}>
                      {navigation.type === "url" && "url" in item ? (
                        item.isSpecial && item.title === "Ask AI" ? (
                          <SidebarMenuButton
                            tooltip={item.title}
                            onClick={() => {
                              if (pathname !== "/chat") router.push("/chat");
                              setOpen(true);
                              onChatViewChange?.(true);
                            }}
                            isActive={pathname === "/chat"}
                          >
                            {item.icon && <item.icon />}
                            <span>{item.title}</span>
                          </SidebarMenuButton>
                        ) : item.isSpecial && item.title === "Settings" ? (
                          <SidebarMenuButton
                            tooltip={item.title}
                            onClick={() => {
                              if (pathname !== "/settings") router.push("/settings");
                              setOpen(true);
                              onSettingsViewChange?.(true);
                            }}
                            isActive={pathname === "/settings"}
                          >
                            {item.icon && <item.icon />}
                            <span>{item.title}</span>
                          </SidebarMenuButton>
                        ) : (
                          <SidebarMenuButton
                            tooltip={item.title}
                            asChild
                            isActive={item.url === "/" ? pathname === "/" : pathname.startsWith(item.url)}
                          >
                            <Link href={item.url}>
                              {item.icon && <item.icon />}
                              <span>{item.title}</span>
                            </Link>
                          </SidebarMenuButton>
                        )
                      ) : (
                        <SidebarMenuButton
                          tooltip={item.title}
                          onClick={() => navigation.onItemClick(item)}
                          isActive={
                            navigation.type === "section" && "section" in item
                              ? navigation.currentActive === item.section
                              : false
                          }
                        >
                          {item.icon && <item.icon />}
                          <span>{item.title}</span>
                        </SidebarMenuButton>
                      )}
                    </SidebarMenuItem>
                  ))}
                </SidebarMenu>
              </SidebarGroupContent>
            </SidebarGroup>

            {/* Secondary nav pinned to bottom */}
            <div className="mt-auto">
              <SidebarGroup>
                <SidebarGroupContent className="flex flex-col gap-1">
                  <SidebarMenu>
                    {navigation.secondaryItems.map(item => (
                      <SidebarMenuItem key={item.title}>
                        {navigation.type === "url" && "url" in item ? (
                          item.isSpecial && item.title === "Settings" ? (
                            <SidebarMenuButton
                              tooltip={item.title}
                              onClick={() => {
                                if (pathname !== "/settings") router.push("/settings");
                                setOpen(true);
                                onSettingsViewChange?.(true);
                              }}
                              isActive={pathname === "/settings"}
                            >
                              {item.icon && <item.icon />}
                              <span>{item.title}</span>
                            </SidebarMenuButton>
                          ) : (
                            <SidebarMenuButton tooltip={item.title} asChild>
                              <Link href={item.url}>
                                {item.icon && <item.icon />}
                                <span>{item.title}</span>
                              </Link>
                            </SidebarMenuButton>
                          )
                        ) : (
                          <SidebarMenuButton tooltip={item.title} onClick={() => navigation.onItemClick(item)}>
                            {item.icon && <item.icon />}
                            <span>{item.title}</span>
                          </SidebarMenuButton>
                        )}
                      </SidebarMenuItem>
                    ))}
                  </SidebarMenu>
                </SidebarGroupContent>
              </SidebarGroup>
            </div>
          </>
        )}
      </SidebarContent>

      <SidebarFooter>
        <div className="px-2 py-2 group-data-[collapsible=icon]:hidden">
          <p className="text-[11px] text-sidebar-foreground/40">Internal Knowledge Hub</p>
        </div>
      </SidebarFooter>
    </Sidebar>
  );
}
