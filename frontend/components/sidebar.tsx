"use client";

import * as React from "react";
import { BaseSidebar } from "@/components/sidebar-base";
import { createUrlNavigation } from "@/lib/navigation-utils";

interface MorphikSidebarProps {
  showChatView?: boolean;
  onChatViewChange?: (show: boolean) => void;
  activeChatId?: string;
  onChatSelect?: (id: string | undefined) => void;
  showSettingsView?: boolean;
  onSettingsViewChange?: (show: boolean) => void;
  activeSettingsTab?: string;
  onSettingsTabChange?: (tab: string) => void;
}

export function MorphikSidebarLocal({
  showChatView = false,
  onChatViewChange,
  activeChatId,
  onChatSelect,
  showSettingsView = false,
  onSettingsViewChange,
  activeSettingsTab = "api-keys",
  onSettingsTabChange,
}: MorphikSidebarProps) {
  const handleChatClick = React.useCallback(() => {
    if (typeof window !== "undefined") {
      sessionStorage.removeItem("chatViewManuallyHidden");
    }
    onChatViewChange?.(true);
  }, [onChatViewChange]);

  const handleSettingsClick = React.useCallback(() => {
    if (typeof window !== "undefined") {
      sessionStorage.removeItem("settingsViewManuallyHidden");
    }
    onSettingsViewChange?.(true);
  }, [onSettingsViewChange]);

  const navigation = createUrlNavigation(handleChatClick, handleSettingsClick);

  return (
    <BaseSidebar
      showChatView={showChatView}
      onChatViewChange={onChatViewChange}
      activeChatId={activeChatId}
      onChatSelect={onChatSelect}
      showSettingsView={showSettingsView}
      onSettingsViewChange={onSettingsViewChange}
      activeSettingsTab={activeSettingsTab}
      onSettingsTabChange={onSettingsTabChange}
      navigation={navigation}
      collapsible="icon"
    />
  );
}

// Backward-compatible alias for existing imports
export const MorphikSidebar = MorphikSidebarLocal;
