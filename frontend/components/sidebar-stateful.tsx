"use client";

import * as React from "react";
import { BaseSidebar } from "@/components/sidebar-base";
import { createSectionNavigation } from "@/lib/navigation-utils";

interface KnowledgeHubSidebarStatefulProps {
  currentSection: string;
  onSectionChange: (section: string) => void;
  showChatView?: boolean;
  onChatViewChange?: (show: boolean) => void;
  activeChatId?: string;
  onChatSelect?: (id: string | undefined) => void;
  showSettingsView?: boolean;
  onSettingsViewChange?: (show: boolean) => void;
  activeSettingsTab?: string;
  onSettingsTabChange?: (tab: string) => void;
}

export function MorphikSidebarRemote({
  currentSection,
  onSectionChange,
  showChatView = false,
  onChatViewChange,
  activeChatId,
  onChatSelect,
  showSettingsView = false,
  onSettingsViewChange,
  activeSettingsTab = "api-keys",
  onSettingsTabChange,
}: KnowledgeHubSidebarStatefulProps) {
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

  const navigation = createSectionNavigation(onSectionChange, handleChatClick, currentSection, handleSettingsClick);

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
