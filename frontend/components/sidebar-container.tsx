"use client";

import React from "react";
import { MorphikSidebar } from "@/components/sidebar";
import { usePathname } from "next/navigation";
import { useChatContext } from "@/components/chat/chat-context";

export function SidebarContainer() {
  const pathname = usePathname();
  const {
    showChatView,
    setShowChatView,
    activeChatId,
    setActiveChatId,
    showSettingsView,
    setShowSettingsView,
    activeSettingsTab,
    setActiveSettingsTab,
  } = useChatContext();

  // Ensure chat view is shown when on chat page
  React.useEffect(() => {
    if (pathname === "/chat") {
      const wasOnChatPage = sessionStorage.getItem("lastPage") === "/chat";
      const hasManuallyHidden = sessionStorage.getItem("chatViewManuallyHidden") === "true";

      if (!wasOnChatPage) {
        sessionStorage.removeItem("chatViewManuallyHidden");
        setShowChatView(true);
      } else if (!hasManuallyHidden && !showChatView) {
        setShowChatView(true);
      }

      sessionStorage.setItem("lastPage", "/chat");
    } else if (typeof window !== "undefined") {
      sessionStorage.setItem("lastPage", window.location.pathname);
    }
  }, [pathname, showChatView, setShowChatView]);

  // Ensure settings view is shown when on settings page
  React.useEffect(() => {
    if (pathname === "/settings") {
      const wasOnSettingsPage = sessionStorage.getItem("lastPage") === "/settings";
      const hasManuallyHidden = sessionStorage.getItem("settingsViewManuallyHidden") === "true";

      if (!wasOnSettingsPage) {
        sessionStorage.removeItem("settingsViewManuallyHidden");
        setShowSettingsView(true);
      } else if (!hasManuallyHidden && !showSettingsView) {
        setShowSettingsView(true);
      }

      sessionStorage.setItem("lastPage", "/settings");
    }
  }, [pathname, showSettingsView, setShowSettingsView]);

  const handleChatViewChange = React.useCallback(
    (show: boolean) => {
      if (typeof window !== "undefined" && !show) {
        sessionStorage.setItem("chatViewManuallyHidden", "true");
      } else if (typeof window !== "undefined" && show) {
        sessionStorage.removeItem("chatViewManuallyHidden");
      }
      setShowChatView(show);
    },
    [setShowChatView]
  );

  const handleSettingsViewChange = React.useCallback(
    (show: boolean) => {
      if (typeof window !== "undefined" && !show) {
        sessionStorage.setItem("settingsViewManuallyHidden", "true");
      } else if (typeof window !== "undefined" && show) {
        sessionStorage.removeItem("settingsViewManuallyHidden");
      }
      setShowSettingsView(show);
    },
    [setShowSettingsView]
  );

  return (
    <MorphikSidebar
      showChatView={showChatView}
      onChatViewChange={handleChatViewChange}
      activeChatId={activeChatId}
      onChatSelect={setActiveChatId}
      showSettingsView={showSettingsView}
      onSettingsViewChange={handleSettingsViewChange}
      activeSettingsTab={activeSettingsTab}
      onSettingsTabChange={setActiveSettingsTab}
    />
  );
}
