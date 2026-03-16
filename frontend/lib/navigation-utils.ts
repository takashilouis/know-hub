import { IconFiles, IconSearch, IconMessage, IconSettings, IconLayoutDashboard } from "@tabler/icons-react";

export interface BaseNavItem {
  title: string;
  icon: React.ComponentType;
  isSpecial?: boolean;
}

export interface UrlNavItem extends BaseNavItem {
  type: "url";
  url: string;
}

export interface SectionNavItem extends BaseNavItem {
  type: "section";
  section: string;
}

export type NavItem = UrlNavItem | SectionNavItem;

export interface NavigationStrategy {
  type: "url" | "section";
  mainItems: NavItem[];
  secondaryItems: NavItem[];
  onItemClick: (item: NavItem) => void;
  currentActive?: string;
}

export const baseMainNavItems: Omit<BaseNavItem, "type">[] = [
  {
    title: "Home",
    icon: IconLayoutDashboard,
  },
  {
    title: "Knowledge Base",
    icon: IconFiles,
  },
  {
    title: "Search",
    icon: IconSearch,
  },
  {
    title: "Ask AI",
    icon: IconMessage,
    isSpecial: true,
  },
];

export const baseSecondaryNavItems: Omit<BaseNavItem, "type">[] = [
  {
    title: "Settings",
    icon: IconSettings,
    isSpecial: true,
  },
];

// No external links for internal tool
export const externalNavItems: { title: string; url: string; icon: React.ComponentType }[] = [];

export const createUrlNavigation = (onChatClick: () => void, onSettingsClick?: () => void): NavigationStrategy => ({
  type: "url",
  mainItems: baseMainNavItems.map((item, index) => {
    const urlMap = ["/", "/documents", "/search", "/chat"];
    return {
      ...item,
      type: "url" as const,
      url: urlMap[index] ?? "/",
    };
  }),
  secondaryItems: baseSecondaryNavItems.map(item => ({
    ...item,
    type: "url" as const,
    url: "/settings",
  })),
  onItemClick: item => {
    if ("url" in item) {
      if (item.isSpecial && item.url === "/settings" && onSettingsClick) {
        onSettingsClick();
        return;
      }
      if (item.isSpecial && item.url === "/chat") {
        if (typeof window !== "undefined" && window.location.pathname === "/chat") {
          onChatClick();
          return;
        }
      }
      window.location.href = item.url;
    }
  },
});

export const createSectionNavigation = (
  onSectionChange: (section: string) => void,
  onChatClick: () => void,
  currentSection?: string,
  onSettingsClick?: () => void
): NavigationStrategy => ({
  type: "section",
  mainItems: baseMainNavItems.map((item, index) => {
    const sectionMap = ["home", "documents", "search", "chat"];
    return {
      ...item,
      type: "section" as const,
      section: sectionMap[index] ?? "home",
    };
  }),
  secondaryItems: baseSecondaryNavItems.map(item => ({
    ...item,
    type: "section" as const,
    section: "settings",
  })),
  currentActive: currentSection,
  onItemClick: item => {
    if (item.isSpecial && "section" in item && item.section === "chat") {
      onChatClick();
      onSectionChange(item.section);
    } else if (item.isSpecial && "section" in item && item.section === "settings" && onSettingsClick) {
      onSettingsClick();
      onSectionChange(item.section);
    } else if ("section" in item) {
      onSectionChange(item.section);
    }
  },
});
