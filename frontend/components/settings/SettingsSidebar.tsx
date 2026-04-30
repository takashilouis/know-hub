import React from "react";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/ui/tooltip";

interface SettingsSidebarProps {
  activeTab: string;
  onTabChange: (tab: string) => void;
  onBackClick?: () => void;
  collapsed: boolean;
  onToggle: () => void;
}

const TABS = [
  { id: "api-keys",      label: "API Keys",       icon: "key" },
  { id: "models",        label: "Models",          icon: "psychology" },
  { id: "configuration", label: "Configuration",   icon: "tune" },
  { id: "usage",         label: "Usage",           icon: "bar_chart" },
];

export const SettingsSidebar: React.FC<SettingsSidebarProps> = ({
  activeTab,
  onTabChange,
  onBackClick,
  collapsed,
  onToggle,
}) => {
  if (collapsed) {
    return (
      <TooltipProvider>
        <div className="flex h-full w-10 flex-col items-center border-r border-kh-border bg-kh-black py-2">
          {TABS.map(tab => (
            <Tooltip key={tab.id}>
              <TooltipTrigger asChild>
                <button
                  aria-label={tab.label}
                  className={`mt-2 p-2 transition-colors ${
                    activeTab === tab.id ? "text-kh-accent" : "text-kh-muted hover:text-white"
                  }`}
                  onClick={() => { onTabChange(tab.id); onToggle(); }}
                >
                  <span className="material-symbols-outlined text-[18px]">{tab.icon}</span>
                </button>
              </TooltipTrigger>
              <TooltipContent side="right" className="border-kh-border bg-kh-surface text-kh-text">
                {tab.label}
              </TooltipContent>
            </Tooltip>
          ))}
        </div>
      </TooltipProvider>
    );
  }

  return (
    /* Matches settings/code.html inner-tabs layout */
    <div className="flex h-full w-[200px] flex-col border-r border-kh-border bg-kh-black p-6">
      <nav className="flex flex-col gap-2">
        {TABS.map(tab => (
          <button
            key={tab.id}
            onClick={() => onTabChange(tab.id)}
            className={`px-3 py-2 text-sm font-medium text-left transition-colors ${
              activeTab === tab.id
                ? "border-l-2 border-kh-accent bg-kh-surface text-kh-text"
                : "border-l-2 border-transparent text-kh-muted hover:bg-kh-surface hover:text-kh-text"
            }`}
          >
            {tab.label}
          </button>
        ))}
      </nav>
    </div>
  );
};
