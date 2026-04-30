import type { Metadata } from "next";
import { cookies } from "next/headers";
import localFont from "next/font/local";
import "./globals.css";
import { AlertSystem } from "@/components/ui/alert-system";
import { ThemeProvider } from "@/components/theme-provider";
import { DynamicSiteHeader } from "@/components/dynamic-site-header";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar-components";
import { MorphikProvider } from "@/contexts/morphik-context";
import { HeaderProvider } from "@/contexts/header-context";
import { ChatProvider } from "@/components/chat/chat-context";
import { SidebarContainer } from "@/components/sidebar-container";

const geistSans = localFont({
  src: "./fonts/GeistVF.woff",
  variable: "--font-geist-sans",
  weight: "100 900",
});
const geistMono = localFont({
  src: "./fonts/GeistMonoVF.woff",
  variable: "--font-geist-mono",
  weight: "100 900",
});

export const metadata: Metadata = {
  title: "KNOW-hub | The Obsidian Void",
  description: "Enterprise-grade knowledge engine — Search, explore, and query across your organization's intelligence.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  const sidebarCookie = cookies().get("sidebar_state")?.value;
  const defaultOpen = sidebarCookie === undefined ? true : sidebarCookie === "true";

  return (
    <html lang="en" className="dark" suppressHydrationWarning>
      <head>
        {/* Obsidian Void typography stack */}
        <link rel="preconnect" href="https://fonts.googleapis.com" />
        <link rel="preconnect" href="https://fonts.gstatic.com" crossOrigin="anonymous" />
        <link
          href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap"
          rel="stylesheet"
        />
        <link
          href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:wght,FILL@100..700,0..1&display=swap"
          rel="stylesheet"
        />
      </head>
      <body className={`${geistSans.variable} ${geistMono.variable} antialiased bg-kh-black text-kh-text`}>
        <ThemeProvider attribute="class" defaultTheme="dark" enableSystem={false} disableTransitionOnChange>
          <div className="min-h-screen bg-kh-black">
            <MorphikProvider>
              <HeaderProvider>
                <ChatProvider>
                  <SidebarProvider
                    defaultOpen={defaultOpen}
                    style={
                      {
                        "--sidebar-width": "calc(var(--spacing) * 70)",
                        "--header-height": "calc(var(--spacing) * 12)",
                      } as React.CSSProperties
                    }
                  >
                    <SidebarContainer />
                    <SidebarInset>
                      <DynamicSiteHeader />
                      <div className="flex flex-1 flex-col p-4 md:p-6">{children}</div>
                    </SidebarInset>
                  </SidebarProvider>
                </ChatProvider>
              </HeaderProvider>
            </MorphikProvider>
          </div>
          <AlertSystem position="bottom-right" />
        </ThemeProvider>
      </body>
    </html>
  );
}
