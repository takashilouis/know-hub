"use client";

export const dynamic = "force-dynamic";

import { useEffect, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import { useMorphik } from "@/contexts/morphik-context";
import { useHeader } from "@/contexts/header-context";
import { PDFViewer } from "@/components/pdf/PDFViewer";

function PDFViewerContent() {
  const { apiBaseUrl, authToken } = useMorphik();
  const searchParams = useSearchParams();
  const { setCustomBreadcrumbs } = useHeader();

  const documentId = searchParams?.get("document") || null;

  useEffect(() => {
    setCustomBreadcrumbs([
      { label: "Home", href: "/" },
      { label: "Documents", href: "/documents" },
      { label: "PDF Viewer" },
    ]);
    return () => setCustomBreadcrumbs(null);
  }, [setCustomBreadcrumbs]);

  return (
    /* -m-4 md:-m-6 cancels the parent layout padding so the PDF viewer fills full width/height */
    <div className="-m-4 md:-m-6 flex flex-1 flex-col overflow-hidden">
      <PDFViewer apiBaseUrl={apiBaseUrl} authToken={authToken} initialDocumentId={documentId || undefined} />
    </div>
  );
}

export default function PDFViewerPage() {
  return (
    <Suspense
      fallback={
        <div className="flex h-full items-center justify-center">
          <p className="font-mono text-xs uppercase tracking-widest text-kh-muted animate-pulse">
            Initializing PDF engine...
          </p>
        </div>
      }
    >
      <PDFViewerContent />
    </Suspense>
  );
}
