"use client";

export const dynamic = "force-dynamic";

import { useEffect, useState, Suspense } from "react";
import DocumentsSection from "@/components/documents/DocumentsSection";
import { useMorphik } from "@/contexts/morphik-context";
import { useRouter, useSearchParams } from "next/navigation";
import { useHeader } from "@/contexts/header-context";

/* ── Obsidian Void toolbar button ── */
function ObsidianButton({
  onClick,
  icon,
  label,
  danger = false,
}: {
  onClick: () => void;
  icon: string;
  label: string;
  danger?: boolean;
}) {
  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-2 border bg-transparent px-4 py-2 font-display text-sm font-medium uppercase tracking-wider transition-colors hover:bg-kh-surface ${
        danger
          ? "border-kh-danger/50 text-kh-danger hover:border-kh-danger"
          : "border-kh-border text-kh-accent hover:border-kh-accent"
      }`}
    >
      <span className="material-symbols-outlined text-[18px]">{icon}</span>
      {label}
    </button>
  );
}

function ObsidianIconButton({ onClick, icon, title }: { onClick: () => void; icon: string; title: string }) {
  return (
    <button
      onClick={onClick}
      title={title}
      className="flex items-center justify-center border border-kh-border bg-transparent p-2 text-kh-text transition-colors hover:bg-kh-surface hover:border-kh-accent"
    >
      <span className="material-symbols-outlined text-[18px]">{icon}</span>
    </button>
  );
}

function DocumentsContent() {
  const { apiBaseUrl, authToken } = useMorphik();
  const router = useRouter();
  const searchParams = useSearchParams();
  const { setCustomBreadcrumbs, setRightContent } = useHeader();

  const folderParam = searchParams?.get("folder") || null;
  const [currentFolder, setCurrentFolder] = useState<string | null>(folderParam);
  const [selectedDocuments, setSelectedDocuments] = useState<string[]>([]);
  const [showNewFolderDialog, setShowNewFolderDialog] = useState(false);
  const [showUploadDialog, setShowUploadDialog] = useState(false);

  useEffect(() => {
    setCurrentFolder(folderParam);
  }, [folderParam]);

  useEffect(() => {
    const breadcrumbs = [
      { label: "Home", href: "/" },
      {
        label: "Knowledge Base",
        ...(currentFolder ? { href: "/documents" } : {}),
      },
      ...(currentFolder
        ? [{ label: currentFolder === "all" ? "All Documents" : currentFolder }]
        : []),
    ];
    setCustomBreadcrumbs(breadcrumbs);

    /* ── Obsidian Void toolbar buttons ── */
    const rightContent = (
      <>
        <ObsidianButton
          onClick={() => window.dispatchEvent(new CustomEvent("openNewFolderDialog"))}
          icon="create_new_folder"
          label="New Folder"
        />
        {selectedDocuments.length > 0 && (
          <ObsidianButton
            onClick={() => window.dispatchEvent(new CustomEvent("deleteMultipleDocuments"))}
            icon="delete"
            label={`Delete (${selectedDocuments.length})`}
            danger
          />
        )}
        <ObsidianIconButton
          onClick={() => window.location.reload()}
          icon="refresh"
          title="Refresh documents"
        />
        <ObsidianButton
          onClick={() => window.dispatchEvent(new CustomEvent("openUploadDialog"))}
          icon="upload"
          label="Upload"
        />
      </>
    );

    setRightContent(rightContent);

    return () => {
      setCustomBreadcrumbs(null);
      setRightContent(null);
    };
  }, [currentFolder, router, selectedDocuments, setCustomBreadcrumbs, setRightContent]);

  useEffect(() => {
    const handleSelectionChange = (event: CustomEvent<{ selectedDocuments?: string[] }>) => {
      setSelectedDocuments(event.detail?.selectedDocuments || []);
    };
    const handleOpenNewFolderDialog = () => setShowNewFolderDialog(true);
    const handleOpenUploadDialog = () => setShowUploadDialog(true);

    window.addEventListener("documentsSelectionChanged", handleSelectionChange as EventListener);
    window.addEventListener("openNewFolderDialog", handleOpenNewFolderDialog);
    window.addEventListener("openUploadDialog", handleOpenUploadDialog);

    return () => {
      window.removeEventListener("documentsSelectionChanged", handleSelectionChange as EventListener);
      window.removeEventListener("openNewFolderDialog", handleOpenNewFolderDialog);
      window.removeEventListener("openUploadDialog", handleOpenUploadDialog);
    };
  }, []);

  const handleFolderClick = (folderName: string | null) => {
    setCurrentFolder(folderName);
    if (folderName) {
      router.push(`/documents?folder=${encodeURIComponent(folderName)}`);
    } else {
      router.push("/documents");
    }
  };

  return (
    <DocumentsSection
      apiBaseUrl={apiBaseUrl}
      authToken={authToken}
      initialFolder={folderParam}
      onDocumentUpload={undefined}
      onDocumentDelete={undefined}
      onDocumentClick={undefined}
      onFolderCreate={undefined}
      onFolderClick={handleFolderClick}
      onRefresh={undefined}
      onViewInPDFViewer={(documentId: string) => {
        router.push(`/pdf?document=${documentId}`);
      }}
      showNewFolderDialog={showNewFolderDialog}
      setShowNewFolderDialog={setShowNewFolderDialog}
      showUploadDialog={showUploadDialog}
      setShowUploadDialog={setShowUploadDialog}
    />
  );
}

export default function DocumentsPage() {
  return (
    <Suspense fallback={
      <div className="flex items-center justify-center p-12">
        <div className="font-mono text-xs uppercase tracking-widest text-kh-muted animate-pulse">
          Loading Knowledge Base...
        </div>
      </div>
    }>
      <DocumentsContent />
    </Suspense>
  );
}
