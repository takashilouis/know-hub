"use client";

import { useState, useEffect, useCallback } from "react";
import {
  getConnectorAuthStatus,
  initiateConnectorAuth,
  disconnectConnector,
  ingestConnectorFile,
  submitManualCredentials,
  type ConnectorAuthStatus,
  type CredentialField,
} from "@/lib/connectorsApi";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Loader2, AlertCircle } from "lucide-react";
import { FileBrowser } from "./FileBrowser";
import { Textarea } from "@/components/ui/textarea";
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogClose } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";

interface ConnectorCardProps {
  connectorType: string;
  displayName: string;
  icon?: React.ElementType;
  apiBaseUrl: string;
  authToken: string | null;
  materialIcon?: string;
  description?: string;
}

export function ConnectorCard({
  connectorType,
  displayName,
  apiBaseUrl,
  authToken,
  materialIcon = "link",
  description,
}: ConnectorCardProps) {
  const [authStatus, setAuthStatus] = useState<ConnectorAuthStatus | null>(null);
  const [isLoading, setIsLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [isSubmitting, setIsSubmitting] = useState<boolean>(false);
  const [showFileBrowserModal, setShowFileBrowserModal] = useState<boolean>(false);
  const [showIngestionModal, setShowIngestionModal] = useState<boolean>(false);

  const [ingestionTargetFileId, setIngestionTargetFileId] = useState<string | null>(null);
  const [ingestionTargetFileName, setIngestionTargetFileName] = useState<string | null>(null);
  const [ingestionMetadata, setIngestionMetadata] = useState<string>("{}");

  const [showCredentialsModal, setShowCredentialsModal] = useState<boolean>(false);
  const [credentialFields, setCredentialFields] = useState<CredentialField[]>([]);
  const [credentialValues, setCredentialValues] = useState<Record<string, string>>({});
  const [credentialInstructions, setCredentialInstructions] = useState<string>("");

  const fetchStatus = useCallback(async () => {
    setIsLoading(true);
    setError(null);
    try {
      const status = await getConnectorAuthStatus(apiBaseUrl, connectorType, authToken);
      setAuthStatus(status);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch status.");
      setAuthStatus(null);
    } finally {
      setIsLoading(false);
    }
  }, [apiBaseUrl, connectorType, authToken]);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  const handleConnect = async () => {
    setError(null);
    setIsSubmitting(true);
    try {
      const connectionsSectionUrl = new URL(window.location.origin);
      connectionsSectionUrl.pathname = "/";
      connectionsSectionUrl.searchParams.set("section", "connections");

      const authResponse = await initiateConnectorAuth(
        apiBaseUrl,
        connectorType,
        connectionsSectionUrl.toString(),
        authToken
      );

      if ("auth_type" in authResponse && authResponse.auth_type === "manual_credentials") {
        setCredentialFields(authResponse.required_fields);
        setCredentialInstructions(authResponse.instructions || "");
        const initialValues: Record<string, string> = {};
        authResponse.required_fields.forEach(field => {
          initialValues[field.name] = "";
        });
        setCredentialValues(initialValues);
        setShowCredentialsModal(true);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to initiate connection.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleSubmitCredentials = async () => {
    setError(null);
    setIsSubmitting(true);
    try {
      const missingFields = credentialFields
        .filter(field => field.required && !credentialValues[field.name]?.trim())
        .map(field => field.label);

      if (missingFields.length > 0) {
        throw new Error(`Please fill in: ${missingFields.join(", ")}`);
      }

      await submitManualCredentials(apiBaseUrl, connectorType, credentialValues, authToken);
      setShowCredentialsModal(false);
      await fetchStatus();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to submit credentials.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleDisconnect = async () => {
    setError(null);
    setIsSubmitting(true);
    try {
      await disconnectConnector(apiBaseUrl, connectorType, authToken);
      await fetchStatus();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to disconnect.");
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleFileIngest = async (fileId: string, fileName: string, ingestedConnectorType: string) => {
    if (ingestedConnectorType !== connectorType) return;
    setIngestionTargetFileId(fileId);
    setIngestionTargetFileName(fileName);
    setIngestionMetadata("{}");
    setShowIngestionModal(true);
    setError(null);
  };

  const handleRepositoryIngest = async (repoPath: string, ingestedConnectorType: string) => {
    if (ingestedConnectorType !== connectorType) return;
    setIsSubmitting(true);
    setError(null);
    try {
      const response = await fetch(`${apiBaseUrl}/ee/connectors/${ingestedConnectorType}/ingest-repository`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${authToken}`,
        },
        body: JSON.stringify({
          connector_type: ingestedConnectorType,
          repo_path: repoPath,
          folder_name: "github-repos",
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || "Failed to ingest repository");
      }

      const result = await response.json();
      const docCount = result.documents?.length || 0;
      alert(`Successfully ingested repository "${repoPath}"! Created ${docCount} document${docCount !== 1 ? "s" : ""}.`);
      setShowFileBrowserModal(false);
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : "Unknown error";
      setError(errorMessage);
      alert(`Failed to ingest repository "${repoPath}": ${errorMessage}`);
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleConfirmFileIngest = async () => {
    if (!ingestionTargetFileId || !ingestionTargetFileName) return;
    setIsSubmitting(true);
    setError(null);
    try {
      const result = await ingestConnectorFile(apiBaseUrl, connectorType, authToken, ingestionTargetFileId, {
        metadata: JSON.parse(ingestionMetadata),
      });
      alert(
        `Successfully started ingestion for "${ingestionTargetFileName}"! Document ID: ${result.morphik_document_id || result.document_id || "N/A"}`
      );
      setShowIngestionModal(false);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Failed to ingest file.";
      setError(errorMessage);
      alert(`Failed to ingest "${ingestionTargetFileName}": ${errorMessage}`);
    } finally {
      setIsSubmitting(false);
    }
  };

  const isConnected = !isLoading && !error && authStatus?.is_authenticated;
  const isErrorState = !isLoading && (!!error || (!authStatus && !isLoading));

  // ── Stitch Obsidian Void tile ──────────────────────────────────────────────
  return (
    <>
      {/* Card tile — matches connections/code.html exactly */}
      <div
        className={`group relative flex h-[160px] max-w-[320px] w-full cursor-pointer flex-col justify-between border p-5 transition-colors
          ${isConnected
            ? "border-kh-border bg-kh-surface hover:border-kh-accent"
            : "border-[#262626] bg-kh-surface opacity-80 hover:border-neutral-600 hover:opacity-100"
          }`}
        onClick={() => {
          if (isConnected) setShowFileBrowserModal(true);
          else if (!isLoading && !isSubmitting) handleConnect();
        }}
        title={isConnected ? `Browse ${displayName} files` : `Connect to ${displayName}`}
      >
        {/* Top row: icon + status badge */}
        <div className="flex items-start justify-between">
          <div className={`flex h-10 w-10 items-center justify-center border
            ${isConnected ? "border-kh-border bg-[#0D0D0D]" : "border-[#262626] bg-kh-black"}`}
          >
            {isLoading ? (
              <Loader2 className="h-5 w-5 animate-spin text-kh-muted" />
            ) : (
              <span
                className={`material-symbols-outlined text-[24px] ${isConnected ? "text-white" : "text-kh-muted"}`}
                style={isConnected ? { fontVariationSettings: "'FILL' 1" } : undefined}
              >
                {materialIcon}
              </span>
            )}
          </div>

          {/* Status badge */}
          {isLoading ? null : isConnected ? (
            <span className="flex items-center gap-1.5 border border-kh-accent/30 bg-kh-accent/5 px-2 py-1 font-mono text-[11px] font-medium tracking-wide text-kh-accent">
              <span className="h-1.5 w-1.5 animate-pulse bg-kh-accent" />
              CONNECTED
            </span>
          ) : isErrorState ? (
            <span className="border border-red-800 bg-red-950/30 px-2 py-1 font-mono text-[11px] font-medium tracking-wide text-red-400">
              ERROR
            </span>
          ) : (
            <span className="border border-kh-border bg-kh-black px-2 py-1 font-mono text-[11px] font-medium tracking-wide text-kh-muted">
              DISCONNECTED
            </span>
          )}
        </div>

        {/* Bottom: name + description/status */}
        <div>
          <h3
            className={`font-display text-[18px] font-semibold leading-tight mb-1 transition-colors
              ${isConnected ? "text-white" : "text-kh-muted group-hover:text-white"}`}
          >
            {displayName}
          </h3>
          <p className="font-mono text-[13px] text-kh-muted truncate">
            {isLoading
              ? "Checking status..."
              : isConnected
              ? authStatus?.message || description || "Connected"
              : error
              ? error.slice(0, 48)
              : description || "Click to connect"}
          </p>
        </div>

        {/* Submitting overlay */}
        {isSubmitting && (
          <div className="absolute inset-0 flex items-center justify-center bg-kh-black/70">
            <Loader2 className="h-6 w-6 animate-spin text-kh-accent" />
          </div>
        )}
      </div>

      {/* ── Manual Credentials Modal ── */}
      <Dialog open={showCredentialsModal} onOpenChange={setShowCredentialsModal}>
        <DialogContent className="sm:max-w-[500px] bg-kh-surface border-kh-border text-kh-text">
          <DialogHeader>
            <DialogTitle className="font-display text-white">Connect to {displayName}</DialogTitle>
          </DialogHeader>
          <div className="space-y-4 py-4">
            {credentialInstructions && (
              <div className="border border-kh-border bg-kh-black p-3 font-mono text-sm text-kh-muted">
                {credentialInstructions}
              </div>
            )}
            {credentialFields.map(field => (
              <div key={field.name} className="space-y-2">
                <Label htmlFor={field.name} className="font-mono text-xs uppercase tracking-widest text-kh-muted">
                  {field.label}
                  {field.required && <span className="text-red-500 ml-1">*</span>}
                </Label>
                {field.description && <p className="text-xs text-kh-muted">{field.description}</p>}
                {field.type === "select" ? (
                  <Select
                    value={credentialValues[field.name] || ""}
                    onValueChange={value => setCredentialValues(prev => ({ ...prev, [field.name]: value }))}
                  >
                    <SelectTrigger className="border-kh-border bg-kh-black text-kh-text">
                      <SelectValue placeholder={`Select ${field.label}`} />
                    </SelectTrigger>
                    <SelectContent className="bg-kh-surface border-kh-border">
                      {field.options?.map(option => (
                        <SelectItem key={option.value} value={option.value}>
                          {option.label}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                ) : (
                  <Input
                    id={field.name}
                    type={field.type}
                    value={credentialValues[field.name] || ""}
                    onChange={e => setCredentialValues(prev => ({ ...prev, [field.name]: e.target.value }))}
                    placeholder={field.description}
                    required={field.required}
                    className="border-kh-border bg-kh-black text-kh-text"
                  />
                )}
              </div>
            ))}
            {error && (
              <div className="flex items-center gap-2 border border-red-800 bg-red-950/30 p-2 text-sm text-red-400">
                <AlertCircle className="h-4 w-4 shrink-0" /> {error}
              </div>
            )}
          </div>
          <DialogFooter>
            <DialogClose asChild>
              <Button variant="outline" className="border-kh-border text-kh-muted">Cancel</Button>
            </DialogClose>
            <Button onClick={handleSubmitCredentials} disabled={isSubmitting}
              className="bg-kh-accent text-black hover:bg-kh-accent/90">
              {isSubmitting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
              Connect
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* ── File Browser Modal ── */}
      <Dialog open={showFileBrowserModal} onOpenChange={setShowFileBrowserModal}>
        <DialogContent className="h-[80vh] max-w-4xl bg-kh-surface border-kh-border text-kh-text">
          <DialogHeader>
            <DialogTitle className="font-display text-white">Browse {displayName}</DialogTitle>
          </DialogHeader>
          <div className="h-full overflow-y-auto">
            <FileBrowser
              connectorType={connectorType}
              apiBaseUrl={apiBaseUrl}
              authToken={authToken}
              onFileIngest={handleFileIngest}
              onRepositoryIngest={handleRepositoryIngest}
            />
          </div>
        </DialogContent>
      </Dialog>

      {/* ── Ingestion Modal ── */}
      <Dialog open={showIngestionModal} onOpenChange={setShowIngestionModal}>
        <DialogContent className="sm:max-w-[625px] bg-kh-surface border-kh-border text-kh-text">
          <DialogHeader>
            <DialogTitle className="font-display text-white">
              Ingest: {ingestionTargetFileName || "File"}
            </DialogTitle>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="grid grid-cols-4 items-center gap-4">
              <label htmlFor="metadata" className="col-span-1 text-right font-mono text-xs uppercase tracking-widest text-kh-muted">
                Metadata (JSON)
              </label>
              <Textarea
                id="metadata"
                value={ingestionMetadata}
                onChange={e => setIngestionMetadata(e.target.value)}
                className="col-span-3 h-24 border-kh-border bg-kh-black font-mono text-sm text-kh-text"
                placeholder='e.g. { "source": "google_drive" }'
              />
            </div>
            {error && (
              <div className="col-span-4 flex items-center gap-2 border border-red-800 bg-red-950/30 p-2 text-sm text-red-400">
                <AlertCircle className="mr-1 inline h-4 w-4" /> {error}
              </div>
            )}
          </div>
          <DialogFooter>
            <DialogClose asChild>
              <Button variant="outline" className="border-kh-border text-kh-muted">Cancel</Button>
            </DialogClose>
            <Button onClick={handleConfirmFileIngest} disabled={isSubmitting}
              className="bg-kh-accent text-black hover:bg-kh-accent/90">
              {isSubmitting ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
              Confirm Ingest
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
