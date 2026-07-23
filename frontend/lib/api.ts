import type { SourceResponse } from "./types";

export const BRIDGE_URL =
  process.env.NEXT_PUBLIC_BRIDGE_URL ?? "http://127.0.0.1:8000";

export async function askQuestion(
  question: string,
  simulateFailure = false
): Promise<string> {
  const res = await fetch(`${BRIDGE_URL}/ask`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ question, simulate_failure: simulateFailure }),
  });
  if (!res.ok) throw new Error(`ask failed: ${res.status}`);
  const data = await res.json();
  return data.workflow_id as string;
}

export interface HistoryResponse {
  workflow_id: string;
  head: number;
  events: any[];
}

export async function fetchHistory(
  workflowId: string,
  fromOffset = 0
): Promise<HistoryResponse> {
  const res = await fetch(
    `${BRIDGE_URL}/history/${encodeURIComponent(workflowId)}?from_offset=${fromOffset}`
  );
  if (!res.ok) throw new Error(`history failed: ${res.status}`);
  return res.json();
}

export async function fetchOffset(
  workflowId: string
): Promise<{ head: number; last_persisted: number | null }> {
  const res = await fetch(`${BRIDGE_URL}/offset/${encodeURIComponent(workflowId)}`);
  if (!res.ok) throw new Error(`offset failed: ${res.status}`);
  return res.json();
}

export async function fetchSource(): Promise<SourceResponse> {
  const res = await fetch(`${BRIDGE_URL}/source`);
  if (!res.ok) throw new Error(`source failed: ${res.status}`);
  return res.json();
}

export async function armNetworkBlip(): Promise<void> {
  await fetch(`${BRIDGE_URL}/chaos/drop-next-sse`, { method: "POST" });
}

export function streamUrl(workflowId: string, fromOffset: number): string {
  return `${BRIDGE_URL}/stream/${encodeURIComponent(workflowId)}?from_offset=${fromOffset}`;
}
