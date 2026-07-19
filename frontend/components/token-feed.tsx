"use client";

import { useEffect, useRef } from "react";
import type { Segment } from "@/lib/types";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export function TokenFeed({ segments }: { segments: Segment[] }) {
  const endRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth", block: "end" });
  }, [segments]);

  if (segments.length === 0) {
    return (
      <div className="flex h-full items-center justify-center text-muted-foreground text-sm">
        No stream yet. Ask a question to begin.
      </div>
    );
  }

  return (
    <div className="flex flex-col gap-3 overflow-y-auto p-4 font-mono text-sm leading-relaxed">
      {segments.map((seg, i) => (
        <div key={i} className="flex flex-col gap-1">
          {seg.attempt > 1 && <RetryBoundary attempt={seg.attempt} />}
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <Badge variant={seg.retracted ? "destructive" : "secondary"}>
              attempt {seg.attempt}
            </Badge>
            <span className="truncate">publisher_id: {seg.publisherId || "—"}</span>
            {seg.retracted && (
              <span className="text-destructive">· stale / retracted</span>
            )}
            {seg.complete && !seg.retracted && (
              <span className="text-primary">· complete</span>
            )}
          </div>
          <div
            className={cn(
              "whitespace-pre-wrap break-words rounded-md border border-border bg-secondary/30 p-3",
              seg.retracted && "retracted",
              seg.errored && "border-destructive"
            )}
          >
            {seg.text || <span className="text-muted-foreground">…</span>}
            {seg.fallbackReason && (
              <div className="mt-2 text-xs text-muted-foreground">
                fallback reason: {seg.fallbackReason}
              </div>
            )}
          </div>
        </div>
      ))}
      <div ref={endRef} />
    </div>
  );
}

function RetryBoundary({ attempt }: { attempt: number }) {
  return (
    <div className="my-2 flex items-center gap-2">
      <div className="h-px flex-1 bg-destructive/60" />
      <span className="rounded bg-destructive px-2 py-0.5 text-xs font-bold uppercase tracking-wider text-destructive-foreground">
        ⟲ RETRY BOUNDARY — new publisher_id (attempt {attempt})
      </span>
      <div className="h-px flex-1 bg-destructive/60" />
    </div>
  );
}
