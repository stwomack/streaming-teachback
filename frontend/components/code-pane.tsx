"use client";

import { useEffect, useMemo, useRef } from "react";
import type { SourceResponse } from "@/lib/types";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export type FileKey = "workflow" | "activity";

export function CodePane({
  sources,
  activeFile,
  onSelectFile,
  highlightSubstrings,
}: {
  sources: SourceResponse | null;
  activeFile: FileKey;
  onSelectFile: (f: FileKey) => void;
  highlightSubstrings: string[];
}) {
  const file = sources?.[activeFile];
  const lines = useMemo(() => (file?.code ?? "").split("\n"), [file]);

  const highlighted = useMemo(() => {
    const set = new Set<number>();
    lines.forEach((line, idx) => {
      if (highlightSubstrings.some((s) => s && line.includes(s))) set.add(idx);
    });
    return set;
  }, [lines, highlightSubstrings]);

  const firstRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    firstRef.current?.scrollIntoView({ behavior: "smooth", block: "center" });
  }, [activeFile, highlightSubstrings]);

  let firstSeen = false;

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-center gap-2 border-b border-border p-2">
        {(["workflow", "activity"] as FileKey[]).map((k) => (
          <Button
            key={k}
            size="sm"
            variant={activeFile === k ? "default" : "outline"}
            onClick={() => onSelectFile(k)}
          >
            {sources?.[k]?.path ?? k}
          </Button>
        ))}
      </div>
      <div className="flex-1 overflow-auto bg-[#0b1120] font-mono text-xs leading-5">
        {lines.map((line, idx) => {
          const isHi = highlighted.has(idx);
          const attachRef = isHi && !firstSeen;
          if (attachRef) firstSeen = true;
          return (
            <div
              key={idx}
              ref={attachRef ? firstRef : undefined}
              className={cn(
                "flex whitespace-pre",
                isHi && "bg-primary/20 border-l-2 border-primary"
              )}
            >
              <span className="w-10 flex-none select-none pr-3 text-right text-muted-foreground/50">
                {idx + 1}
              </span>
              <code className="flex-1 pr-4">{line || " "}</code>
            </div>
          );
        })}
      </div>
    </div>
  );
}
