"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { TokenFeed } from "@/components/token-feed";
import { CodePane, type FileKey } from "@/components/code-pane";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { reduceEvent, reduceAll } from "@/lib/reducer";
import type { EventKind, Segment, SourceResponse, StreamEvent } from "@/lib/types";
import {
  armNetworkBlip,
  askQuestion,
  fetchHistory,
  fetchSource,
  streamUrl,
} from "@/lib/api";

const LS_KEY = "streams-demo-workflow-id";

type ConnState = "idle" | "connecting" | "live" | "reconnecting" | "closed";

// Maps the current phase (last event kind) to which source file + which lines to
// spotlight in the right pane, so the code follows the execution.
const PHASE_CODE: Record<
  EventKind | "idle",
  { file: FileKey; anchors: string[] }
> = {
  idle: {
    file: "workflow",
    anchors: ["self.stream = WorkflowStream(prior_state", "execute_activity"],
  },
  start: { file: "activity", anchors: ["kind=KIND_START"] },
  token: {
    file: "activity",
    anchors: ["async for text in stream.text_stream", "kind=KIND_TOKEN"],
  },
  retry: { file: "activity", anchors: ["if attempt > 1", "kind=KIND_RETRY"] },
  fallback: {
    file: "activity",
    anchors: ["except (anthropic.APIError", "kind=KIND_FALLBACK"],
  },
  complete: { file: "activity", anchors: ["kind=KIND_COMPLETE"] },
  error: { file: "activity", anchors: ["kind=KIND_ERROR", "kind=KIND_FALLBACK"] },
};

export default function Page() {
  const [question, setQuestion] = useState("Teach me everything I need to know about the late bronze age collapse in 1000 words or more. Don't stop until the report is done");
  const [workflowId, setWorkflowId] = useState<string | null>(null);
  const [segments, setSegments] = useState<Segment[]>([]);
  const [phase, setPhase] = useState<EventKind | "idle">("idle");
  const [conn, setConn] = useState<ConnState>("idle");
  const [lastOffset, setLastOffset] = useState<number | null>(null);
  const [sources, setSources] = useState<SourceResponse | null>(null);
  const [activeFile, setActiveFile] = useState<FileKey>("workflow");

  const esRef = useRef<EventSource | null>(null);

  useEffect(() => {
    fetchSource().then(setSources).catch(() => {});
  }, []);

  const closeStream = useCallback(() => {
    esRef.current?.close();
    esRef.current = null;
  }, []);

  const handleEvent = useCallback((evt: StreamEvent) => {
    setSegments((prev) => reduceEvent(prev, evt));
    setPhase(evt.kind);
    setLastOffset(evt.offset);
    const cfg = PHASE_CODE[evt.kind];
    if (cfg) setActiveFile(cfg.file);
    if (evt.kind === "complete") {
      // Response finished — stop the tail so EventSource doesn't reconnect-spin
      // against a terminal workflow.
      setConn("closed");
      closeStream();
    }
  }, [closeStream]);

  const openStream = useCallback(
    (wf: string, fromOffset: number) => {
      closeStream();
      setConn("connecting");
      const es = new EventSource(streamUrl(wf, fromOffset));
      esRef.current = es;
      es.onopen = () => setConn("live");
      es.onmessage = (m) => {
        try {
          handleEvent(JSON.parse(m.data) as StreamEvent);
        } catch {
          /* ignore malformed frame */
        }
      };
      es.onerror = () => {
        // Browser auto-reconnects using Last-Event-ID; we just reflect state.
        setConn((c) => (c === "closed" ? "closed" : "reconnecting"));
      };
    },
    [closeStream, handleEvent]
  );

  // Scenario 5: on load, rehydrate an in-flight workflow from buffered history,
  // then attach the live tail from the current head — the task does not restart.
  const rehydrate = useCallback(
    async (wf: string) => {
      setConn("connecting");
      const h = await fetchHistory(wf, 0);
      setSegments(reduceAll(h.events as StreamEvent[]));
      if (h.events.length) {
        const last = h.events[h.events.length - 1] as StreamEvent;
        setPhase(last.kind);
        setLastOffset(last.offset);
      }
      openStream(wf, h.head);
    },
    [openStream]
  );

  useEffect(() => {
    const saved =
      typeof window !== "undefined" ? window.localStorage.getItem(LS_KEY) : null;
    if (saved) {
      setWorkflowId(saved);
      rehydrate(saved).catch(() => setConn("idle"));
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const onAsk = useCallback(
    async (simulateFailure = false) => {
      closeStream();
      setSegments([]);
      setPhase("idle");
      setLastOffset(null);
      const wf = await askQuestion(question, simulateFailure);
      setWorkflowId(wf);
      window.localStorage.setItem(LS_KEY, wf);
      openStream(wf, 0);
    },
    [question, openStream, closeStream]
  );

  const codeCfg = PHASE_CODE[phase] ?? PHASE_CODE.idle;

  return (
    <main className="flex h-screen flex-col gap-3 p-4">
      <Header
        conn={conn}
        workflowId={workflowId}
        lastOffset={lastOffset}
        phase={phase}
      />

      <ControlBar
        question={question}
        setQuestion={setQuestion}
        onAsk={onAsk}
        disabled={conn === "connecting"}
      />

      <div className="grid min-h-0 flex-1 grid-cols-2 gap-3">
        <Card className="flex min-h-0 flex-col">
          <CardHeader className="border-b border-border py-3">
            <CardTitle className="flex items-center justify-between text-sm">
              <span>Live token feed</span>
              <span className="text-xs font-normal text-muted-foreground">
                left pane · subscriber view (offset-addressed)
              </span>
            </CardTitle>
          </CardHeader>
          <CardContent className="min-h-0 flex-1 overflow-hidden p-0">
            <TokenFeed segments={segments} />
          </CardContent>
        </Card>

        <Card className="flex min-h-0 flex-col">
          <CardHeader className="border-b border-border py-3">
            <CardTitle className="flex items-center justify-between text-sm">
              <span>Executing Python</span>
              <span className="text-xs font-normal text-muted-foreground">
                right pane · phase: {phase}
              </span>
            </CardTitle>
          </CardHeader>
          <CardContent className="min-h-0 flex-1 overflow-hidden p-0">
            <CodePane
              sources={sources}
              activeFile={activeFile}
              onSelectFile={setActiveFile}
              highlightSubstrings={codeCfg.anchors}
            />
          </CardContent>
        </Card>
      </div>
    </main>
  );
}

function Header({
  conn,
  workflowId,
  lastOffset,
  phase,
}: {
  conn: ConnState;
  workflowId: string | null;
  lastOffset: number | null;
  phase: string;
}) {
  const connVariant =
    conn === "live"
      ? "default"
      : conn === "reconnecting"
      ? "destructive"
      : "secondary";
  return (
    <div className="flex items-center justify-between">
      <div>
        <h1 className="text-lg font-semibold">
          Temporal Workflow Streams — mechanics demo
        </h1>
        <p className="text-xs text-muted-foreground">
          Four distinct failure/resume scenarios. Watch the offset, the retry
          boundary, and the code follow execution.
        </p>
      </div>
      <div className="flex items-center gap-2 text-xs">
        <Badge variant={connVariant as any}>SSE: {conn}</Badge>
        <Badge variant="outline">offset: {lastOffset ?? "—"}</Badge>
        <Badge variant="outline">phase: {phase}</Badge>
        <span className="max-w-[220px] truncate text-muted-foreground">
          {workflowId ?? "no workflow"}
        </span>
      </div>
    </div>
  );
}

function ControlBar({
  question,
  setQuestion,
  onAsk,
  disabled,
}: {
  question: string;
  setQuestion: (q: string) => void;
  onAsk: (simulateFailure?: boolean) => void;
  disabled: boolean;
}) {
  const [blipArmed, setBlipArmed] = useState(false);
  return (
    <div className="flex flex-wrap items-center gap-2">
      <input
        className="h-9 min-w-[320px] flex-1 rounded-md border border-input bg-background px-3 text-sm outline-none focus-visible:ring-2 focus-visible:ring-ring"
        value={question}
        onChange={(e) => setQuestion(e.target.value)}
        placeholder="Ask a plain question…"
        onKeyDown={(e) => e.key === "Enter" && !disabled && onAsk(false)}
      />
      <Button onClick={() => onAsk(false)} disabled={disabled}>
        Ask
      </Button>
      <Button
        variant="outline"
        onClick={() => onAsk(true)}
        disabled={disabled}
        title="Scenario 2: force a real activity-level retry early in the stream — new publisher, RETRY boundary, retraction — without killing the worker"
      >
        Ask with simulated failure
      </Button>
      <Button
        variant="outline"
        onClick={async () => {
          await armNetworkBlip();
          setBlipArmed(true);
          setTimeout(() => setBlipArmed(false), 4000);
        }}
        title="Scenario 4: drop the SSE connection once; EventSource auto-reconnects via Last-Event-ID"
      >
        {blipArmed ? "Blip armed ✓" : "Arm network blip"}
      </Button>
      <span className="text-xs text-muted-foreground">
        Scenario 3 via script · Scenario 5: refresh this tab
      </span>
    </div>
  );
}
