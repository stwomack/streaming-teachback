import type { Segment, StreamEvent } from "./types";

// Idempotent reducer over stream events. The SAME function drives both the
// page-load rehydration (replaying buffered history) and the live SSE tail, so
// a refreshed page reconstructs byte-identical state.
//
// The core teaching point lives here: on a `retry` event we do NOT dedupe or
// merge — we RETRACT the prior attempt's partial output and start a fresh
// segment, because an activity-level retry is a new publisher whose events were
// never deduplicated against the dead attempt's.
export function reduceEvent(prev: Segment[], evt: StreamEvent): Segment[] {
  const segments = prev.map((s) => ({ ...s }));
  const last = segments[segments.length - 1];

  switch (evt.kind) {
    case "retry": {
      // Retract the dead attempt's partial output and open a new segment.
      if (last) last.retracted = true;
      segments.push(newSegment(evt));
      return segments;
    }
    case "start": {
      // Idempotent: a `retry` already opened this attempt's segment. Only open
      // one if none exists for this attempt yet (the normal first-attempt path).
      if (!last || last.attempt !== evt.attempt || last.complete || last.retracted) {
        segments.push(newSegment(evt));
      } else if (!last.publisherId) {
        last.publisherId = evt.publisher_id;
      }
      return segments;
    }
    case "token": {
      const seg = ensureSegment(segments, evt);
      seg.text += evt.text;
      return segments;
    }
    case "fallback": {
      const seg = ensureSegment(segments, evt);
      seg.text += evt.text;
      seg.fallbackReason = evt.detail;
      return segments;
    }
    case "complete": {
      const seg = segments[segments.length - 1];
      if (seg) seg.complete = true;
      return segments;
    }
    case "error": {
      const seg = ensureSegment(segments, evt);
      seg.errored = true;
      seg.fallbackReason = evt.detail;
      return segments;
    }
    default:
      return segments;
  }
}

export function reduceAll(events: StreamEvent[]): Segment[] {
  return events.reduce(reduceEvent, [] as Segment[]);
}

function newSegment(evt: StreamEvent): Segment {
  return {
    attempt: evt.attempt,
    publisherId: evt.publisher_id,
    text: "",
    retracted: false,
    complete: false,
    errored: false,
  };
}

function ensureSegment(segments: Segment[], evt: StreamEvent): Segment {
  let last = segments[segments.length - 1];
  if (!last || last.complete || last.retracted) {
    last = newSegment(evt);
    segments.push(last);
  }
  return last;
}
