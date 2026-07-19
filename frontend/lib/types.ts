export type EventKind =
  | "start"
  | "token"
  | "retry"
  | "fallback"
  | "complete"
  | "error";

export interface StreamEvent {
  kind: EventKind;
  seq: number;
  text: string;
  attempt: number;
  publisher_id: string;
  detail: string;
  offset: number;
}

export interface Segment {
  attempt: number;
  publisherId: string;
  text: string;
  retracted: boolean;
  complete: boolean;
  errored: boolean;
  fallbackReason?: string;
}

export interface SourceFile {
  path: string;
  code: string;
}

export interface SourceResponse {
  workflow: SourceFile;
  activity: SourceFile;
}
