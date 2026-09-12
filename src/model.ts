import { Schema } from "effect";

export type Bytes = Uint8Array<ArrayBufferLike>;

export class KeySelector extends Schema.Class<KeySelector>("KeySelector")({
  key: Schema.Uint8Array,
  orEqual: Schema.Boolean,
  offset: Schema.Int,
}) {
  static lastLessThan(key: Bytes): KeySelector {
    return new KeySelector({ key, orEqual: false, offset: 0 });
  }

  static lastLessOrEqual(key: Bytes): KeySelector {
    return new KeySelector({ key, orEqual: true, offset: 0 });
  }

  static firstGreaterThan(key: Bytes): KeySelector {
    return new KeySelector({ key, orEqual: true, offset: 1 });
  }

  static firstGreaterOrEqual(key: Bytes): KeySelector {
    return new KeySelector({ key, orEqual: false, offset: 1 });
  }
}

export class KeyValue extends Schema.Class<KeyValue>("KeyValue")({
  key: Schema.Uint8Array,
  value: Schema.Uint8Array,
}) {}

export const StreamingMode = {
  WantAll: -2,
  Iterator: -1,
  Exact: 0,
  Small: 1,
  Medium: 2,
  Large: 3,
  Serial: 4,
} as const;

export type StreamingMode = typeof StreamingMode[keyof typeof StreamingMode];

export interface RangeOptions {
  readonly begin: KeySelector;
  readonly end: KeySelector;
  readonly limit?: number;
  readonly targetBytes?: number;
  readonly mode?: StreamingMode;
  readonly reverse?: boolean;
  readonly snapshot?: boolean;
}

export interface TransactionOptions {
  readonly timeoutMs?: number;
  readonly retryLimit?: number;
  readonly maxRetryDelayMs?: number;
}

export interface TransactionAttempt {
  readonly attempt: number;
  readonly maybeCommitted: boolean;
}

export const keyRange = (
  begin: Bytes,
  end: Bytes,
  options: Omit<RangeOptions, "begin" | "end"> = {},
): RangeOptions => ({
  begin: KeySelector.firstGreaterOrEqual(begin),
  end: KeySelector.firstGreaterOrEqual(end),
  ...options,
});
