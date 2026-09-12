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

export class ConflictRange
  extends Schema.Class<ConflictRange>("ConflictRange")({
    begin: Schema.Uint8Array,
    end: Schema.Uint8Array,
  }) {}

export const ConflictRangeType = {
  Read: 0,
  Write: 1,
} as const;

export type ConflictRangeType =
  typeof ConflictRangeType[keyof typeof ConflictRangeType];

export const MutationType = {
  Add: 2,
  BitAnd: 6,
  BitOr: 7,
  BitXor: 8,
  AppendIfFits: 9,
  Max: 12,
  Min: 13,
  SetVersionstampedKey: 14,
  SetVersionstampedValue: 15,
  ByteMin: 16,
  ByteMax: 17,
  CompareAndClear: 20,
} as const;

export type MutationType = typeof MutationType[keyof typeof MutationType];

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
  readonly reportConflictingKeys?: boolean;
  /** Whether to retry when FoundationDB cannot determine if commit succeeded. Defaults to true. */
  readonly retryOnMaybeCommitted?: boolean;
}

export interface TransactionAttempt {
  readonly attempt: number;
  readonly maybeCommitted: boolean;
  /** Key ranges reported for the preceding failed commit. */
  readonly conflictingKeyRanges: ReadonlyArray<ConflictRange>;
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
