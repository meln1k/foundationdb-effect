import { Schema } from "effect";

export class FoundationDbError extends Schema.TaggedError<FoundationDbError>()(
  "FoundationDbError",
  {
    operation: Schema.String,
    code: Schema.Int,
    message: Schema.String,
    retryable: Schema.Boolean,
    maybeCommitted: Schema.Boolean,
    retryableNotCommitted: Schema.Boolean,
  },
) {}

export const isFoundationDbError = Schema.is(FoundationDbError);
