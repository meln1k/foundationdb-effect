import { Schema } from "effect";

/** Internal storage failures, exposed as the cause of Cluster PersistenceError. */
export class MissingMessageRecordError
  extends Schema.TaggedError<MissingMessageRecordError>()(
    "MissingMessageRecordError",
    {
      record: Schema.Literals(["order", "envelope", "reply"]),
      id: Schema.String,
    },
  ) {
  override get message(): string {
    return `Missing mailbox ${this.record}`;
  }
}

export class DuplicateReplyError
  extends Schema.TaggedError<DuplicateReplyError>()(
    "DuplicateReplyError",
    {
      requestId: Schema.String,
      replyId: Schema.String,
      sequence: Schema.NullOr(Schema.Int),
    },
  ) {
  override get message(): string {
    return "Duplicate reply sequence or exit";
  }
}

export class RunnerMachineIdsExhaustedError
  extends Schema.TaggedError<RunnerMachineIdsExhaustedError>()(
    "RunnerMachineIdsExhaustedError",
    {
      address: Schema.String,
      capacity: Schema.Int,
    },
  ) {
  override get message(): string {
    return "RunnerStorage machine IDs exhausted; unregister retired runners";
  }
}
