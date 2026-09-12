import { Console, Effect, Random, Ref, Schema, Stream } from "effect";
import {
  FoundationDb,
  FoundationDbTransaction,
  keyRange,
  pack,
  Subspace,
  unpack,
} from "../mod.ts";
import { assert, runMain } from "./_shared.ts";

class NoRemainingSeats extends Schema.TaggedError<NoRemainingSeats>()(
  "NoRemainingSeats",
  { className: Schema.String },
) {}

class TooManyClasses extends Schema.TaggedError<TooManyClasses>()(
  "TooManyClasses",
  { student: Schema.String },
) {}

const SeatsTuple = Schema.Tuple([Schema.BigInt]);
const ClassKeyTuple = Schema.Tuple([Schema.String]);
const AttendanceKeyTuple = Schema.Tuple([Schema.String, Schema.String]);

const levels = [
  "intro",
  "for dummies",
  "remedial",
  "101",
  "201",
  "301",
  "mastery",
  "lab",
  "seminar",
] as const;
const subjects = [
  "chem",
  "bio",
  "cs",
  "geometry",
  "calc",
  "alg",
  "film",
  "music",
  "art",
  "dance",
] as const;
const times = Array.from({ length: 18 }, (_, index) => `${index + 2}:00`);
const allClasses = times.flatMap((time) =>
  subjects.flatMap((subject) =>
    levels.map((level) => `${time} ${subject} ${level}`)
  )
);

const choose = Effect.fnUntraced(function* <A>(
  values: ReadonlyArray<A>,
) {
  assert(values.length > 0, "cannot choose from an empty collection");
  const random = yield* Random.next;
  return values[
    Math.min(Math.floor(random * values.length), values.length - 1)
  ];
});

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const attendance = yield* Subspace.fromTuple(["attends"]);
  const classes = yield* Subspace.fromTuple(["class"]);
  const [attendanceBegin, attendanceEnd] = yield* attendance.range();
  const [classesBegin, classesEnd] = yield* classes.range();

  const seatsFor = Effect.fn("example.seatsFor")(function* (
    className: string,
    snapshot: boolean,
  ) {
    const transaction = yield* FoundationDbTransaction;
    const encoded = yield* transaction.get(
      yield* classes.pack([className]),
      { snapshot },
    );
    assert(encoded !== undefined, `class not found: ${className}`);
    const [seats] = yield* unpack(encoded, SeatsTuple);
    return seats;
  });

  const ditchTransaction = Effect.fn("example.ditchTransaction")(function* (
    student: string,
    className: string,
  ) {
    const transaction = yield* FoundationDbTransaction;
    const attendanceKey = yield* attendance.pack([student, className]);
    const enrolled = yield* transaction.get(attendanceKey, { snapshot: true });
    if (enrolled === undefined) {
      return;
    }
    const seats = yield* seatsFor(className, true);
    yield* transaction.set(
      yield* classes.pack([className]),
      yield* pack([seats + 1n]),
    );
    yield* transaction.clear(attendanceKey);
  });

  const signupTransaction = Effect.fn("example.signupTransaction")(function* (
    student: string,
    className: string,
  ) {
    const transaction = yield* FoundationDbTransaction;
    const attendanceKey = yield* attendance.pack([student, className]);
    const enrolled = yield* transaction.get(attendanceKey, { snapshot: true });
    if (enrolled !== undefined) {
      return;
    }
    const seats = yield* seatsFor(className, true);
    if (seats <= 0n) {
      return yield* new NoRemainingSeats({ className });
    }
    const [begin, end] = yield* attendance.range([student]);
    const current = yield* Stream.runCollect(
      transaction.getRange(keyRange(begin, end, { limit: 5 })),
    );
    if (current.length >= 5) {
      return yield* new TooManyClasses({ student });
    }
    yield* transaction.set(
      yield* classes.pack([className]),
      yield* pack([seats - 1n]),
    );
    yield* transaction.set(attendanceKey, yield* pack([""]));
  });

  const availableClasses = Effect.fn("example.availableClasses")(function* () {
    return yield* database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const rows = yield* Stream.runCollect(
          transaction.getRange(keyRange(classesBegin, classesEnd)),
        );
        const available: Array<string> = [];
        for (const row of rows) {
          const [seats] = yield* unpack(row.value, SeatsTuple);
          if (seats <= 0n) {
            continue;
          }
          const [name] = yield* classes.unpack(
            row.key,
            ClassKeyTuple,
          );
          available.push(name);
        }
        return available;
      }),
    );
  });

  yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* transaction.clearRange(attendanceBegin, attendanceEnd);
      yield* transaction.clearRange(classesBegin, classesEnd);
      for (const className of allClasses) {
        yield* transaction.set(
          yield* classes.pack([className]),
          yield* pack([100n]),
        );
      }
    }),
  );
  yield* Console.log(`initialized ${allClasses.length} classes`);

  const simulateStudent = Effect.fn("example.simulateStudent")(function* (
    student: string,
  ) {
    const enrolled = yield* Ref.make<ReadonlyArray<string>>([]);
    const candidates = yield* Ref.make<ReadonlyArray<string>>(allClasses);
    for (let operation = 0; operation < 10; operation++) {
      const current = yield* Ref.get(enrolled);
      const available = yield* Ref.get(candidates);
      const moods = current.length === 0
        ? ["add"] as const
        : current.length >= 5
        ? ["ditch", "switch"] as const
        : ["add", "ditch", "switch"] as const;
      const mood = yield* choose(moods);
      const oldClass = mood === "add" ? undefined : yield* choose(current);
      const newClass = mood === "ditch" ? undefined : yield* choose(available);
      const action = mood === "add"
        ? database.withTransaction(
          signupTransaction(student, newClass!),
        )
        : mood === "ditch"
        ? database.withTransaction(
          ditchTransaction(student, oldClass!),
        )
        : database.withTransaction(
          Effect.gen(function* () {
            yield* ditchTransaction(student, oldClass!);
            yield* signupTransaction(student, newClass!);
          }),
        );

      const succeeded = yield* action.pipe(
        Effect.as(true),
        Effect.catch((error) =>
          Console.log(
            `${student}: ${error._tag}; refreshing available classes`,
          ).pipe(
            Effect.andThen(availableClasses()),
            Effect.tap((fresh) => Ref.set(candidates, fresh)),
            Effect.as(false),
          )
        ),
      );
      if (!succeeded) {
        continue;
      }
      if (mood === "add") {
        yield* Ref.update(enrolled, (items) => [...items, newClass!]);
      } else if (mood === "ditch") {
        yield* Ref.update(
          enrolled,
          (items) => items.filter((item) => item !== oldClass),
        );
      } else {
        yield* Ref.update(enrolled, (items) => [
          ...items.filter((item) => item !== oldClass),
          newClass!,
        ]);
      }
    }
  });

  const students = Array.from({ length: 10 }, (_, index) => `s${index}`);
  yield* Effect.forEach(students, simulateStudent, {
    concurrency: "unbounded",
    discard: true,
  });
  yield* Effect.forEach(students, (student) =>
    database.withTransaction(
      Effect.gen(function* () {
        const transaction = yield* FoundationDbTransaction;
        const [begin, end] = yield* attendance.range([student]);
        const rows = yield* Stream.runCollect(
          transaction.getRange(keyRange(begin, end)),
        );
        yield* Effect.forEach(rows, (row) =>
          Effect.gen(function* () {
            const [, className] = yield* attendance.unpack(
              row.key,
              AttendanceKeyTuple,
            );
            yield* Console.log(`${student} is taking: ${className}`);
          }), { discard: true });
      }),
    ), { concurrency: 1, discard: true });
  yield* Console.log("100 student operations completed");
});

await runMain(program);
