import { Console, Effect, Schema, Stream } from "effect";
import {
  FoundationDb,
  FoundationDbTransaction,
  keyRange,
  pack,
  Subspace,
  unpack,
} from "../mod.ts";
import { assert, bytes, concatBytes, runMain } from "./_shared.ts";

interface User {
  readonly id: string;
  readonly name: string;
  readonly zipcode: string;
}

const IndexTuple = Schema.Tuple([Schema.String, Schema.String]);
const NameTuple = Schema.Tuple([Schema.String]);

const seedUsers: ReadonlyArray<User> = [
  { id: "001", zipcode: "20500", name: "Barack" },
  { id: "002", zipcode: "20500", name: "Michelle" },
  { id: "003", zipcode: "20500", name: "Sasha" },
  { id: "004", zipcode: "20500", name: "Malia" },
  { id: "005", zipcode: "20500", name: "Bo" },
  { id: "101", zipcode: "SW1A 1AA", name: "Elizabeth" },
  { id: "102", zipcode: "SW1A 1AA", name: "Philip" },
];

const program = Effect.gen(function* () {
  const database = yield* FoundationDb;
  const users = Subspace.fromBytes(bytes("user"));
  const zipcodeIndex = Subspace.fromBytes(bytes("zipcode_index"));
  const [usersBegin, usersEnd] = yield* users.range();
  const [indexBegin, indexEnd] = yield* zipcodeIndex.range();

  yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      yield* transaction.clearRange(usersBegin, usersEnd);
      yield* transaction.clearRange(indexBegin, indexEnd);
      for (const user of seedUsers) {
        const userKey = yield* users.pack([user.id, user.zipcode]);
        const indexKey = yield* zipcodeIndex.pack([user.zipcode, user.id]);
        yield* transaction.set(userKey, yield* pack([user.name]));
        yield* transaction.set(indexKey, new Uint8Array());
      }
    }),
  );

  const found = yield* database.withTransaction(
    Effect.gen(function* () {
      const transaction = yield* FoundationDbTransaction;
      const packedPrefix = yield* zipcodeIndex.pack(["205"]);
      const begin = packedPrefix;
      const end = concatBytes(packedPrefix.slice(0, -1), Uint8Array.of(0xff));
      const rows = yield* Stream.runCollect(
        transaction.getRange(keyRange(begin, end)),
      );

      return yield* Effect.forEach(rows, (row) =>
        Effect.gen(function* () {
          const [zipcode, id] = yield* zipcodeIndex.unpack(
            row.key,
            IndexTuple,
          );
          const value = yield* transaction.get(
            yield* users.pack([id, zipcode]),
          );
          assert(value !== undefined, `missing user ${id}`);
          const [name] = yield* unpack(value, NameTuple);
          return { id, zipcode, name } satisfies User;
        }));
    }),
  );

  assert(found.length === 5, `expected 5 users, found ${found.length}`);
  yield* Effect.forEach(found, (user) =>
    Console.log(
      `id => '${user.id}', zipcode => '${user.zipcode}', name => '${user.name}'`,
    ), { discard: true });
});

await runMain(program);
