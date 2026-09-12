import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";
import {
  OtlpLogger,
  OtlpSerialization,
  OtlpTracer,
} from "effect/unstable/observability";
import { FoundationDb, type FoundationDbOptions } from "../mod.ts";

const nativeExtension = Deno.build.os === "darwin"
  ? "dylib"
  : Deno.build.os === "windows"
  ? "dll"
  : "so";

export const foundationDbOptions: FoundationDbOptions = {
  libraryPath:
    `./target/debug/libeffect_foundationdb_native.${nativeExtension}`,
  transactionDefaults: {
    timeoutMs: 5_000,
    retryLimit: 3,
  },
};

const encoder = new TextEncoder();
const decoder = new TextDecoder("utf-8", { fatal: true });

export const bytes = (value: string): Uint8Array => encoder.encode(value);
export const text = (value: Uint8Array): string => decoder.decode(value);

export const concatBytes = (
  ...parts: ReadonlyArray<Uint8Array<ArrayBufferLike>>
): Uint8Array => {
  const output = new Uint8Array(
    parts.reduce((length, part) => length + part.byteLength, 0),
  );
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.byteLength;
  }
  return output;
};

export const littleEndianInt64 = (value: bigint): Uint8Array => {
  const output = new Uint8Array(8);
  new DataView(output.buffer).setBigInt64(0, value, true);
  return output;
};

export const readLittleEndianInt64 = (value: Uint8Array): bigint => {
  if (value.byteLength !== 8) {
    throw new Error(
      `expected an 8-byte integer, got ${value.byteLength} bytes`,
    );
  }
  return new DataView(
    value.buffer,
    value.byteOffset,
    value.byteLength,
  ).getBigInt64(0, true);
};

export const assert: (
  condition: boolean,
  message: string,
) => asserts condition = (condition, message) => {
  if (!condition) {
    throw new Error(message);
  }
};

export const assertBytesEqual = (
  actual: Uint8Array,
  expected: Uint8Array,
): void => {
  assert(
    actual.byteLength === expected.byteLength &&
      actual.every((byte, index) => byte === expected[index]),
    `byte arrays differ (${actual.byteLength} vs ${expected.byteLength} bytes)`,
  );
};

const exampleName = new URL(Deno.mainModule).pathname
  .split("/")
  .at(-1)
  ?.replace(/\.ts$/, "") ?? "unknown";

const telemetryEndpoint = (): string | undefined => {
  try {
    return Deno.env.get("EFFECT_FOUNDATIONDB_OTLP_ENDPOINT")?.replace(
      /\/$/,
      "",
    );
  } catch {
    return undefined;
  }
};

const telemetryLayer = (endpoint: string) => {
  const resource = {
    serviceName: `effect-foundationdb-example-${exampleName}`,
    serviceVersion: "0.1.0",
    attributes: {
      "example.name": exampleName,
    },
  };

  return Layer.merge(
    OtlpTracer.layer({
      url: `${endpoint}/v1/traces`,
      resource,
      exportInterval: "100 millis",
    }),
    OtlpLogger.layer({
      url: `${endpoint}/v1/logs`,
      resource,
      exportInterval: "100 millis",
    }),
  ).pipe(
    Layer.provide(OtlpSerialization.layerJson),
    Layer.provide(FetchHttpClient.layer),
  );
};

export const runMain = <A, E>(
  program: Effect.Effect<A, E, FoundationDb>,
): Promise<A> => {
  const endpoint = telemetryEndpoint();
  const observed = Effect.gen(function* () {
    yield* Effect.logInfo("example started");
    const result = yield* program;
    yield* Effect.logInfo("example completed");
    return result;
  }).pipe(
    Effect.withSpan("example.run", {
      attributes: { "example.name": exampleName },
    }),
    Effect.annotateLogs({ example: exampleName }),
    Effect.provide(FoundationDb.layer(foundationDbOptions)),
  );

  return Effect.runPromise(
    endpoint === undefined
      ? observed
      : observed.pipe(Effect.provide(telemetryLayer(endpoint))),
  );
};
