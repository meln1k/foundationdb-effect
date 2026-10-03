import { testClusterFile } from "./support/real-database.ts";

const clusterFile = testClusterFile();
const run = async (
  command: string,
  args: string[],
  env?: Record<string, string>,
) => {
  const status = await new Deno.Command(command, {
    args,
    ...(env === undefined ? {} : { env }),
    stdin: "null",
    stdout: "inherit",
    stderr: "inherit",
  }).spawn().status;
  if (!status.success) {
    throw new Error(`${command} exited with code ${status.code}`);
  }
};

// The local service owns the cluster; this launcher only checks connectivity.
// A transactional read (not merely an open TCP port) establishes readiness.
try {
  await run(Deno.env.get("FDBCLI") ?? "fdbcli", [
    "-C",
    clusterFile,
    "--exec",
    "get __effect_foundationdb_test_readiness__",
    "--timeout",
    "20",
  ]);
} catch (cause) {
  throw new Error(
    "Cannot reach FoundationDB. Start the service (amp orb services ensure in an orb), or set FDB_TEST_CLUSTER_FILE to a running development cluster.",
    { cause },
  );
}
console.log(
  `Running against FoundationDB at ${clusterFile} with isolated test directories`,
);
const files = Deno.args.length > 0
  ? Deno.args
  : ["tests/storage_test.ts", "tests/integration/live_test.ts"];
// Separate processes isolate the native-lifecycle integration test.
for (const file of files) {
  await run(Deno.execPath(), [
    "test",
    "--allow-env",
    "--allow-ffi",
    "--allow-read",
    file,
  ], { FDB_TEST_CLUSTER_FILE: clusterFile });
}
