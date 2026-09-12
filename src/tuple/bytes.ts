export const concatBytes = (
  ...parts: ReadonlyArray<Uint8Array>
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

export const compareBytes = (
  left: Uint8Array,
  right: Uint8Array,
): number => {
  const length = Math.min(left.byteLength, right.byteLength);
  for (let index = 0; index < length; index++) {
    if (left[index] !== right[index]) {
      return left[index] < right[index] ? -1 : 1;
    }
  }
  return left.byteLength === right.byteLength
    ? 0
    : left.byteLength < right.byteLength
    ? -1
    : 1;
};
