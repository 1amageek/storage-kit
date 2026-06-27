const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

export function encodeBase64URL(bytes) {
  let output = "";
  let index = 0;
  while (index + 3 <= bytes.length) {
    const chunk = (bytes[index] << 16) | (bytes[index + 1] << 8) | bytes[index + 2];
    output += alphabet[(chunk >> 18) & 0x3f];
    output += alphabet[(chunk >> 12) & 0x3f];
    output += alphabet[(chunk >> 6) & 0x3f];
    output += alphabet[chunk & 0x3f];
    index += 3;
  }
  const remaining = bytes.length - index;
  if (remaining === 1) {
    const chunk = bytes[index] << 16;
    output += alphabet[(chunk >> 18) & 0x3f];
    output += alphabet[(chunk >> 12) & 0x3f];
  } else if (remaining === 2) {
    const chunk = (bytes[index] << 16) | (bytes[index + 1] << 8);
    output += alphabet[(chunk >> 18) & 0x3f];
    output += alphabet[(chunk >> 12) & 0x3f];
    output += alphabet[(chunk >> 6) & 0x3f];
  }
  return output;
}

export function decodeBase64URL(value) {
  const output = [];
  let buffer = 0;
  let bits = 0;
  for (const character of value) {
    const decoded = alphabet.indexOf(character);
    if (decoded < 0) {
      throw new Error("Invalid base64url character");
    }
    buffer = (buffer << 6) | decoded;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      output.push((buffer >> bits) & 0xff);
    }
  }
  return new Uint8Array(output);
}
