const crypto = require("node:crypto");

// Minimal RFC 2045/2047/2231 MIME builder for sends carrying attachments. The
// simple SES content shape has no attachment slot, so those sends go through
// SES raw content, and this file owns the wire format. Every body part is
// base64 so any html/text content is safe regardless of line length or
// charset. Bcc is deliberately absent from the headers: SES takes the envelope
// recipients from Destination, and a Bcc header would leak the list.

const CRLF = "\r\n";

const isAscii = (value) => /^[\x20-\x7e]*$/.test(value);

// RFC 2047 encoded-word for header values (Subject, display names).
function encodeHeaderWord(value) {
  if (isAscii(value)) return value;
  return `=?UTF-8?B?${Buffer.from(value, "utf8").toString("base64")}?=`;
}

// RFC 2231 filename parameter; plain quoted form when it is ASCII already.
function filenameParam(filename) {
  const safe = filename.replace(/["\\\r\n]/g, "_");
  if (isAscii(safe)) return `filename="${safe}"`;
  const encoded = encodeURIComponent(safe).replace(/['()*]/g, (c) =>
    `%${c.charCodeAt(0).toString(16).toUpperCase()}`,
  );
  return `filename="${safe.replace(/[^\x20-\x7e]/g, "_")}"; filename*=UTF-8''${encoded}`;
}

const base64Lines = (buffer) =>
  buffer.toString("base64").replace(/(.{76})/g, `$1${CRLF}`);

const boundary = () => `----=_itzbase_${crypto.randomBytes(12).toString("hex")}`;

function textPart(contentType, content) {
  return [
    `Content-Type: ${contentType}; charset=UTF-8`,
    "Content-Transfer-Encoding: base64",
    "",
    base64Lines(Buffer.from(content, "utf8")),
  ].join(CRLF);
}

function bodyPart(message) {
  const parts = [];
  if (message.text) parts.push(textPart("text/plain", message.text));
  if (message.html) parts.push(textPart("text/html", message.html));
  if (parts.length === 1) return parts[0];
  const b = boundary();
  return [
    `Content-Type: multipart/alternative; boundary="${b}"`,
    "",
    ...parts.flatMap((part) => [`--${b}`, part]),
    `--${b}--`,
  ].join(CRLF);
}

function attachmentPart(attachment) {
  return [
    `Content-Type: ${attachment.contentType}`,
    "Content-Transfer-Encoding: base64",
    `Content-Disposition: attachment; ${filenameParam(attachment.filename)}`,
    "",
    base64Lines(attachment.data),
  ].join(CRLF);
}

// message: the queued message; attachments: [{ filename, contentType, data }]
// with data already fetched as a Buffer. Returns the raw message bytes.
function buildRawMessage(message, attachments) {
  const headers = [
    `From: ${message.from}`,
    `To: ${message.to.join(", ")}`,
    ...(message.cc?.length ? [`Cc: ${message.cc.join(", ")}`] : []),
    ...(message.replyTo?.length ? [`Reply-To: ${message.replyTo.join(", ")}`] : []),
    `Subject: ${encodeHeaderWord(message.subject)}`,
    "MIME-Version: 1.0",
  ];
  const b = boundary();
  const lines = [
    ...headers,
    `Content-Type: multipart/mixed; boundary="${b}"`,
    "",
    `--${b}`,
    bodyPart(message),
    ...attachments.flatMap((attachment) => [`--${b}`, attachmentPart(attachment)]),
    `--${b}--`,
    "",
  ];
  return Buffer.from(lines.join(CRLF), "utf8");
}

module.exports = { buildRawMessage };
