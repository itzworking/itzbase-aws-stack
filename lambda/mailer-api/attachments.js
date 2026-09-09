// Attachment validation for the mailer gateway. Only URL attachments are
// accepted: the producer stores metadata on the FIFO queue and the sender
// fetches the bytes at send time, so an inline `content` payload would sit
// against the 256 KB SQS message cap. `path` is accepted as an alias of `url`
// (the common transactional-email API shape names it that way).

const MAX_ATTACHMENTS = 10;

function normalizeAttachment(input, index) {
  const label = `attachments[${index}]`;
  if (!input || typeof input !== "object") return { error: `${label}: invalid attachment object` };

  if (input.content != null) {
    return { error: `${label}: inline content is not supported, provide a url` };
  }

  const rawUrl = typeof input.url === "string" ? input.url : input.path;
  if (typeof rawUrl !== "string" || !rawUrl.trim()) {
    return { error: `${label}: missing url` };
  }
  let url;
  try {
    url = new URL(rawUrl.trim());
  } catch {
    return { error: `${label}: invalid url` };
  }
  if (url.protocol !== "https:" && url.protocol !== "http:") {
    return { error: `${label}: url must be http(s)` };
  }

  const filename =
    typeof input.filename === "string" && input.filename.trim()
      ? input.filename.trim()
      : decodeURIComponent(url.pathname.split("/").filter(Boolean).pop() || "");
  if (!filename) return { error: `${label}: missing filename` };

  const contentType =
    (typeof input.content_type === "string" && input.content_type.trim()) ||
    (typeof input.contentType === "string" && input.contentType.trim()) ||
    null;

  return { attachment: { filename, contentType, url: url.toString() } };
}

// Returns { error } or { attachments } (an empty array when none were given).
function normalizeAttachments(input) {
  if (input == null) return { attachments: [] };
  if (!Array.isArray(input)) return { error: "attachments must be an array" };
  if (input.length > MAX_ATTACHMENTS) {
    return { error: `Too many attachments (max ${MAX_ATTACHMENTS})` };
  }
  const attachments = [];
  for (let i = 0; i < input.length; i++) {
    const built = normalizeAttachment(input[i], i);
    if (built.error) return { error: built.error };
    attachments.push(built.attachment);
  }
  return { attachments };
}

module.exports = { normalizeAttachments };
