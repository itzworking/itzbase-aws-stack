const {
  SSMClient,
  GetParametersByPathCommand,
} = require("@aws-sdk/client-ssm");
const {
  SQSClient,
  SendMessageCommand,
  SendMessageBatchCommand,
} = require("@aws-sdk/client-sqs");
const {
  DynamoDBClient,
  PutItemCommand,
  GetItemCommand,
} = require("@aws-sdk/client-dynamodb");
const crypto = require("node:crypto");
const {
  FAKE_EMAIL_ID,
  hasValidSyntax,
  isDeliverable,
} = require("./recipients");
const { normalizeAttachments } = require("./attachments");

// Plain JS so it needs no bundling; the Lambda Node 22 runtime ships AWS SDK v3.
// Producer + read side of the throttled mailer, exposed as a drop-in /emails API
// (the common transactional-email API shape) so a caller can swap their email
// provider for this service with only a base-URL change. API Gateway routes (all
// on the shared API):
//   POST  /emails             -> send one    -> 200 { id }
//   POST  /emails/batch       -> send many   -> 200 { data: [{ id }] }
//   GET   /emails/{id}        -> retrieve     -> 200 { object:"email", id, last_event, ... }
//   PATCH /emails/{id}        -> stub: scheduling not supported
//   POST  /emails/{id}/cancel -> stub: scheduling not supported
// Auth accepts either `Authorization: Bearer <key>` or `x-api-key`, both matched
// against a per-profile key in SSM (/itzbase/profiles/<slug>/api-key).
// Account-wide SES defaults (/itzbase/ses/default-*) fill a missing sender /
// reply-to. Fake recipients (bad syntax, reserved or disposable domains, see
// recipients.js) are dropped before enqueue; an email left with no real `to`
// is acknowledged with the all-zeros FAKE_EMAIL_ID and never sent, and GET on
// that id returns a synthetic "delivered" status. Each real enqueue mints an
// emailId (the returned `id`), enqueues onto the FIFO queue, and writes an
// EMAIL#<id> tracking row to the events table so GET works immediately; the
// consumer stamps the id as an SES tag and the ingest Lambda advances
// last_event as SES events arrive.
const ssm = new SSMClient({});
const sqs = new SQSClient({});
const ddb = new DynamoDBClient({});

const QUEUE_URL = process.env.QUEUE_URL;
const EVENTS_TABLE = process.env.EVENTS_TABLE_NAME;
const ROOT_PATH = "/itzbase/";
const PROFILES_PREFIX = "profiles/";
const CACHE_TTL_MS = 20_000;
const MAX_BATCH = 100; // Batch sends are capped at 100 per request.
const TTL_DAYS = 90;

// SES event type -> public `last_event` status vocabulary.
const LAST_EVENT = {
  Send: "sent",
  Delivery: "delivered",
  Bounce: "bounced",
  Complaint: "complained",
  Reject: "failed",
  Open: "opened",
  Click: "clicked",
  RenderingFailure: "failed",
  DeliveryDelay: "delivery_delayed",
};

// In-memory cache of the relevant SSM tree, refreshed every 20s on a warm Lambda
// (kept short so a newly-created profile key starts working quickly).
let cache = null;

const json = (statusCode, body) => ({
  statusCode,
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

// Error envelope: { statusCode, message, name }.
const fail = (statusCode, name, message) =>
  json(statusCode, { statusCode, message, name });

function getHeader(headers, name) {
  if (!headers) return undefined;
  const lower = name.toLowerCase();
  for (const key of Object.keys(headers)) {
    if (key.toLowerCase() === lower) return headers[key];
  }
  return undefined;
}

// Some clients send `Authorization: Bearer <key>`; existing callers send
// `x-api-key`. Accept either so the swap is a pure base-URL change.
function getApiKey(headers) {
  const auth = getHeader(headers, "authorization");
  if (auth) {
    const m = /^Bearer\s+(.+)$/i.exec(String(auth).trim());
    if (m) return m[1].trim();
  }
  const xkey = getHeader(headers, "x-api-key");
  return xkey ? String(xkey).trim() : null;
}

function toList(value) {
  if (value == null) return [];
  const parts = Array.isArray(value) ? value : String(value).split(",");
  return parts.map((p) => String(p).trim()).filter(Boolean);
}

const sList = (arr) => ({ L: (arr || []).map((v) => ({ S: String(v) })) });
const fromList = (attr) => (attr && attr.L ? attr.L.map((x) => x.S) : []);

// One recursive scan of /itzbase/ yields both the profile api-keys and the
// account-wide SES defaults. The operator writes the namespace, so unknown
// leaves (e.g. ses/configuration-set) are simply ignored.
async function loadConfig() {
  const profiles = new Map();
  const defaults = { sender: null, replyTo: null };
  let nextToken;
  do {
    const res = await ssm.send(
      new GetParametersByPathCommand({
        Path: ROOT_PATH,
        Recursive: true,
        WithDecryption: true,
        MaxResults: 10,
        NextToken: nextToken,
      }),
    );
    for (const param of res.Parameters || []) {
      const rest = param.Name.slice(ROOT_PATH.length);
      if (rest.startsWith(PROFILES_PREFIX)) {
        const sub = rest.slice(PROFILES_PREFIX.length); // <slug>/<leaf>
        const slash = sub.indexOf("/");
        if (slash === -1) continue;
        const slug = sub.slice(0, slash);
        const leaf = sub.slice(slash + 1);
        const entry = profiles.get(slug) || { slug, key: null };
        if (leaf === "api-key") entry.key = param.Value;
        profiles.set(slug, entry);
      } else if (rest === "ses/default-sender") {
        defaults.sender = param.Value;
      } else if (rest === "ses/default-reply-to") {
        defaults.replyTo = param.Value;
      }
    }
    nextToken = res.NextToken;
  } while (nextToken);
  return { profiles, defaults };
}

async function getConfig() {
  const now = Date.now();
  if (cache && now - cache.at < CACHE_TTL_MS) return cache.config;
  const config = await loadConfig();
  cache = { at: now, config };
  return config;
}

function findProfile(profiles, apiKey) {
  const presented = Buffer.from(apiKey);
  for (const entry of profiles.values()) {
    if (!entry.key) continue;
    const candidate = Buffer.from(entry.key);
    if (
      candidate.length === presented.length &&
      crypto.timingSafeEqual(candidate, presented)
    ) {
      return entry;
    }
  }
  return null;
}

// Validate one inbound email and merge in the account-wide defaults. Returns
// { error } (caller responds 422), { fake: true } (every `to` recipient is
// fake: caller acknowledges with FAKE_EMAIL_ID and sends nothing), or
// { message } ready to enqueue. Fake cc/bcc recipients are dropped silently.
// Attachments are URL references only (see attachments.js); scheduled sends
// are not supported by this gateway.
function buildMessage(input, profile, defaults) {
  if (!input || typeof input !== "object") return { error: "Invalid email object" };

  if (input.scheduled_at != null || input.scheduledAt != null) {
    return { error: "Scheduling (scheduled_at) is not supported" };
  }
  const attachments = normalizeAttachments(input.attachments);
  if (attachments.error) return { error: attachments.error };

  const toRaw = toList(input.to);
  if (toRaw.length === 0) return { error: "Missing recipient: to" };

  const subject = typeof input.subject === "string" ? input.subject : "";
  if (!subject) return { error: "Missing field: subject" };

  const html = typeof input.html === "string" && input.html ? input.html : undefined;
  const text = typeof input.text === "string" && input.text ? input.text : undefined;
  if (!html && !text) return { error: "Provide at least one of: html, text" };

  const from =
    (typeof input.from === "string" && input.from.trim()) || defaults.sender;
  if (!from) {
    return {
      error:
        "Missing sender: provide 'from' or set the account default sender",
    };
  }
  if (!hasValidSyntax(from)) return { error: "Invalid sender address: from" };

  const replyTo = (
    input.replyTo != null || input.reply_to != null
      ? toList(input.replyTo ?? input.reply_to)
      : defaults.replyTo
        ? toList(defaults.replyTo)
        : []
  ).filter(hasValidSyntax);

  // Recipient hygiene: drop fake addresses (bad syntax, reserved or disposable
  // domains) instead of bouncing them. With no real `to` left, the whole email
  // is fake and is only acknowledged, never sent.
  const to = toRaw.filter(isDeliverable);
  const cc = toList(input.cc).filter(isDeliverable);
  const bcc = toList(input.bcc).filter(isDeliverable);
  const dropped = toRaw.length + toList(input.cc).length + toList(input.bcc).length
    - (to.length + cc.length + bcc.length);
  if (dropped > 0) {
    console.log("Dropped fake recipients", { profile: profile.slug, dropped });
  }
  if (to.length === 0) return { fake: true };

  return {
    message: {
      to,
      cc,
      bcc,
      from,
      replyTo,
      subject,
      html,
      text,
      attachments: attachments.attachments,
      tags: Array.isArray(input.tags) ? input.tags : [],
      profile: profile.slug,
    },
  };
}

// Each enqueue carries its emailId in the message body so the consumer can stamp
// it as an SES tag, and uses it as the SQS dedup key.
async function enqueueOne(message, emailId) {
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: QUEUE_URL,
      MessageBody: JSON.stringify({ ...message, emailId }),
      MessageGroupId: "default",
      MessageDeduplicationId: emailId,
    }),
  );
}

async function enqueueBatch(messages, emailIds) {
  for (let i = 0; i < messages.length; i += 10) {
    const chunk = messages.slice(i, i + 10);
    const entries = chunk.map((message, j) => ({
      Id: `m${i + j}`,
      MessageBody: JSON.stringify({ ...message, emailId: emailIds[i + j] }),
      MessageGroupId: "default",
      MessageDeduplicationId: emailIds[i + j],
    }));
    const res = await sqs.send(
      new SendMessageBatchCommand({ QueueUrl: QUEUE_URL, Entries: entries }),
    );
    if (res.Failed && res.Failed.length) {
      console.error("SQS batch partial failure", JSON.stringify(res.Failed));
      throw new Error("Failed to enqueue some emails");
    }
  }
}

// Tracking row written at enqueue time so GET /emails/{id} resolves immediately
// (status "queued"); the ingest Lambda later advances last_event on the same
// item. Conditional so a (timing-impossible) earlier event row is never
// clobbered. Best-effort: a failure here never fails the send.
async function writeTrackingRow(message, emailId) {
  if (!EVENTS_TABLE) return;
  const now = new Date().toISOString();
  const ttl = Math.floor(Date.now() / 1000) + TTL_DAYS * 86400;
  try {
    await ddb.send(
      new PutItemCommand({
        TableName: EVENTS_TABLE,
        ConditionExpression: "attribute_not_exists(pk)",
        Item: {
          pk: { S: `EMAIL#${emailId}` },
          sk: { S: "MAP" },
          object: { S: "email" },
          from: { S: message.from },
          to: sList(message.to),
          cc: sList(message.cc),
          bcc: sList(message.bcc),
          replyTo: sList(message.replyTo),
          subject: { S: message.subject },
          tags: { S: JSON.stringify(message.tags || []) },
          profile: { S: message.profile },
          createdAt: { S: now },
          ttl: { N: String(ttl) },
        },
      }),
    );
  } catch (err) {
    if (err?.name !== "ConditionalCheckFailedException") {
      console.error("Tracking row write failed", emailId, err?.name);
    }
  }
}

// GET /emails/{id}: shape the tracking row into the retrieve response.
// html/text are null (bodies are not persisted), scheduled_at is always null.
async function retrieve(id) {
  if (!EVENTS_TABLE) return fail(404, "not_found", `Email ${id} not found`);
  const res = await ddb.send(
    new GetItemCommand({
      TableName: EVENTS_TABLE,
      Key: { pk: { S: `EMAIL#${id}` }, sk: { S: "MAP" } },
    }),
  );
  const item = res.Item;
  if (!item) return fail(404, "not_found", `Email ${id} not found`);

  let tags = [];
  try {
    tags = JSON.parse(item.tags?.S || "[]");
  } catch {
    tags = [];
  }
  const lastEvent = item.lastEventType?.S
    ? LAST_EVENT[item.lastEventType.S] || "queued"
    : "queued";

  return json(200, {
    object: "email",
    id,
    to: fromList(item.to),
    from: item.from?.S || null,
    created_at: item.createdAt?.S || null,
    subject: item.subject?.S || null,
    html: null,
    text: null,
    bcc: fromList(item.bcc),
    cc: fromList(item.cc),
    reply_to: fromList(item.replyTo),
    last_event: lastEvent,
    scheduled_at: null,
    tags,
  });
}

exports.handler = async (event) => {
  const method = event.httpMethod || "";
  const resource = event.resource || event.requestContext?.resourcePath || "";

  const apiKey = getApiKey(event.headers);
  if (!apiKey) {
    return fail(
      401,
      "missing_api_key",
      "Missing API key. Send Authorization: Bearer <key> or x-api-key.",
    );
  }

  let config;
  try {
    config = await getConfig();
  } catch (err) {
    console.error("Failed to load config from SSM", err);
    return fail(500, "internal_error", "Configuration error");
  }

  const profile = findProfile(config.profiles, apiKey);
  if (!profile) return fail(401, "invalid_api_key", "Invalid API key");

  // Scheduling-only endpoints are intentionally unsupported by this gateway.
  if (method === "PATCH") {
    return fail(422, "not_supported", "Updating or rescheduling emails is not supported");
  }
  if (method === "POST" && resource.endsWith("/cancel")) {
    return fail(422, "not_supported", "Canceling scheduled emails is not supported");
  }

  if (method === "GET") {
    const id = event.pathParameters?.id;
    if (!id) return fail(400, "validation_error", "Missing email id");
    // Sentinel id handed out for never-sent fake emails: synthesize a terminal
    // status so polling clients resolve without special-casing it.
    if (id === FAKE_EMAIL_ID) {
      return json(200, {
        object: "email",
        id,
        to: [],
        from: null,
        created_at: null,
        subject: null,
        html: null,
        text: null,
        bcc: [],
        cc: [],
        reply_to: [],
        last_event: "delivered",
        scheduled_at: null,
        tags: [],
      });
    }
    try {
      return await retrieve(id);
    } catch (err) {
      console.error("Retrieve failed", err);
      return fail(500, "internal_error", "Failed to retrieve email");
    }
  }

  let body;
  try {
    body = JSON.parse(event.body || "{}");
  } catch {
    return fail(400, "validation_error", "Invalid JSON body");
  }

  const isBatch = resource.endsWith("/batch");

  try {
    if (isBatch) {
      const emails = Array.isArray(body)
        ? body
        : Array.isArray(body.emails)
          ? body.emails
          : null;
      if (!emails) return fail(422, "validation_error", "Expected an array of emails");
      if (emails.length === 0) return fail(422, "validation_error", "No emails provided");
      if (emails.length > MAX_BATCH) {
        return fail(422, "validation_error", `Too many emails (max ${MAX_BATCH} per request)`);
      }

      // One id per input email, in order; all-fake emails get the sentinel id
      // and are skipped, the rest enqueue normally.
      const ids = new Array(emails.length);
      const real = [];
      for (let i = 0; i < emails.length; i++) {
        const built = buildMessage(emails[i], profile, config.defaults);
        if (built.error) return fail(422, "validation_error", `emails[${i}]: ${built.error}`);
        if (built.fake) {
          ids[i] = FAKE_EMAIL_ID;
          continue;
        }
        ids[i] = crypto.randomUUID();
        real.push({ message: built.message, id: ids[i] });
      }
      await enqueueBatch(real.map((r) => r.message), real.map((r) => r.id));
      await Promise.all(real.map((r) => writeTrackingRow(r.message, r.id)));
      return json(200, { data: ids.map((id) => ({ id })) });
    }

    const built = buildMessage(body, profile, config.defaults);
    if (built.error) return fail(422, "validation_error", built.error);
    if (built.fake) return json(200, { id: FAKE_EMAIL_ID });
    const id = crypto.randomUUID();
    await enqueueOne(built.message, id);
    await writeTrackingRow(built.message, id);
    return json(200, { id });
  } catch (err) {
    console.error("Failed to enqueue email(s)", err);
    return fail(500, "internal_error", "Failed to queue email");
  }
};
