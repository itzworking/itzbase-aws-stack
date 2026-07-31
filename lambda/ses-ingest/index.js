const {
  DynamoDBClient,
  PutItemCommand,
  UpdateItemCommand,
} = require("@aws-sdk/client-dynamodb");

// Plain JS so it needs no bundling; the Lambda Node 22 runtime ships AWS SDK v3.
const ddb = new DynamoDBClient({});
const TABLE = process.env.TABLE_NAME;
const TTL_DAYS = 90;

// Each SNS record carries one SES event. Store the raw event (queryable per day
// and per message via the GSI) and bump a per-day per-type rollup counter.
exports.handler = async (event) => {
  for (const record of event.Records || []) {
    const sns = record.Sns;
    if (!sns || !sns.Message) continue;

    let msg;
    try {
      msg = JSON.parse(sns.Message);
    } catch {
      continue;
    }

    const eventType = msg.eventType || msg.notificationType;
    if (!eventType) continue;

    const mail = msg.mail || {};
    const messageId = mail.messageId || "unknown";
    const ts = mail.timestamp || new Date().toISOString();
    const day = ts.slice(0, 10);
    const ttl = Math.floor(Date.now() / 1000) + TTL_DAYS * 86400;
    const counter = eventType.replace(/[^A-Za-z0-9]/g, "") || "Unknown";

    // The mailer stamps the caller-facing emailId as the `itz-email-id` message
    // tag; SES echoes tags back as { name: [values] } on every event.
    const tags = mail.tags || {};
    const emailId = Array.isArray(tags["itz-email-id"])
      ? tags["itz-email-id"][0]
      : undefined;

    await ddb.send(
      new PutItemCommand({
        TableName: TABLE,
        Item: {
          pk: { S: `EVT#${day}` },
          sk: { S: `${ts}#${messageId}#${counter}` },
          gsi1pk: { S: `MSG#${messageId}` },
          gsi1sk: { S: `${ts}#${counter}` },
          eventType: { S: eventType },
          messageId: { S: messageId },
          ...(emailId ? { emailId: { S: emailId } } : {}),
          timestamp: { S: ts },
          raw: { S: JSON.stringify(msg).slice(0, 380000) },
          ttl: { N: String(ttl) },
        },
      }),
    );

    await ddb.send(
      new UpdateItemCommand({
        TableName: TABLE,
        Key: { pk: { S: `ROLLUP#${day}` }, sk: { S: "TOTAL" } },
        UpdateExpression: "ADD #c :one",
        ExpressionAttributeNames: { "#c": counter },
        ExpressionAttributeValues: { ":one": { N: "1" } },
      }),
    );

    // Internal mapping: the caller only knows the emailId, so keep an item that
    // resolves it to the SES messageId (query the by-message GSI for the full
    // timeline) and the most recent event seen for it. Idempotent per event.
    if (emailId) {
      await ddb.send(
        new UpdateItemCommand({
          TableName: TABLE,
          Key: { pk: { S: `EMAIL#${emailId}` }, sk: { S: "MAP" } },
          UpdateExpression:
            "SET sesMessageId = :mid, lastEventType = :evt, lastEventAt = :ts, #ttl = :ttl",
          ExpressionAttributeNames: { "#ttl": "ttl" },
          ExpressionAttributeValues: {
            ":mid": { S: messageId },
            ":evt": { S: eventType },
            ":ts": { S: ts },
            ":ttl": { N: String(ttl) },
          },
        }),
      );
    }
  }

  return { ok: true };
};
