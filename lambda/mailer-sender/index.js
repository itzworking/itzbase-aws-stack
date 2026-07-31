const {
  SESv2Client,
  SendEmailCommand,
  GetAccountCommand,
} = require("@aws-sdk/client-sesv2");
const { SSMClient, GetParameterCommand } = require("@aws-sdk/client-ssm");

// Plain JS so it needs no bundling; the Lambda Node 22 runtime ships AWS SDK v3.
// Consumer side of the throttled mailer: SQS (FIFO, single message group)
// delivers queued emails one batch at a time and this Lambda sends each through
// SES, pacing to the account's SES max send rate so the gateway can never exceed
// the per-second limit. The SES configuration set (written to SSM by the stack)
// routes send/bounce/open/click events into the event pipeline.
const ses = new SESv2Client({});
const ssm = new SSMClient({});

const CONFIG_SET_PARAM = "/itzbase/ses/configuration-set";

let configSetCache; // string | null once resolved
let sendRateCache; // number once resolved

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function getConfigSet() {
  if (configSetCache !== undefined) return configSetCache;
  try {
    const res = await ssm.send(new GetParameterCommand({ Name: CONFIG_SET_PARAM }));
    configSetCache = res.Parameter?.Value || null;
  } catch (err) {
    console.warn("No SES configuration set in SSM, sending without one", err?.name);
    configSetCache = null;
  }
  return configSetCache;
}

async function getSendRate() {
  if (sendRateCache !== undefined) return sendRateCache;
  try {
    const res = await ses.send(new GetAccountCommand({}));
    const rate = res.SendQuota?.MaxSendRate;
    sendRateCache = rate && rate > 0 ? rate : 1;
  } catch (err) {
    console.warn("Could not read SES send rate, defaulting to 1/s", err?.name);
    sendRateCache = 1;
  }
  return sendRateCache;
}

async function sendOne(message, configSet) {
  const destination = { ToAddresses: message.to };
  if (message.cc?.length) destination.CcAddresses = message.cc;
  if (message.bcc?.length) destination.BccAddresses = message.bcc;

  const bodyContent = {};
  if (message.html) bodyContent.Html = { Data: message.html, Charset: "UTF-8" };
  if (message.text) bodyContent.Text = { Data: message.text, Charset: "UTF-8" };

  // Stamp our emailId as a message tag so it rides into every SES event
  // notification (mail.tags), letting the ingest Lambda map the caller's id to
  // the SES message id and the resulting send/bounce/open/click events.
  const res = await ses.send(
    new SendEmailCommand({
      FromEmailAddress: message.from,
      Destination: destination,
      ...(message.replyTo?.length ? { ReplyToAddresses: message.replyTo } : {}),
      ...(configSet ? { ConfigurationSetName: configSet } : {}),
      ...(message.emailId
        ? { EmailTags: [{ Name: "itz-email-id", Value: message.emailId }] }
        : {}),
      Content: {
        Simple: {
          Subject: { Data: message.subject, Charset: "UTF-8" },
          Body: bodyContent,
        },
      },
    }),
  );
  return res.MessageId;
}

exports.handler = async (event) => {
  const configSet = await getConfigSet();
  const rate = await getSendRate();
  const intervalMs = Math.ceil(1000 / rate);
  const batchItemFailures = [];

  const records = event.Records || [];
  for (let i = 0; i < records.length; i++) {
    const record = records[i];

    let message;
    try {
      message = JSON.parse(record.body);
    } catch {
      // Unparseable payload: drop it rather than retry forever (poison message).
      console.error("Dropping unparseable message", record.messageId);
      continue;
    }

    try {
      const sesMessageId = await sendOne(message, configSet);
      console.log("sent", {
        emailId: message.emailId,
        sesMessageId,
        profile: message.profile,
        to: message.to,
      });
    } catch (err) {
      console.error("SES send failed", {
        messageId: record.messageId,
        error: err?.message,
      });
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }

    // Pace to the SES max send rate; no delay after the final record.
    if (i < records.length - 1) await sleep(intervalMs);
  }

  return { batchItemFailures };
};
