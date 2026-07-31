import * as path from "node:path";
import { Duration, Stack } from "aws-cdk-lib";
import { LambdaIntegration, RestApi } from "aws-cdk-lib/aws-apigateway";
import { Table } from "aws-cdk-lib/aws-dynamodb";
import { PolicyStatement } from "aws-cdk-lib/aws-iam";
import {
  Code,
  Function as LambdaFunction,
  Runtime,
} from "aws-cdk-lib/aws-lambda";
import { SqsEventSource } from "aws-cdk-lib/aws-lambda-event-sources";
import { Queue } from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";

export interface MailerApiProps {
  api: RestApi;
  eventsTable: Table;
}

// Throttled email gateway, attached to the shared API, exposed as a drop-in
// /emails API (POST /emails, POST /emails/batch, GET /emails/{id}, plus PATCH +
// /cancel stubs) matching the common transactional-email API shape. The producer
// Lambda authenticates the caller via
// `Authorization: Bearer <key>` or `x-api-key` against a per-profile key in SSM
// (/itzbase/profiles/<slug>/api-key), applies the account-wide SES defaults
// (/itzbase/ses/default-*), enqueues each email onto a FIFO queue, and writes an
// EMAIL#<id> tracking row to the events table so GET resolves immediately. The
// sender Lambda paces sends to the account's SES max send rate, so the gateway
// can never exceed SES limits. The SES config set written by the stack routes
// events into the ingest pipeline, which advances each row's last_event.
export class MailerApi extends Construct {
  constructor(scope: Construct, id: string, props: MailerApiProps) {
    super(scope, id);
    const { api, eventsTable } = props;
    const stack = Stack.of(this);

    const dlq = new Queue(this, "Dlq", {
      fifo: true,
      retentionPeriod: Duration.days(14),
    });

    const queue = new Queue(this, "Queue", {
      fifo: true,
      visibilityTimeout: Duration.seconds(180),
      deadLetterQueue: { queue: dlq, maxReceiveCount: 5 },
    });

    const sender = new LambdaFunction(this, "Sender", {
      description:
        "ITzBase: consumes the mailer queue and sends through SES, paced to the account send rate",
      runtime: Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: Code.fromAsset(
        path.join(__dirname, "..", "..", "lambda", "mailer-sender"),
      ),
      timeout: Duration.seconds(60),
      // No reserved concurrency (it can exceed a freshly-vended account's Lambda
      // limit and fail the deploy). The FIFO queue uses a single message group,
      // so SQS processes one batch at a time = serial; combined with in-handler
      // pacing to the SES max send rate, the gateway never exceeds SES limits.
    });
    // SES has no L2 grant helper; scope to the send + account-read actions.
    sender.addToRolePolicy(
      new PolicyStatement({
        actions: ["ses:SendEmail", "ses:SendRawEmail", "ses:GetAccount"],
        resources: ["*"],
      }),
    );
    sender.addToRolePolicy(
      new PolicyStatement({
        actions: ["ssm:GetParameter"],
        resources: [
          stack.formatArn({
            service: "ssm",
            resource: "parameter",
            resourceName: "itzbase/ses/configuration-set",
          }),
        ],
      }),
    );
    sender.addEventSource(
      new SqsEventSource(queue, {
        batchSize: 10,
        reportBatchItemFailures: true,
      }),
    );

    const producer = new LambdaFunction(this, "Producer", {
      description:
        "ITzBase: /emails gateway, authenticates profile keys and enqueues sends",
      runtime: Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: Code.fromAsset(
        path.join(__dirname, "..", "..", "lambda", "mailer-api"),
      ),
      environment: {
        QUEUE_URL: queue.queueUrl,
        EVENTS_TABLE_NAME: eventsTable.tableName,
      },
      timeout: Duration.seconds(30),
    });
    queue.grantSendMessages(producer);
    // Tracking row write (PutItem) + retrieve (GetItem) on the email-event table.
    producer.addToRolePolicy(
      new PolicyStatement({
        actions: ["dynamodb:PutItem", "dynamodb:GetItem"],
        resources: [eventsTable.tableArn],
      }),
    );
    // One recursive read of /itzbase/ covers the profile keys and the
    // account-wide SES defaults.
    producer.addToRolePolicy(
      new PolicyStatement({
        actions: ["ssm:GetParametersByPath"],
        resources: [
          stack.formatArn({
            service: "ssm",
            resource: "parameter",
            resourceName: "itzbase",
          }),
          stack.formatArn({
            service: "ssm",
            resource: "parameter",
            resourceName: "itzbase/*",
          }),
        ],
      }),
    );
    // The profile api-key is a SecureString param; decrypting it on read needs
    // kms:Decrypt on the account's default SSM key, scoped via the SSM service.
    producer.addToRolePolicy(
      new PolicyStatement({
        actions: ["kms:Decrypt"],
        resources: ["*"],
        conditions: {
          StringEquals: { "kms:ViaService": `ssm.${stack.region}.amazonaws.com` },
        },
      }),
    );

    // The /emails surface, all served by the producer Lambda.
    const integration = new LambdaIntegration(producer);
    const emails = api.root.addResource("emails");
    emails.addMethod("POST", integration); // send one    -> { id }
    emails.addResource("batch").addMethod("POST", integration); // send many -> { data }
    const emailById = emails.addResource("{id}");
    emailById.addMethod("GET", integration); // retrieve -> { object, id, last_event, ... }
    emailById.addMethod("PATCH", integration); // stub: scheduling not supported
    emailById.addResource("cancel").addMethod("POST", integration); // stub: scheduling not supported
  }
}
