import * as path from "node:path";
import {
  CfnOutput,
  Duration,
  RemovalPolicy,
  Stack,
  StackProps,
  Tags,
} from "aws-cdk-lib";
import { Cors, RestApi } from "aws-cdk-lib/aws-apigateway";
import { AttributeType, BillingMode, Table } from "aws-cdk-lib/aws-dynamodb";
import {
  Code,
  Function as LambdaFunction,
  Runtime,
} from "aws-cdk-lib/aws-lambda";
import {
  BlockPublicAccess,
  Bucket,
  BucketEncryption,
  HttpMethods,
} from "aws-cdk-lib/aws-s3";
import {
  ConfigurationSet,
  ConfigurationSetTlsPolicy,
  EmailSendingEvent,
  EventDestination,
} from "aws-cdk-lib/aws-ses";
import { Topic } from "aws-cdk-lib/aws-sns";
import { LambdaSubscription } from "aws-cdk-lib/aws-sns-subscriptions";
import { StringParameter } from "aws-cdk-lib/aws-ssm";
import { Construct } from "constructs";
import { MailerApi } from "./constructs/mailer-api";

// The ITzBase account baseline: everything a managed member account needs to
// run its applications. S3 storage, the SES email-event pipeline, and the
// shared REST API hosting the mailer gateway. Dynamic per-app pieces (SES
// identities, DNS verification, per-app IAM users, mailer profiles) are
// managed by the operator at runtime, not here.
export class ITzBaseStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, {
      ...props,
      description:
        "ITzBase account baseline: S3 storage, SES email pipeline, mailer API",
    });

    // --- Storage ---------------------------------------------------------
    // One private bucket per account. App isolation is by key prefix: the
    // operator mints one prefix-scoped IAM user per app. No bucketName:
    // CloudFormation generates a unique one (itzbase-...), consumers read it
    // from the StorageBucketName output.
    const bucket = new Bucket(this, "Bucket", {
      blockPublicAccess: BlockPublicAccess.BLOCK_ALL,
      encryption: BucketEncryption.S3_MANAGED,
      versioned: false,
      enforceSSL: true,
      removalPolicy: RemovalPolicy.RETAIN,
      // Apps upload from the browser via presigned PUT, so cross-origin
      // requests must be allowed. Presigned auth is the security boundary,
      // not CORS: the bucket stays private.
      cors: [
        {
          allowedOrigins: ["*"],
          allowedMethods: [HttpMethods.GET, HttpMethods.PUT, HttpMethods.HEAD],
          allowedHeaders: ["*"],
        },
      ],
    });
    Tags.of(bucket).add(
      "itzbase:purpose",
      "Account storage bucket. App isolation by key prefix",
    );

    // --- Email-event pipeline --------------------------------------------
    // Config set -> SNS -> ingest Lambda -> in-account DynamoDB. All email
    // tracking data stays inside this account.
    const events = new Table(this, "EmailEvents", {
      partitionKey: { name: "pk", type: AttributeType.STRING },
      sortKey: { name: "sk", type: AttributeType.STRING },
      billingMode: BillingMode.PAY_PER_REQUEST,
      timeToLiveAttribute: "ttl",
      removalPolicy: RemovalPolicy.RETAIN,
    });
    events.addGlobalSecondaryIndex({
      indexName: "by-message",
      partitionKey: { name: "gsi1pk", type: AttributeType.STRING },
      sortKey: { name: "gsi1sk", type: AttributeType.STRING },
    });
    Tags.of(events).add(
      "itzbase:purpose",
      "Email events and per-email tracking rows for the mailer pipeline",
    );

    const ingest = new LambdaFunction(this, "SesIngest", {
      description:
        "ITzBase: writes SES events from SNS into the email-events table",
      runtime: Runtime.NODEJS_22_X,
      handler: "index.handler",
      code: Code.fromAsset(path.join(__dirname, "..", "lambda", "ses-ingest")),
      environment: { TABLE_NAME: events.tableName },
      timeout: Duration.seconds(30),
    });
    events.grantWriteData(ingest);

    const topic = new Topic(this, "SesEvents", {
      displayName: "ITzBase SES events",
    });
    topic.addSubscription(new LambdaSubscription(ingest));

    const configSet = new ConfigurationSet(this, "ConfigurationSet", {
      tlsPolicy: ConfigurationSetTlsPolicy.REQUIRE,
      reputationMetrics: true,
    });
    configSet.addEventDestination("Events", {
      destination: EventDestination.snsTopic(topic),
      events: [
        EmailSendingEvent.SEND,
        EmailSendingEvent.DELIVERY,
        EmailSendingEvent.BOUNCE,
        EmailSendingEvent.COMPLAINT,
        EmailSendingEvent.REJECT,
        EmailSendingEvent.OPEN,
        EmailSendingEvent.CLICK,
        EmailSendingEvent.RENDERING_FAILURE,
        EmailSendingEvent.DELIVERY_DELAY,
      ],
    });

    // The mailer's sender Lambda reads this at runtime to stamp every send
    // with the config set, so all sends flow into the event pipeline.
    new StringParameter(this, "ConfigurationSetParam", {
      parameterName: "/itzbase/ses/configuration-set",
      stringValue: configSet.configurationSetName,
      description: "SES configuration set name, read by the ITzBase mailer",
    });

    // --- Shared REST API --------------------------------------------------
    // One API Gateway hosts every ITzBase endpoint; new features attach their
    // own resources to it as constructs (see MailerApi) rather than standing
    // up another API Gateway.
    const api = new RestApi(this, "Api", {
      description: "ITzBase shared REST API for the member account",
      deployOptions: { stageName: "prod" },
      defaultCorsPreflightOptions: {
        allowOrigins: Cors.ALL_ORIGINS,
        allowMethods: ["GET", "POST", "PATCH", "OPTIONS"],
        allowHeaders: ["Content-Type", "x-api-key", "Authorization"],
      },
    });

    new MailerApi(this, "Mailer", { api, eventsTable: events });

    // --- Outputs ----------------------------------------------------------
    // Output keys are a consumed contract: the operator resolves the bucket,
    // config set, and API URL from them at runtime. Never rename them.
    new CfnOutput(this, "StorageBucketName", { value: bucket.bucketName });
    new CfnOutput(this, "SesConfigurationSetName", {
      value: configSet.configurationSetName,
    });
    new CfnOutput(this, "EmailEventsTableName", { value: events.tableName });
    new CfnOutput(this, "ApiUrl", { value: api.url });
  }
}
