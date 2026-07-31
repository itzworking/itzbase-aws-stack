# ITzBase

The baseline AWS stack [ITzWorking](https://www.itzworking.io) deploys into
every AWS member account it manages for its clients. Used in production by
ITzWorking; published for transparency. No support commitment, no roadmap.

One CDK app, one stack, `ITzBase`. No parameters, no context: the stack
deploys identically into every account, and account targeting is by
credentials only.

## What it deploys

- **Storage**: one private S3 bucket per account (SSE, TLS enforced, public
  access blocked). App isolation is by key prefix; the operator mints one
  prefix-scoped IAM user per app outside this stack.
- **Email-event pipeline**: an SES configuration set (TLS required,
  reputation metrics) routing send/delivery/bounce/complaint/open/click
  events through SNS into an ingest Lambda and an in-account DynamoDB table.
  Email tracking data never leaves the account.
- **Mailer API**: a shared API Gateway REST API hosting a throttled email
  gateway, exposed as a drop-in `/emails` API (the common
  transactional-email API shape) so an application can swap its email
  provider for this service with only a base-URL change.

  | Method | Path | Behaviour |
  |---|---|---|
  | `POST` | `/emails` | Send one. Returns `200 { id }`. |
  | `POST` | `/emails/batch` | Send up to 100 (bare array or `{ emails }`). Returns `200 { data: [{ id }] }`. |
  | `GET` | `/emails/{id}` | Retrieve. Returns `200 { object, id, to, from, subject, last_event, ... }` (`html`/`text` are `null`; bodies are not persisted). |
  | `PATCH` | `/emails/{id}` | Stub: `422 not_supported` (no scheduling). |
  | `POST` | `/emails/{id}/cancel` | Stub: `422 not_supported` (no scheduling). |

  Auth is `Authorization: Bearer <key>` or `x-api-key`, matched against
  per-profile keys in SSM (`/itzbase/profiles/<slug>/api-key`). Sends are
  paced to the account's SES max send rate through a FIFO queue, so the
  gateway can never exceed SES limits. Not supported: `scheduled_at`,
  `attachments` (rejected with `422 validation_error`), custom `headers`
  (accepted but ignored).

## The contract

Two integration surfaces, both stable:

- **Stack outputs** `StorageBucketName`, `SesConfigurationSetName`,
  `EmailEventsTableName`, `ApiUrl`: how the operator (or you) resolves the
  deployed resources.
- **The SSM namespace `/itzbase/`**: the stack writes
  `ses/configuration-set`; the operator writes `profiles/<slug>/api-key`
  (SecureString), `ses/default-sender`, `ses/default-reply-to`; the mailer
  reads the whole namespace at request time.

## Reversibility

There is no lock-in in this stack, by design. Everything runs inside your
own AWS account and the Lambda source is deployed unbundled and readable in
your own console. If the engagement with ITzWorking ends:

1. The account is yours. The stack, the bucket, the email pipeline, and all
   data in them stay in your account and keep running unchanged.
2. Remove ITzWorking's management access (the OIDC deploy role and any
   operator-created IAM users you no longer want) and the stack is fully
   self-contained.
3. Mailer profiles are plain SSM parameters under `/itzbase/profiles/`; you
   can create, rotate, or delete keys yourself with `aws ssm put-parameter`.
4. To walk away from the stack entirely, empty the bucket, then
   `cdk destroy ITzBase` (the bucket and events table are `RETAIN`ed, so
   deleting them is always an explicit act, never a side effect).

## Local

```
npm install
npm run build   # tsc
npm run synth   # cdk synth
```

Deploying requires credentials for the target account:

```
npx cdk deploy ITzBase
```

## License

Apache-2.0. See `LICENSE` and `NOTICE`.
