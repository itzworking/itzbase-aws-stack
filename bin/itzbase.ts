#!/usr/bin/env node
import * as cdk from "aws-cdk-lib";
import { ITzBaseStack } from "../lib/itzbase-stack";

const app = new cdk.App();

// Account targeting is by credentials, never by parameters or context: the
// stack deploys identically into every account.
const env = {
  account: process.env.CDK_DEPLOY_ACCOUNT || process.env.CDK_DEFAULT_ACCOUNT,
  region: process.env.CDK_DEPLOY_REGION || process.env.CDK_DEFAULT_REGION,
};

new ITzBaseStack(app, "ITzBase", { env });
