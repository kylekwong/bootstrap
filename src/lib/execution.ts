import hash from "object-hash";
import { TextEncoder } from "util";
import { ErrorObject, serializeError } from "serialize-error";

import {
  BucketsClient,
  DeleteObjectCommand,
  PutObjectCommand,
  ReadBucketCommand,
} from "@stedi/sdk-client-buckets";

import { bucketClient } from "./buckets.js";
import { requiredEnvVar } from "./environment.js";
import { trackProgress } from "./progressTracking.js";

const bucketName = requiredEnvVar("EXECUTIONS_BUCKET_NAME");

let _executionsBucketClient: BucketsClient;
let _infiniteLoopCheckPassed: boolean = false;

export type FailureRecord = { bucketName?: string, key: string };
export type FailureResponse = { statusCode: number, message: string, failureRecord: FailureRecord, error: ErrorObject }

export const recordNewExecution = async (executionId: string, input: any) => {
  const client = await executionsBucketClient();
  const result = await client.send(
    new PutObjectCommand({
      bucketName,
      key: `functions/${functionName()}/${executionId}/input.json`,
      body: new TextEncoder().encode(JSON.stringify(input)),
    })
  );
  if (result)
    console.log({ action: "recordNewExecution", executionId, result });
};

export const markExecutionAsSuccessful = async (executionId: string) => {
  const client = await executionsBucketClient();
  const inputResult = await client.send(
    new DeleteObjectCommand({
      bucketName,
      key: `functions/${functionName()}/${executionId}/input.json`,
    })
  );

  if (inputResult)
    console.log({
      action: "markExecutionAsSuccessful",
      executionId,
      inputResult,
    });

  // async invokes automatically retries on failure, so
  // we should attempt to cleanup any leftover failure results
  // as this might be a later retry invoke
  const previousFailure = await client.send(
    new DeleteObjectCommand({
      bucketName,
      key: `functions/${functionName()}/${executionId}/failure.json`,
    })
  );

  if (previousFailure)
    console.log({
      action: "markExecutionAsSuccessful",
      executionId,
      previousFailure,
    });
  return { inputResult, previousFailure };
};

export const failedExecution = async (executionId: string, error: Error, context?: any): Promise<FailureResponse> => {
  const rawError = serializeError(error)
  const failureRecord = await markExecutionAsFailed(executionId, rawError, context);
  const statusCode = (error as any)?.["$metadata"]?.httpStatusCode || 500;
  const message = "execution failed";
  await trackProgress(message, { error: rawError });
  return { statusCode, message, failureRecord, error: rawError }
}

const markExecutionAsFailed = async (
  executionId: string,
  error: ErrorObject,
  context?: any,
): Promise<FailureRecord> => {
  const client = await executionsBucketClient();
  const key = `functions/${functionName()}/${executionId}/failure.json`;
  const result = await client.send(
    new PutObjectCommand({
      bucketName,
      key,
      body: new TextEncoder().encode(JSON.stringify({ error, context })),
    })
  );

  if (result)
    console.log({ action: "markExecutionAsFailed", executionId, result });

  return { bucketName, key };
};

export const generateExecutionId = (event: any) => hash({
  functionName: functionName(),
  event,
});

export const functionName = () => requiredEnvVar("STEDI_FUNCTION_NAME");

const executionsBucketClient = async (): Promise<BucketsClient> => {
  if(_executionsBucketClient === undefined) {
    _executionsBucketClient = bucketClient();
  }

  if (!_infiniteLoopCheckPassed) {
    // guard against infinite Function execution loops
    const executionsBucket = await  _executionsBucketClient.send(new ReadBucketCommand({ bucketName }));
    if (executionsBucket.notifications?.functions?.some((fn) => fn.functionName === functionName())) {
      throw new Error("Error: executions bucket has recursive notifications configured")
    }
    _infiniteLoopCheckPassed = true;
  }

  return _executionsBucketClient;
}