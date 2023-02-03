import {
  GetValueCommand,
  SetValueCommand,
  StashClient,
} from "@stedi/sdk-client-stash";
import { serializeError } from "serialize-error";

import { requiredEnvVar } from "../../../lib/environment.js";
import { PARTNERS_KEYSPACE_NAME } from "../../../lib/constants.js";
import {
  failedExecution, FailureResponse,
  generateExecutionId,
  markExecutionAsSuccessful,
  recordNewExecution
} from "../../../lib/execution.js";
import {
  FtpPollerConfig,
  FtpPollerConfigMap,
} from "../../../lib/types/FtpPollerConfig.js";
import { FtpPollingResults } from "./types.js";
import { pollFtp } from "./protocols/ftp.js";
import { pollSftp } from "./protocols/sftp.js";

const keyspaceName = PARTNERS_KEYSPACE_NAME;
const ftpConfigStashKey = "bootstrap|ftp-poller-config";

const stashClient = new StashClient({
  region: "us",
  apiKey: requiredEnvVar("STEDI_API_KEY"),
});

const pollerHandlerMap: {
  [index: string]: (ftpConfig: FtpPollerConfig) => Promise<FtpPollingResults>;
} = {
  ftp: pollFtp,
  sftp: pollSftp,
};

export const handler = async (
  event: any
): Promise<FtpPollingResults | FailureResponse> => {
  const ftpConfigId = event.ftpConfigId;
  const executionTime = new Date().toISOString();
  const executionId = generateExecutionId({ executionTime });

  await recordNewExecution(executionId, { executionTime });
  await console.log("starting", {
    executionId,
    payload: JSON.stringify({ executionTime }),
  });

  try {
    const stashResponse = await stashClient.send(
      new GetValueCommand({
        keyspaceName,
        key: ftpConfigStashKey,
      })
    );

    // `FtpPollerConfigMap.parse` handles failed stash lookup as well (value is undefined)
    const ftpConfigMap: FtpPollerConfigMap = FtpPollerConfigMap.parse(
      stashResponse.value
    );
    const ftpConfig: FtpPollerConfig = ftpConfigMap[ftpConfigId];

    if (!ftpConfig) {
      return failedExecution(
        executionId,
        new Error(`config not found for key: ${ftpConfigId}`)
      );
    }

    if (
      !Object.keys(pollerHandlerMap).includes(
        ftpConfig.connectionDetails.protocol
      )
    ) {
      const error = new Error(
        `unsupported connection protocol: ${ftpConfig.connectionDetails.protocol}`
      );
      return failedExecution(executionId, error);
    }

    console.log("polling ftp", {
      executionId,
      payload: JSON.stringify({
        ftpConfigKey: ftpConfigId,
        hostname: ftpConfig.connectionDetails.config.host,
        remotePath: ftpConfig.remotePath,
      }),
    });

    const results = await pollerHandlerMap[
      ftpConfig.connectionDetails.protocol
      ](ftpConfig);

    if (results.processingErrors.length > 0) {
      return failedExecution(
        executionId,
        new Error("at least one processing error encountered during polling"),
        results.processingErrors
      );
    }

    // update `lastPollTime` for this ftp config
    ftpConfig.lastPollTime = new Date();
    const value = {
      ...(ftpConfigMap as object),
      [ftpConfigId]: { ...(ftpConfig as object) },
    };

    await stashClient.send(
      new SetValueCommand({
        keyspaceName,
        key: ftpConfigStashKey,
        value,
      })
    );

    await markExecutionAsSuccessful(executionId);
    return results;
  } catch (e) {
    const error =
      e instanceof Error ? e : new Error(`unknown error: ${serializeError(e)}`);
    return failedExecution(executionId, error);
  }
};
