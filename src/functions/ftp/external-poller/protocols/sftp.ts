import sftp from "ssh2-sftp-client";
import path from "path";
import { serializeError } from "serialize-error";

import { PutObjectCommand } from "@stedi/sdk-client-buckets";

import { bucketClient } from "../../../../lib/buckets.js";
import { requiredEnvVar } from "../../../../lib/environment.js";
import { FtpPollerConfig } from "../../../../lib/types/FtpPollerConfig.js";
import {
    FtpPollingResults,
    ProcessingError,
    RemoteSftpFileDetails,
    SkippedItem,
} from "../types";

const destinationBucketName = requiredEnvVar("SFTP_BUCKET_NAME");
const sftpClient = new sftp();

export const pollSftp = async (
    ftpConfig: FtpPollerConfig
): Promise<FtpPollingResults> => {
    await sftpClient.connect(ftpConfig.connectionDetails.config);

    const fileDetails =
        ftpConfig.remoteFiles && ftpConfig.remoteFiles.length > 0
            ? await getSpecifiedFileDetails(
                ftpConfig.remotePath,
                ftpConfig.remoteFiles
            )
            : await getAllFileDetailsForPath(ftpConfig.remotePath);

    const sftpPollingResults: FtpPollingResults = {
        processedFiles: [],
        skippedItems: fileDetails.skippedItems || [],
        processingErrors: fileDetails.processingErrors || [],
    };

    for await (const file of fileDetails.filesToProcess) {
        const remoteFilePath = path.normalize(
            `${ftpConfig.remotePath}/${file.name}`
        );

        // if last poll time is not set, use `0` (epoch)
        // if remote file modifiedAt is not set, use current time
        const lastPollTimestamp = ftpConfig.lastPollTime?.getTime() || 0;
        const remoteFileTimestamp = file.modifyTime || Date.now();
        if (remoteFileTimestamp < lastPollTimestamp) {
            sftpPollingResults.skippedItems.push(<SkippedItem>{
                path: remoteFilePath,
                reason: `remote timestamp (${remoteFileTimestamp}) is not newer than last poll timestamp (${lastPollTimestamp})`,
            });
            break;
        }

        try {
            const fileContents = await sftpClient.get(remoteFilePath);

            const destinationKey = `${ftpConfig.destinationPath}/${file.name}`;
            await bucketClient().send(
                new PutObjectCommand({
                    bucketName: destinationBucketName,
                    key: destinationKey,
                    body: fileContents,
                })
            );

            // clean up temporary local file, and optionally remote file on ftp server
            ftpConfig.deleteAfterProcessing && (await sftpClient.delete(remoteFilePath));

            sftpPollingResults.processedFiles.push(remoteFilePath);
        } catch (e) {
            const errorMessage =
                e instanceof Error
                    ? e.message
                    : serializeError(e).message ?? "_unknown_";

            sftpPollingResults.processingErrors.push({
                path: remoteFilePath,
                errorMessage,
            });
        }
    }

    await sftpClient.end();

    return sftpPollingResults;
}

const getSpecifiedFileDetails = async (
    remotePath: string,
    remoteFiles: string[]
): Promise<RemoteSftpFileDetails> => {
    const filesToProcess: sftp.FileInfo[] = [];
    const processingErrors: ProcessingError[] = [];

    for await (const file of remoteFiles) {
        const remoteFilePath = path.normalize(`${remotePath}/${file}`);
        const listResult: sftp.FileInfo[] = await sftpClient.list(remoteFilePath);
        if (listResult.length !== 1) {
            processingErrors.push({
                path: remoteFilePath,
                errorMessage: `expected exactly one match for list of single file ${remoteFilePath}`,
            });
            break;
        }

        listResult[0].type === "-"
            ? filesToProcess.push(listResult[0])
            : // handle non-file as processing error since file was specifically requested
            processingErrors.push({
                path: remoteFilePath,
                errorMessage: `requested remote file ${file}, but it is not a file`,
            });
    }

    return {
        filesToProcess,
        processingErrors,
    };
};

const getAllFileDetailsForPath = async (
    remotePath: string
): Promise<RemoteSftpFileDetails> => {
    const directoryContents: sftp.FileInfo[] = await sftpClient.list(remotePath);
    return directoryContents.reduce(
        (remoteSftpFileDetails: RemoteSftpFileDetails, currentFile) => {
            currentFile.type === "-"
                ? remoteSftpFileDetails.filesToProcess.push(currentFile)
                : remoteSftpFileDetails.skippedItems?.push({
                    path: path.normalize(`${remotePath}/${currentFile.name}`),
                    reason: "not a file",
                });

            return remoteSftpFileDetails;
        },
        { filesToProcess: [], skippedItems: [] }
    );
};
