import { SetValueCommand } from "@stedi/sdk-client-stash";
import { PARTNERS_KEYSPACE_NAME } from "../../lib/constants.js";
import { requiredEnvVar } from "../../lib/environment.js";
import { Partnership } from "../../lib/types/PartnerRouting.js";
import { stashClient as buildStashClient } from "../../lib/stash.js";
import { savePartnership } from "../../lib/savePartnership.js";

type CreateSampleStashRecordsInput = {
  guide850: string;
  guide855: string;
};

export const createSampleStashRecords = async ({
  guide850,
  guide855,
}: CreateSampleStashRecordsInput) => {
  const stashClient = buildStashClient();

  const partnership: Partnership = {
    transactionSets: [],
  };

  // outbound 850 from THISISME to ANOTHERMERCH
  partnership.transactionSets.push({
    description: "Purchase Orders sent to ANOTHERMERCH",
    guideId: guide850,
    destinations: [
      {
        destination: {
          type: "bucket",
          bucketName: requiredEnvVar("SFTP_BUCKET_NAME"),
          path: "trading_partners/ANOTHERMERCH/outbound",
        },
      },
    ],
    receivingPartnerId: "another-merchant",
    sendingPartnerId: "this-is-me",
    usageIndicatorCode: "T",
  });

  // inbound 855 from ANOTHERMERCH to THISISME
  partnership.transactionSets.push({
    description: "Purchase Order Acknowledgements received from ANOTHERMERCH",
    guideId: guide855,
    destinations: [
      {
        destination: {
          type: "webhook",
          url: requiredEnvVar("DESTINATION_WEBHOOK_URL"),
        },
      },
    ],
    receivingPartnerId: "this-is-me",
    sendingPartnerId: "another-merchant",
    usageIndicatorCode: "T",
  });

  // write to Stash
  await savePartnership("partnership|this-is-me|another-merchant", partnership);

  await stashClient.send(
    new SetValueCommand({
      keyspaceName: PARTNERS_KEYSPACE_NAME,
      key: `lookup|ISA|14/ANOTHERMERCH`,
      value: {
        partnerId: "another-merchant",
      },
    })
  );
  await stashClient.send(
    new SetValueCommand({
      keyspaceName: PARTNERS_KEYSPACE_NAME,
      key: `lookup|ISA|ZZ/THISISME`,
      value: {
        partnerId: "this-is-me",
      },
    })
  );
};
