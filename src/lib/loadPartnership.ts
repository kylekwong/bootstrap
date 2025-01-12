import { GetValueCommand } from "@stedi/sdk-client-stash";
import { PARTNERS_KEYSPACE_NAME } from "./constants.js";
import { stashClient as buildStashClient } from "./stash.js";
import { PartnershipSchema, Partnership } from "./types/PartnerRouting.js";

const stashClient = buildStashClient();

export const loadPartnership = async (
  sendingPartnerId: string,
  receivingPartnerId: string
): Promise<Partnership> => {
  let partnership: Partnership | undefined;

  const keysToCheck = [
    `partnership|${sendingPartnerId}|${receivingPartnerId}`,
    `partnership|${receivingPartnerId}|${sendingPartnerId}`,
  ];

  for (const key of keysToCheck) {
    try {
      const { value } = await stashClient.send(
        new GetValueCommand({
          keyspaceName: PARTNERS_KEYSPACE_NAME,
          key,
        })
      );

      if (value !== null && typeof value === "object") {
        partnership = PartnershipSchema.parse(value);
        break;
      }
    } catch (error) {
      console.log(error);
    }
  }

  if (partnership === undefined)
    throw new Error(
      `No partnership found for '${sendingPartnerId}' and '${receivingPartnerId}' in '${PARTNERS_KEYSPACE_NAME}' keyspace`
    );

  return partnership;
};
