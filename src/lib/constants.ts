import dotenv from "dotenv";

import { requiredEnvVar } from "./environment.js";

dotenv.config({ override: true });

export const DEFAULT_SDK_CLIENT_PROPS = {
  apiKey: requiredEnvVar("STEDI_API_KEY"),
  region: "us",
};

export const PARTNERS_KEYSPACE_NAME = "partners-configuration";
export const OUTBOUND_CONTROL_NUMBER_KEYSPACE_NAME = "outbound-control-numbers";
export const INBOUND_CONTROL_NUMBER_KEYSPACE_NAME = "inbound-control-numbers";
