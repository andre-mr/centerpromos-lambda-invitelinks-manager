import { jest } from "@jest/globals";
import { handler } from "../src/index.mjs";
import dotenv from "dotenv";
dotenv.config();

jest.mock("@aws-sdk/client-dynamodb", () => {
  return {
    DynamoDBClient: jest.fn().mockImplementation(() => ({
      send: jest.fn(),
    })),
  };
});

jest.mock("@aws-sdk/lib-dynamodb", () => {
  return {
    DynamoDBDocumentClient: {
      from: jest.fn().mockReturnValue({
        send: jest.fn().mockResolvedValue({ UnprocessedItems: {} }),
      }),
    },
    BatchWriteCommand: jest.fn().mockImplementation((params) => params),
  };
});

const credentials = {
  accessKeyId: process.env.AMAZON_ACCESS_KEY_ID,
  secretAccessKey: process.env.AMAZON_SECRET_ACCESS_KEY,
};

const ACCOUNT_TEST = process.env.ACCOUNT_TEST;
const CAMPAIGN_TEST = process.env.CAMPAIGN_TEST || ACCOUNT_TEST;

const events = {
  validRequest: {
    accounts: [
      {
        [ACCOUNT_TEST]: [CAMPAIGN_TEST],
      },
    ],
    apiKey: process.env.API_KEY,
    credentials,
  },
  missingAccounts: {
    apiKey: process.env.API_KEY,
    credentials,
  },
  unauthorizedRequest: {
    accounts: [
      {
        [ACCOUNT_TEST]: [CAMPAIGN_TEST],
      },
    ],
    apiKey: "wrong-key",
    credentials,
  },
};

describe("Lambda Handler - Atualização de Invite Links (cenário real de tabela)", () => {
  beforeEach(() => {
    expect(process.env.AMAZON_ACCESS_KEY_ID).toBeDefined();
    expect(process.env.AMAZON_SECRET_ACCESS_KEY).toBeDefined();
    expect(process.env.AMAZON_MAIN_REGION).toBeDefined();
    expect(process.env.AMAZON_DYNAMODB_TABLE).toBeDefined();
    expect(process.env.API_KEY).toBeDefined();
    expect(process.env.ACCOUNT_TEST).toBeDefined();
    expect(process.env.CAMPAIGN_TEST).toBeDefined();
  });

  test("deve atualizar com sucesso os invite links para a account/campanha de teste", async () => {
    const response = await handler(events.validRequest);
    expect(response.statusCode).toBe(200);
    const body = JSON.parse(response.body);
    expect(body.message).toBe("Groups and invite links updated successfully");
  }, 30000);

  test("deve retornar erro 400 se não enviar accounts", async () => {
    const response = await handler(events.missingAccounts);
    expect(response.statusCode).toBe(400);
    const body = JSON.parse(response.body);
    expect(body.message).toBe("Bad Request: Missing accounts");
  });

  test("deve rejeitar requests não autorizados", async () => {
    const response = await handler(events.unauthorizedRequest);
    expect(response.statusCode).toBe(401);
    const body = JSON.parse(response.body);
    expect(body.message).toBe("Unauthorized: Invalid or missing API key");
  });
});
