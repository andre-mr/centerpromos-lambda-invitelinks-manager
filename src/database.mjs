import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";

let docClient = null;
let AMAZON_DYNAMODB_TABLE = null;

export const initializeClient = (event = {}) => {
  if (!process.env.AMAZON_DYNAMODB_TABLE) {
    throw new Error("AMAZON_DYNAMODB_TABLE is required");
  }

  AMAZON_DYNAMODB_TABLE = process.env.AMAZON_DYNAMODB_TABLE;

  const config = {};
  if (process.env.AMAZON_REGION) {
    config.region = process.env.AMAZON_REGION;
  }

  // only use credentials if provided in the event object (for testing)
  if (event.credentials) {
    config.credentials = {
      accessKeyId: event.credentials.accessKeyId,
      secretAccessKey: event.credentials.secretAccessKey,
    };
  }

  const client = new DynamoDBClient(config);
  docClient = DynamoDBDocumentClient.from(client);
};

export const updateInviteLinks = async (event = {}) => {
  try {
    initializeClient(event);

    if (!event.accounts || !Array.isArray(event.accounts) || event.accounts.length === 0) {
      console.error("Invalid or empty 'accounts' array in the event.");
      return false;
    }

    for (const accountObj of event.accounts) {
      const accountID = Object.keys(accountObj)[0];
      const campaignsToProcess = accountObj[accountID].map((c) => c.replace(/\s/g, "").toLowerCase());

      if (!accountID || !campaignsToProcess || campaignsToProcess.length === 0) {
        continue; // Skip to next account if this one is invalid
      }

      const allCampaigns = await getAllCampaigns(accountID);
      const allGroups = await getAllGroups(accountID);
      const allCategories = await getAllCategories(accountID);
      const validCategorySKs = allCategories.map((cat) => cat.SK.toLowerCase());

      // 1. Filter publishable groups, with InviteCode and belonging to the account's campaigns
      const publishableGroups = allGroups.filter(
        (group) =>
          group.Publishable &&
          (group.InviteCode || group.InviteLink) &&
          group.Campaign &&
          campaignsToProcess.includes(group.Campaign.replace(/\s/g, "").toLowerCase())
      );

      // 2. Filter groups with valid category or without category
      const validGroups = publishableGroups.filter(
        (group) => !group.Category || validCategorySKs.includes(group.Category.toLowerCase())
      );

      // 3. Group by Campaign and then by Category
      const groupsByCampaign = {};
      for (const group of validGroups) {
        const campaignKey = group.Campaign.replace(/\s/g, "").toLowerCase();
        if (!groupsByCampaign[campaignKey]) {
          groupsByCampaign[campaignKey] = {};
        }
        const categoryKey = group.Category ? group.Category.toLowerCase() : "no_category";
        if (!groupsByCampaign[campaignKey][categoryKey]) {
          groupsByCampaign[campaignKey][categoryKey] = [];
        }
        groupsByCampaign[campaignKey][categoryKey].push(group);
      }

      const updatedTime = new Date().toISOString();

      // 4. Process each campaign and its grouped categories
      for (const campaignKey in groupsByCampaign) {
        const categoriesInCampaign = groupsByCampaign[campaignKey];

        for (const categoryKey in categoriesInCampaign) {
          const groups = categoriesInCampaign[categoryKey];

          // Sort groups by number of members in ascending order
          groups.sort((a, b) => (a.Members || 0) - (b.Members || 0));

          const inviteCodes = groups
            .slice(0, 10) // Take the first 10
            .map((g) => `${g.SK}|${g.Name}|${g.InviteCode || g.InviteLink}`);

          const campaignItem = allCampaigns.find((c) => c.SK.toLowerCase() === campaignKey.toLowerCase());

          const itemToUpdate = {
            PK: "WHATSAPP#INVITELINKS",
            AccountSK: accountID.toUpperCase(),
            Campaign: groups[0].Campaign,
            Category: categoryKey === "no_category" ? "" : categoryKey.toLowerCase(),
            Domain: campaignItem?.DomainWhatsAppInviteLinks || "",
            InviteCodes: inviteCodes,
            Updated: updatedTime,
          };

          if (categoryKey === "no_category") {
            itemToUpdate.SK = campaignKey.toUpperCase();
          } else {
            itemToUpdate.SK = `${campaignKey.toUpperCase()}#${categoryKey.toUpperCase()}`;
            itemToUpdate.Category = categoryKey;
          }

          await updateInviteLinksItem(itemToUpdate);
        }
      }

      const allInviteLinksItems = await getAllCategoryInviteLinks(accountID);

      const validSKsSet = new Set();
      for (const campaignKey in groupsByCampaign) {
        for (const categoryKey in groupsByCampaign[campaignKey]) {
          if (categoryKey === "no_category") {
            validSKsSet.add(campaignKey.toUpperCase());
          } else {
            validSKsSet.add(`${campaignKey.toUpperCase()}#${categoryKey.toUpperCase()}`);
          }
        }
      }

      for (const item of allInviteLinksItems) {
        if (!validSKsSet.has(item.SK)) {
          await updateInviteLinksItem({
            PK: item.PK,
            SK: item.SK,
            InviteCodes: [],
            Updated: updatedTime,
          });
        }
      }

      // Pause for 1 second before processing the next account
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }

    return true;
  } catch (error) {
    console.error("Error updating invite links:", error);
    return false;
  }
};

// get all campaign items from account table
async function getAllCampaigns(accountID) {
  const command = new QueryCommand({
    TableName: accountID.toLowerCase(),
    KeyConditionExpression: "PK = :pk",
    ExpressionAttributeValues: {
      ":pk": "CAMPAIGN",
    },
  });

  const response = await docClient.send(command);
  return response.Items || [];
}

// get all group items from account table
async function getAllGroups(accountID) {
  const command = new QueryCommand({
    TableName: accountID.toLowerCase(),
    KeyConditionExpression: "PK = :pk",
    ExpressionAttributeValues: {
      ":pk": "WHATSAPP#GROUP",
    },
  });

  const response = await docClient.send(command);
  return response.Items || [];
}

// get all categories from account table
async function getAllCategories(accountID) {
  const command = new QueryCommand({
    TableName: accountID.toLowerCase(),
    KeyConditionExpression: "PK = :pk",
    ExpressionAttributeValues: {
      ":pk": "WHATSAPP#GROUPCATEGORY",
    },
  });

  const response = await docClient.send(command);
  return response.Items || [];
}

// get all invitelinks items from account table
async function getAllCategoryInviteLinks(accountID) {
  const command = new QueryCommand({
    TableName: AMAZON_DYNAMODB_TABLE,
    KeyConditionExpression: "PK = :pk and begins_with(SK, :sk)",
    ExpressionAttributeValues: {
      ":pk": "WHATSAPP#INVITELINKS",
      ":sk": `${accountID.toUpperCase()}#`,
    },
  });

  const response = await docClient.send(command);
  return response.Items || [];
}

// last step: update each item in main table
async function updateInviteLinksItem(item) {
  const updateExpressionParts = [];
  const expressionAttributeValues = {};
  const expressionAttributeNames = {};

  if (item.AccountSK) {
    updateExpressionParts.push("AccountSK = :accountSK");
    expressionAttributeValues[":accountSK"] = item.AccountSK;
  }
  updateExpressionParts.push("Campaign = :campaign");
  expressionAttributeValues[":campaign"] = item.Campaign || "";
  updateExpressionParts.push("Category = :category");
  expressionAttributeValues[":category"] = item.Category || "";
  if (item.Domain !== undefined) {
    updateExpressionParts.push("#domain = :domain");
    expressionAttributeValues[":domain"] = item.Domain || "";
    expressionAttributeNames["#domain"] = "Domain";
  }
  if (item.InviteCodes) {
    updateExpressionParts.push("InviteCodes = :inviteCodes");
    expressionAttributeValues[":inviteCodes"] = item.InviteCodes || [];
  }
  if (item.Updated) {
    updateExpressionParts.push("Updated = :updated");
    expressionAttributeValues[":updated"] = item.Updated;
  }

  const updateExpression = `SET ${updateExpressionParts.join(", ")}`;

  const commandParams =
    item.Domain !== undefined
      ? {
          TableName: AMAZON_DYNAMODB_TABLE,
          Key: {
            PK: item.PK,
            SK: item.SK,
          },
          UpdateExpression: updateExpression,
          ExpressionAttributeValues: expressionAttributeValues,
          ExpressionAttributeNames: expressionAttributeNames,
        }
      : {
          TableName: AMAZON_DYNAMODB_TABLE,
          Key: {
            PK: item.PK,
            SK: item.SK,
          },
          UpdateExpression: updateExpression,
          ExpressionAttributeValues: expressionAttributeValues,
        };

  const command = new UpdateCommand(commandParams);

  const result = await docClient.send(command);
  if (result.$metadata.httpStatusCode == 200) {
    return true;
  }
  return false;
}
