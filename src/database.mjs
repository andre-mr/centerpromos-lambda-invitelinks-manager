import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, QueryCommand, UpdateCommand } from "@aws-sdk/lib-dynamodb";

let docClient = null;
let AMAZON_DYNAMODB_TABLE = null;
let defaultInviteLinksCache = null; // cache for items from the default table (keyed by SK)

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

          // Decide target table based on DomainWhatsAppInviteLinks
          const hasDomainInviteLinks = !!(campaignItem && campaignItem.DomainWhatsAppInviteLinks);
          const targetTable = hasDomainInviteLinks ? accountID.toLowerCase() : AMAZON_DYNAMODB_TABLE;
          const itemToUpdate = {
            PK: "WHATSAPP#INVITELINKS",
            Campaign: groups[0].Campaign,
            Category: categoryKey === "no_category" ? "" : categoryKey.toLowerCase(),
            Domain: campaignItem?.DomainWhatsAppInviteLinks || "",
            InviteCodes: inviteCodes,
            Updated: updatedTime,
            TableName: targetTable,
          };

          // Unified SK schema (no account prefix):
          // - SK = "CAMPAIGN" or "CAMPAIGN#CATEGORY"
          if (categoryKey === "no_category") {
            itemToUpdate.SK = campaignKey.toUpperCase();
          } else {
            itemToUpdate.SK = `${campaignKey.toUpperCase()}#${categoryKey.toUpperCase()}`;
            itemToUpdate.Category = categoryKey;
          }

          await updateInviteLinksItem(itemToUpdate);
        }
      }

      // get all invitelinks items for this account across both possible tables
      // decide which tables to query for existing WHATSAPP#INVITELINKS items:
      // - if all processed campaigns have DomainWhatsAppInviteLinks -> only account table
      // - if none have -> only default table
      // - otherwise -> both
      let anyHasDomain = false;
      let anyNoDomain = false;
      for (const campaignKey in groupsByCampaign) {
        const campaignItem = allCampaigns.find((c) => c.SK.toLowerCase() === campaignKey.toLowerCase());
        if (campaignItem && campaignItem.DomainWhatsAppInviteLinks) {
          anyHasDomain = true;
        } else {
          anyNoDomain = true;
        }
      }
      const queryAccount = anyHasDomain;
      const queryDefault = anyNoDomain;

      const allInviteLinksItems = await getAllCategoryInviteLinks(accountID, { queryDefault, queryAccount });

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

      for (const wrappedItem of allInviteLinksItems) {
        const item = wrappedItem.item;
        const sourceTable = wrappedItem.tableName;
        const skToCheck = item.SK; // SK is already "CAMPAIGN" or "CAMPAIGN#CATEGORY" in any table
        // skToCheck is now in a format comparable with validSKsSet
        if (!validSKsSet.has(skToCheck)) {
          // clear InviteCodes in-place on the correct table
          await updateInviteLinksItem({
            PK: item.PK,
            SK: item.SK,
            InviteCodes: [],
            Updated: updatedTime,
            TableName: sourceTable,
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

// helper: load all WHATSAPP#INVITELINKS from default table into cache (paginated)
async function loadDefaultInviteLinks() {
  if (!AMAZON_DYNAMODB_TABLE) return;
  defaultInviteLinksCache = new Map();
  let ExclusiveStartKey = undefined;
  try {
    do {
      const cmd = new QueryCommand({
        TableName: AMAZON_DYNAMODB_TABLE,
        KeyConditionExpression: "PK = :pk",
        ExpressionAttributeValues: { ":pk": "WHATSAPP#INVITELINKS" },
        ExclusiveStartKey,
      });
      const resp = await docClient.send(cmd);
      const items = resp.Items || [];
      for (const it of items) {
        // store by SK (default table SK is CAMPAIGN or CAMPAIGN#CATEGORY per new schema)
        defaultInviteLinksCache.set(it.SK, it);
      }
      ExclusiveStartKey = resp.LastEvaluatedKey;
    } while (ExclusiveStartKey);
  } catch (err) {
    console.warn("Warning loading default invite links cache:", err && err.name ? err.name : err);
    defaultInviteLinksCache = null;
  }
}

// get all invitelinks items from account table and/or default table for the account
async function getAllCategoryInviteLinks(accountID, options = {}) {
  const { queryDefault = true, queryAccount = true } = options;
  const results = [];

  if (!queryDefault && !queryAccount) {
    return results;
  }

  // 1) items stored in default (central) table
  if (queryDefault) {
    // load cache once (lazy) if not present
    if (defaultInviteLinksCache === null) {
      await loadDefaultInviteLinks();
    }

    if (defaultInviteLinksCache) {
      for (const it of defaultInviteLinksCache.values()) {
        // include all items from the default table (SK = CAMPAIGN or CAMPAIGN#CATEGORY)
        results.push({ item: it, tableName: AMAZON_DYNAMODB_TABLE });
      }
    } else {
      // fallback: query all items with PK=WHATSAPP#INVITELINKS
      try {
        const commandDefault = new QueryCommand({
          TableName: AMAZON_DYNAMODB_TABLE,
          KeyConditionExpression: "PK = :pk",
          ExpressionAttributeValues: {
            ":pk": "WHATSAPP#INVITELINKS",
          },
        });

        const responseDefault = await docClient.send(commandDefault);
        const itemsDefault = responseDefault.Items || [];
        for (const it of itemsDefault) {
          results.push({ item: it, tableName: AMAZON_DYNAMODB_TABLE });
        }
      } catch (err) {
        console.warn("Warning querying default invite links table (fallback):", err && err.name ? err.name : err);
      }
    }
  }

  // 2) items stored in the account's own table (if requested)
  if (queryAccount) {
    try {
      const accountTableName = accountID.toLowerCase();
      const commandAccount = new QueryCommand({
        TableName: accountTableName,
        KeyConditionExpression: "PK = :pk",
        ExpressionAttributeValues: {
          ":pk": "WHATSAPP#INVITELINKS",
        },
      });

      const responseAccount = await docClient.send(commandAccount);
      const itemsAccount = responseAccount.Items || [];
      for (const it of itemsAccount) {
        results.push({ item: it, tableName: accountTableName });
      }
    } catch (err) {
      console.warn(
        `Warning querying account table ${accountID.toLowerCase()} for invite links:`,
        err && err.name ? err.name : err
      );
    }
  }

  return results;
}

// last step: update each item in main table
async function updateInviteLinksItem(item) {
  const updateExpressionParts = [];
  const expressionAttributeValues = {};
  const expressionAttributeNames = {};

  if (item.Campaign) {
    updateExpressionParts.push("Campaign = :campaign");
    expressionAttributeValues[":campaign"] = item.Campaign || "";
  }
  if (item.Category) {
    updateExpressionParts.push("Category = :category");
    expressionAttributeValues[":category"] = item.Category || "";
  }
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

  const tableName = item.TableName || AMAZON_DYNAMODB_TABLE;

  const commandParams = {
    TableName: tableName,
    Key: {
      PK: item.PK,
      SK: item.SK,
    },
    UpdateExpression: updateExpression,
    ExpressionAttributeValues: expressionAttributeValues,
  };

  if (Object.keys(expressionAttributeNames).length > 0) {
    commandParams.ExpressionAttributeNames = expressionAttributeNames;
  }

  const command = new UpdateCommand(commandParams);

  const result = await docClient.send(command);
  if (result.$metadata && result.$metadata.httpStatusCode == 200) {
    // if we maintain a cache for default table, update it to reflect this write
    try {
      if (tableName === AMAZON_DYNAMODB_TABLE && defaultInviteLinksCache instanceof Map) {
        // create/merge a lightweight representation (we only need fields used later)
        const cached = Object.assign({}, defaultInviteLinksCache.get(item.SK) || {}, {
          PK: item.PK,
          SK: item.SK,
          Campaign:
            item.Campaign ||
            (defaultInviteLinksCache.get(item.SK) && defaultInviteLinksCache.get(item.SK).Campaign) ||
            "",
          Category:
            item.Category ||
            (defaultInviteLinksCache.get(item.SK) && defaultInviteLinksCache.get(item.SK).Category) ||
            "",
          Domain:
            item.Domain || (defaultInviteLinksCache.get(item.SK) && defaultInviteLinksCache.get(item.SK).Domain) || "",
          InviteCodes: item.InviteCodes || [],
          Updated:
            item.Updated ||
            (defaultInviteLinksCache.get(item.SK) && defaultInviteLinksCache.get(item.SK).Updated) ||
            "",
        });
        defaultInviteLinksCache.set(item.SK, cached);
      }
    } catch (err) {
      // non-fatal cache update error
      console.warn("Warning updating default invite links cache after write:", err && err.name ? err.name : err);
    }

    return true;
  }
  return false;
}
