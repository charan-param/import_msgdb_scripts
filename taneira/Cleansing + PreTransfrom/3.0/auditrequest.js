/**
 * Taneira
 * FullProd:FirstSampleCompleted -> First/Second Saree Approval Audit Request
 * 
 * This module handles the generation of audit requests for SKUs in production documents
 * based on procurement type and approval status rules.
 */

const chainConfigs = {
    "public:0xb503e55d4718fe050659ae7f2347d06fdbd02498395b5c2994bed7cbcae1abca_FullProd": {
        state_machine: "public:0xb503e55d4718fe050659ae7f2347d06fdbd02498395b5c2994bed7cbcae1abca",
        stateFrom: "FullProd",
        subState: "FirstSampleCompleted",
        outputDocInfo: {
            smID: "public:0xaed0b39a809d7c027a8f17434a585fffaec96a857e7407e3d8476f353118f6ae",
            outputFormat: "",
            stateTo: "PurchaseReq",
            subState: "",
            appShortName: "A/FSSA",
            deleteKeys: [
                "parentDocID",
                "parentTxnID",
                "stateGraph"
            ]
        }
    }
};

const config = {
    mode: "gprm",
    lotSize: 25,
    VALID_PROCUREMENT_TYPES: ["PROD"],
    APPROVED_STATUS: "FirstSecondSareeApprovalAudit:FinalApproval",
    BATCH_SIZE: 10,
    MAX_RETRIES: 3,
    RETRY_DELAY: 1000 // 1 second
};

const constants = {
    collections: {
        catalogue: "addcatalogue_public:0x725691f1d57d4aa98fe5810fd0f45eeceb5979413bbe9a763bc52c07e5f3794d",
        orders: "orders_public:0xc06fb01ce54f96b8ebb7f0d056960b9a392c3c5523ed27fb84744af54011e835",
        statehistory: "statehistory",
        fsAudit: "firstsecondsareeapprovalaudit_0xd288f265ed01c636377ef3ba7cbdd29271a9d00b",
        fullProd: "fullprod_0xd288f265ed01c636377ef3ba7cbdd29271a9d00b",
        purchaseReq: "purchasereq_public:0xc06fb01ce54f96b8ebb7f0d056960b9a392c3c5523ed27fb84744af54011e835"
    },
    ERROR_MESSAGES: {
        INVALID_DOCUMENT: "Invalid or missing document data",
        NO_SKUS: "Document contains no SKUs",
        INVALID_SKU: "Invalid SKU data found",
        INVALID_PROC_TYPE: "Invalid procurement type",
        DB_ERROR: "Database operation failed",
        DUPLICATE_REQUEST: "Audit request already exists",
        SKU_ALREADY_APPROVED: "SKU is already approved",
        NON_PROD_SKU: "Non-PROD SKU type",
        MISSING_CATALOGUE: "SKU not found in catalogue",
        MISSING_ROOT_DOC: "Unable to get root document",
        MISSING_CONTACT: "Missing contact information"
    }
};

// Global Variables
let paramID = "",
    smID = "",
    stateTo = "",
    exchangeParamID = "",
    projectID = "",
    workspace = "",
    portal = "";

/**
 * Main entry point for processing document state changes
 */
async function processChanges(orgParamID, xchangeParamID, sm, state, project, docid, ws, pt) {
    try {
        // Initialize global variables
        state = state.replace(":FirstSampleCompleted", "");
        paramID = orgParamID;
        exchangeParamID = xchangeParamID;
        smID = sm;
        stateTo = state;
        projectID = project;
        workspace = ws;
        portal = pt;

        const chainConfig = chainConfigs[`${smID}_${stateTo}`];
        const outputDocInfo = chainConfig.outputDocInfo;

        // Get and validate document
        const doc = await getDocwithDocID(docid, smID, stateTo, paramID, projectID, workspace, portal);
        if (!doc) {
            throw new Error(constants.ERROR_MESSAGES.INVALID_DOCUMENT);
        }

        const jsonLd = Array.isArray(doc) ? doc : [doc];

        // Process document(s)
        const processedDocs = await processDocuments(jsonLd, chainConfig, outputDocInfo);

        // Create new documents
        return await createDocuments(processedDocs, chainConfig);
    } catch (error) {
        console.error(`Error in processChanges: ${error.message}`);
        return Promise.reject(error);
    }
}

/**
 * Process documents in batches
 */
async function processDocuments(documents, chainConfig, docInfo) {
    const processedDocs = [];
    const batchSize = config.BATCH_SIZE;

    for (let i = 0; i < documents.length; i += batchSize) {
        const batch = documents.slice(i, i + batchSize);
        await Promise.all(batch.map(async (doc) => {
            try {
                // Basic document validation
                if (!doc || !doc.OrderedItems || !Array.isArray(doc.OrderedItems)) {
                    throw new Error(constants.ERROR_MESSAGES.NO_SKUS);
                }

                // Get root document
                const rootDocID = doc?.ReferencesOrder?.[0]?.R_Identifier;
                if (!rootDocID) {
                    throw new Error(constants.ERROR_MESSAGES.MISSING_ROOT_DOC);
                }

                const rootDoc = await getRootDoc(rootDocID);
                if (!rootDoc) {
                    throw new Error(constants.ERROR_MESSAGES.MISSING_ROOT_DOC);
                }

                // Process each SKU
                const skuDocs = await processSKUs(doc, docInfo, chainConfig, rootDoc);
                if (skuDocs.length === 0) {
                    throw new Error(constants.ERROR_MESSAGES.NO_SKUS);
                }
                processedDocs.push(...skuDocs);
            } catch (error) {
                console.error(`Error processing document ${doc._id}: ${error.message}`);
                throw error;
            }
        }));
    }

    return processedDocs;
}

/**
 * Check if SKU exists in previously created production documents
 */
async function checkSKUInProductionDocs(sku, rootTxnId) {
    try {
        // Check in fullprod collection for production documents
        const db = getOrgMongoDBRef(projectID, paramID, workspace, portal);
        const prodDocs = await db.collection(constants.collections.fullProd)
            .find({
                "LocalProperties.P_RootTxnID": rootTxnId,
                "SystemProperties.P_SubState": "FullProd:FirstSampleApproved"
            }, {
                projection: {
                    "OrderedItems": 1
                }
            }).toArray();

        // Extract all SKUs from production documents
        const prodSKUs = prodDocs.flatMap(doc =>
            (doc.OrderedItems || [])
                .map(item => item.I_Number)
                .filter(Boolean)
        );

        // Check if our SKU is in the production list
        return prodSKUs.includes(sku);
    } catch (error) {
        console.error(`Error checking SKU in production documents: ${error.message}`);
        return false;
    }
}

/**
 * Process individual SKUs from a document
 */
async function processSKUs(doc, docInfo, chainConfig, rootDoc) {
    const skuDocs = [];
    const orderedItems = doc.OrderedItems || [];
    const rootTxnId = doc?.LocalProperties?.P_RootTxnID;
    const poNumber = doc?.ReferencesOrder?.[0]?.R_OrderNumber || "";

    if(!poNumber){
        console.warn("skipping missing PO Number in ReferencesOrder");
        throw new Error(constants.ERROR_MESSAGES.MISSING_PONUMBER);
    }

    // Get PO quantities for all SKUs
    const poQuantities = await getPOQuantities(rootDoc);

    for (const item of orderedItems) {
        try {
            if (!item.I_Number) {
                console.warn(`Skipping invalid SKU in document ${doc._id}`);
                continue;
            }

            // Check if SKU is already approved
            const isApproved = await checkSKUInProductionDocs(item.I_Number, rootTxnId);
            if (isApproved) {
                console.log(`SKU ${item.I_Number} is already approved, skipping`);
                continue;
            }

            // Get PO quantity for this SKU
            const poQty = poQuantities.get(item.I_Number);
            if (!poQty) {
                console.warn(`SKU ${item.I_Number} not found in PO, skipping`);
                continue;
            }

            // Get current production quantity
            const prodQty = Number(item.I_Quantity) || 0;

            // Skip if production quantity exceeds PO quantity
            if (prodQty > poQty) {
                console.log(`SKU ${item.I_Number} production quantity (${prodQty}) exceeds PO quantity (${poQty}), skipping`);
                continue;
            }

            // Get SKU details from catalogue
            const skuDetails = await getSKUDetails(item.I_Number);
            // if (!skuDetails) {
            //     console.error(`SKU ${item.I_Number} not found in catalogue`);
            //     continue;
            // }

            // Check procurement type
            // if (skuDetails.I_ProcType !== "PROD") {
            //     console.log(`Skipping non-PROD SKU ${item.I_Number}`);
            //     continue;
            // }

            // Check for existing audit request
            const existingRequest = await checkExistingAuditRequest(poNumber, item.I_Number)
            if (existingRequest && Object.keys(existingRequest).length) {
                console.log(`Audit request already exists for SKU ${item.I_Number}`);
                continue;
            }

            // Create SKU level document with quantities
            const skuDoc = await createSKULevelDocument(doc, item, docInfo, chainConfig, rootDoc, skuDetails, {
                poQty,
                producedQty: prodQty,
                remainingQty: poQty - prodQty
            });
            skuDocs.push(skuDoc);
        } catch (error) {
            console.error(`Error processing SKU ${item.I_Number}: ${error.message}`);
            // Continue with next SKU instead of failing entire batch
        }
    }

    return skuDocs;
}

/**
 * Get quantities from PO for all SKUs
 */
async function getPOQuantities(rootDoc) {
    const quantities = new Map();

    if (!rootDoc?.OrderedItems) {
        return quantities;
    }

    rootDoc.OrderedItems.forEach(item => {
        if (item.I_Number && item.I_Quantity) {
            quantities.set(item.I_Number, Number(item.I_Quantity));
        }
    });

    return quantities;
}

/**
 * Check if SKU is already approved
 */
async function checkSKUApprovalStatus(sku) {
    try {
        const db = getOrgMongoDBRef(projectID, paramID, workspace, portal);
        const result = await db.collection(constants.collections.fsAudit)
            .findOne({
                "OrderedItems.I_Number": sku,
                "SystemProperties.P_SubState": config.APPROVED_STATUS
            });
        return !!result;
    } catch (error) {
        console.error(`Error checking SKU approval status: ${error.message}`);
        return false;
    }
}

/**
 * Get SKU details from catalogue
 */
async function getSKUDetails(sku) {
    try {
        const db = getOrgMongoDBRef(projectID, paramID, workspace, portal);
        return await db.collection(constants.collections.catalogue)
            .findOne({ "Items.I_Number": sku });
    } catch (error) {
        console.error(`Error getting SKU details: ${error.message}`);
        throw new Error(constants.ERROR_MESSAGES.DB_ERROR);
    }
}

/**
 * Check for existing audit request
 */
async function checkExistingAuditRequest(poNumber, sku) {
    console.log("checkExistingAuditRequest Query", { "ReferencesOrder.R_OrderNumber": poNumber, "AnalysisReport.SKU": sku });
    try {
        const db = getOrgMongoDBRef(projectID, paramID, workspace, portal);
        return await db.collection(constants.collections.purchaseReq)
            .findOne({ "ReferencesOrder.R_OrderNumber": poNumber, "AnalysisReport.SKU": sku });
    } catch (error) {
        console.error(`Error checking existing audit request: ${error.message}`);
        return false;
    }
}

/**
 * Create SKU level document
 */
async function createSKULevelDocument(doc, item, docInfo, chainConfig, rootDoc, skuDetails, quantities) {
    try {
        // Create base document
        let skuDoc = JSON.parse(JSON.stringify(doc));

        // Update quantities
        skuDoc.SKUQty = quantities.poQty;
        skuDoc.ProducedQty = quantities.producedQty;
        skuDoc.RemainingQty = quantities.remainingQty;

        // Update OrderedItems to only include current SKU with all details from catalogue
        skuDoc.OrderedItems = [{
            ...item,
            I_Name: skuDetails?.Items?.I_Name || "",
            I_Description: skuDetails?.Items?.I_Description || "",
            I_Category: skuDetails?.Items?.I_Category || "",
            I_SubCategory: skuDetails?.Items?.I_SubCategory || "",
            I_HSNCode: skuDetails?.Items?.I_HSNCode || "",
            I_Type: skuDetails?.Items?.I_Type || "Goods",
            I_Purpose: skuDetails?.Items?.I_Purpose || "Buy",
            I_URL: skuDetails?.Items?.I_URL || "",
            I_BaseColor: skuDetails?.TechDetails?.I_BaseColor || "",
            I_BaseZari: skuDetails?.TechDetails?.I_ZariType || "",
            I_BlouseZari: skuDetails?.TechDetails?.I_ZariType || "",
            I_BlouseWeft: skuDetails?.TechDetails?.I_WeftMaterial || "",
            I_BlouseWarp: skuDetails?.TechDetails?.I_WarpMaterial || "",
            I_BaseWarp: skuDetails?.TechDetails?.I_WarpMaterial || "",
            I_BaseWeft: skuDetails?.TechDetails?.I_WeftMaterial || "",
            I_Cluster: skuDetails?.TechDetails?.I_Cluster || "",
            I_DCDate: skuDetails?.TechDetails?.I_DCDate || "",
            I_DCNumber: skuDetails?.TechDetails?.I_DCNumber || "",
            I_LoomType: skuDetails?.AdditionalProperties?.LoomType || "",
            I_MaterialDescription: skuDetails?.Items?.I_Description || "",
            I_Price: skuDetails?.Pricing?.I_Price || 0,
            I_PriceCurrency: skuDetails?.Pricing?.I_PriceCurrency || "INR",
            I_CostPerItem: skuDetails?.Pricing?.I_CostPerItem || 0,
            // Update quantities in OrderedItems
            I_Quantity: quantities.producedQty,
            I_POQuantity: quantities.poQty,
            I_RemainingQuantity: quantities.remainingQty
        }];

        // Basic modifications
        skuDoc = await basicJsonModification(skuDoc, docInfo);
        skuDoc = await updateSystemProperties(skuDoc);
        skuDoc = await updateDocDetails(skuDoc, docInfo, chainConfig, rootDoc, item.I_Number);
        skuDoc = await updateContacts(skuDoc, rootDoc);
        skuDoc = await updateRoles(skuDoc);

        // Add analysis report with correct quantities
        skuDoc.AnalysisReport = createAnalysisReport(item, rootDoc, skuDetails, quantities);

        // Add parameters from catalogue
        skuDoc.Parameters = extractParameters(skuDetails);

        // Clean up non-schema fields
        skuDoc = deleteNonSchemaFields(skuDoc, chainConfig);

        return skuDoc;
    } catch (error) {
        console.error(`Error creating SKU level document: ${error.message}`);
        throw error;
    }
}

/**
 * Create analysis report for SKU
 */
function createAnalysisReport(item, rootDoc, skuDetails, quantities) {
    // Get the correct quantities from PO and Production
    const poItem = rootDoc?.OrderedItems?.find(i => i.I_Number === item.I_Number);

    return {
        PONo: rootDoc?.DocDetails?.D_OrderNumber || "",
        PODate: formatDate(rootDoc?.DocDetails?.D_OrderedDate),
        POExpiryDate: formatDate(rootDoc?.DocDetails?.D_ExpiryDate),
        SKU: item.I_Number,
        MaterialDescription: skuDetails?.Items?.I_Description || "",
        Craft: skuDetails?.TechDetails?.I_Workmanship || "",
        ClusterOrigin: skuDetails?.TechDetails?.I_Cluster || "",
        Category: skuDetails?.Items?.I_Category || "",
        SareeColor: skuDetails?.TechDetails?.I_BaseColor || "",
        BlouseColor: skuDetails?.TechDetails?.I_BaseColor || "",
        SKUQty: quantities.poQty || 0,           // From PO
        ProducedQty: quantities.producedQty || 0, // From Production
        RemainingQty: quantities.remainingQty || 0, // Calculated difference
        VendorCode: rootDoc?.Seller?.C_InternalID || "",
        VendorName: rootDoc?.Seller?.C_Organization || "",
        SampleSubmittedBy: rootDoc?.Seller?.C_Organization || "",
        EndUse: 'Saree',
        ManufactureDate: new Date().getTime(),
        ProcurementType: skuDetails?.TechDetails?.I_ProcType || "",
        BrandLabelAttached: skuDetails?.TechDetails?.I_BrandLabelAttached || "",
        DesignNumber: skuDetails?.TechDetails?.I_DesignNumber || "",
        FoldType: skuDetails?.TechDetails?.I_FoldType || "",
        InspType: skuDetails?.TechDetails?.I_InspectionType || "",
        WashCare: skuDetails?.TechDetails?.I_Washcare || "",
        TaneiraSilkMarkTagNumber: skuDetails?.TechDetails?.I_TaneiraSilkMarkTagNumber || "",
        Bulk1stPCSReviewDate: skuDetails?.TechDetails?.I_BulkFirstPcsReviewDate || "",
        HandloomMarkIHB: skuDetails?.TechDetails?.I_HandloomMarkIHB || "",
        // Additional fields from catalogue
        Specifications: skuDetails?.Items?.I_Specifications || "",
        Quality: skuDetails?.Items?.I_Quality || "",
        Design: skuDetails?.Items?.I_Design || ""
    };
}

/**
 * Extract technical parameters from catalogue
 */
function extractParameters(skuDetails) {
    return {
        Requirement1: skuDetails?.TechDetails?.I_WarpMaterial || "",
        Requirement2: skuDetails?.TechDetails?.I_WeftMaterial || "",
        Requirement20: skuDetails?.TechDetails?.I_WarpMaterial || "",
        Requirement21: skuDetails?.TechDetails?.I_WeftMaterial || "",
        Requirement9: skuDetails?.TechDetails?.I_ZariType || "",
        Requirement22: skuDetails?.TechDetails?.I_ZariType || "",
        Requirement23: skuDetails?.TechDetails?.I_BaseColor || "",
        Requirement24: skuDetails?.TechDetails?.I_BaseColor || "",
        Requirement19: skuDetails?.TechDetails?.I_Workmanship || "",
        Requirement18: skuDetails?.TechDetails?.I_WeightGMS || "",
        Requirement5: skuDetails?.AdditionalProperties?.LoomType || "",
        Technical: skuDetails?.Items?.I_TechnicalParams || "",
        Quality: skuDetails?.Items?.I_QualityParams || "",
        Design: skuDetails?.Items?.I_DesignParams || ""
    };
}

function basicJsonModification(doc, docInfo) {
    let subState = ""
    if (docInfo['subState']) {
        subState = `${docInfo.stateTo}:${docInfo['subState']}`
    }
    let systemProperties = doc["SystemProperties"] ? doc["SystemProperties"] : {}
    systemProperties["P_SmID"] = docInfo["smID"] ? docInfo["smID"] : ""
    systemProperties["P_SubState"] = subState
    systemProperties["P_RootTxnID"] = [doc["LocalProperties"]["P_RootTxnID"]]
    systemProperties['P_RecordAdded'] = ""
    doc["SystemProperties"] = systemProperties
    doc["roles"] = doc["LocalProperties"]["Roles"]
    doc['parentDocID'] = doc['_id']
    doc['parentTxnID'] = doc['LocalProperties']['TxnID']
    return doc;
}

function updateSystemProperties(jsonLd) {
    let systemProperties = jsonLd["SystemProperties"]
    systemProperties['P_RecordAdded'] = new Date().getTime()
    jsonLd["SystemProperties"] = systemProperties
    return jsonLd;
}

/**
 * Update document details with proper numbering
 */
async function updateDocDetails(doc, docInfo, chainConfig, rootDoc, sku) {
    try {
        const timestamp = new Date().getTime();
        const normalizedSKU = sku.toUpperCase();
        const docNumber = `${docInfo.appShortName}/${normalizedSKU}/${timestamp}`;
        const id = generateDocID(docNumber, docInfo.stateTo, doc.SystemProperties?.P_DocOwner);

        doc.DocDetails = {
            ...doc.DocDetails,
            D_Identifier: id,
            D_OrderNumber: docNumber,
            D_OrderedDate: new Date().getTime()
        };
        doc._id = id;

        return doc;
    } catch (error) {
        console.error(`Error updating document details: ${error.message}`);
        throw error;
    }
}

/**
 * Update contact information
 */
async function updateContacts(doc, rootDoc) {
    try {
        if (!rootDoc.Buyer || !rootDoc.Seller) {
            throw new Error(constants.ERROR_MESSAGES.MISSING_CONTACT);
        }

        doc.Buyer = rootDoc.Buyer;
        doc.Seller = rootDoc.Seller;

        return doc;
    } catch (error) {
        console.error(`Error updating contacts: ${error.message}`);
        throw error;
    }
}

/**
 * Update roles in document
 */
async function updateRoles(doc) {
    try {
        const roles = {
            Buyer: `@accounts/${doc?.Buyer?.C_Identifier}`,
            Sourcing: `@accounts/${doc?.Buyer?.C_Identifier}`,
            Design: `@accounts/${doc?.Buyer?.C_Identifier}`,
            Planners: `@accounts/${doc?.Buyer?.C_Identifier}`,
            Finance: `@accounts/${doc?.Buyer?.C_Identifier}`,
            QC: `@accounts/${doc?.Buyer?.C_Identifier}`,
            Warehouse: `@accounts/${doc?.Buyer?.C_Identifier}`,
            MasterWeaverFin: `@accounts/${doc?.Seller?.C_Identifier}`,
            Seller: `@accounts/${doc?.Seller?.C_Identifier}`
        };

        doc.roles = roles;
        doc.LocalProperties.Roles = roles;

        return doc;
    } catch (error) {
        console.error(`Error updating roles: ${error.message}`);
        throw error;
    }
}

async function getRootDoc(rootDocID) {
    console.log("RootDocID", rootDocID)
    let result = await getDocwithDocID(rootDocID, smID, "Orders", paramID, projectID, workspace, portal)
    if (!isValidArray(result)) {
        return Promise.reject(`Unable to get the root document details`)
    }
    return result[0];
}

/**
 * Create documents with retry mechanism
 */
async function createDocuments(documents, chainConfig) {
    let retries = 0;
    while (retries < config.MAX_RETRIES) {
        try {
            const metaInfo = {
                payloadType: "PRIVATE",
                txnType: "update_sm",
                stateTo: chainConfig.outputDocInfo.stateTo,
                smID: chainConfig.outputDocInfo.smID,
                props: {
                    workspaceName: workspace,
                    portal
                },
                paramID
            };

            return await sendTxn(documents, metaInfo, config.mode);
        } catch (error) {
            retries++;
            if (retries === config.MAX_RETRIES) {
                console.error(`Failed to create documents after ${config.MAX_RETRIES} retries`);
                throw error;
            }
            await new Promise(resolve => setTimeout(resolve, config.RETRY_DELAY));
        }
    }
}

/**
 * Utility function to format dates
 */
function formatDate(date) {
    if (!date) return "";
    if (typeof date === 'number') return date;
    const parsed = Date.parse(date);
    return isNaN(parsed) ? "" : parsed;
}

/**
 * Utility function to validate arrays
 */
function isValidArray(arr) {
    return Array.isArray(arr) && arr.length > 0;
}

/**
 * Delete non-schema fields from document
 */
function deleteNonSchemaFields(doc, chainConfig) {
    if (!isValidArray(chainConfig.outputDocInfo.deleteKeys)) {
        return doc;
    }

    const newDoc = { ...doc };
    for (const key of chainConfig.outputDocInfo.deleteKeys) {
        delete newDoc[key];
    }
    return newDoc;
}
