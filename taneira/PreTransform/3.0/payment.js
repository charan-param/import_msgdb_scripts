let workspace = "", portal = ""

const userID = "8e8fe7183d3cf604f011d8af54cdde9d732363263c88cea106501f37b28534bd"

config = {
  "c_config": {
    "paramid": "0x878042B8E678a1bb5532f45103C72C009a0b8424",
    "projectid": "f308748496aa304b1743",
  },
  "payment": {
    "smID": "public:0x5b5c87559104b3517df2cd795a6cef58485a17d09fec18dc44728bbb398e0e45",
    "stateTo": "Payment"
  },
  "approve": {
    "smID": "public:0x3803c415314f8202f3fd580179f4f165ee751e03f0b0d67d16476714098354b6",
    "stateTo": "Approve"
  },
  "invoice": {
    "smID": "public:0xd68c98368014f2361b8bde715a68ba6682616031cfe35b23cc6a369755982d5d",
    "stateTo": "SAPInvoice"
  },
  "grn": {
    "smID": "public:0x795b9a35ee60fa5f74e9b224f8c818e123fc4a9d4d20c721580d67142c91f39c",
    "stateTo": "GRN"
  }
}


function main() {
  console.log("PreTransform started execute..!")
  return getConfig().then((data) => {
    if (!data || !Object.keys(data).length) {
      return Promise.reject("Unable to get the Config");
    }
    return getPayload();
  }).then((payload) => {
    if (!payload || !Object.keys(payload).length) {
      return Promise.reject("Unable to get the Payload");
    }
    return processPreTransform(payload);
  }).catch((error) => {
    return Promise.reject(error.message || "An error occurred")
  });
}


function processPreTransform(payload) {
  smID = payload?.smID || "";
  stateTo = payload?.stateTo || "";
  orgParamID = payload?.orgParamID || "";
  exchangeParamID = payload?.exchangeParamID || "";

  const metaData = payload?.metaData || {};
  projectID = metaData?.projectID || "";
  workspace = metaData?.workspace || "";
  portal = metaData?.portal || "";
  let document = Array.isArray(payload?.document) ? payload.document : [payload.document || {}];

  let validationResult = []
  let errors = []
  let docInfo = []

  let doc = mergeDocument(document, errors)


  let batchIndex
  return batchProcess(doc, batchIndex, validationResult, errors, docInfo).then(result => {
    console.log("Result", result)
    return { data: result, error: errors, info: docInfo }
  }).catch(err => {
    return err
  })

}


function batchProcess(doc, batchIndex, validationResult, errors, docInfo) {
  if (!batchIndex) {
    batchIndex = 0
  }
  console.log("****Number of docs", doc.length)
  console.log("****Documents", JSON.stringify(doc))

  if (!doc || doc.length === 0) {
    console.info("Batch process completed for doc");
    return Promise.reject("Unable to get the doc.");
  }
  let currentPageStart = batchIndex * 1;
  let currentPageEnd = Math.min((batchIndex + 1) * 1, doc.length);
  console.log("CurrentPageStart", currentPageStart)
  console.log("docLength", doc.length)
  if (currentPageStart >= doc.length) {

    validationResult.map(item => {
      docInfo.push(`${item["DocDetails"]["D_OrderNumber"]}`)
    })

    console.info("Batch process completed for doc");
    console.log("Validation Result****", JSON.stringify(validationResult))
    return Promise.resolve(validationResult);
  }
  let promiseArray = [];

  console.info(`\nTotal ${doc.length} doc, Page start: ${currentPageStart} and Page end: ${currentPageEnd}`);
  for (let index = currentPageStart; index < currentPageEnd; index++) {
    let body = doc[index];
    promiseArray.push(getMatchedDoc(body, errors).then(matched => {
      if (!matched) {
        console.log(`Skipping doc: no matched invoice(s) found`);
        return null;
      }
      const { invoiceDocs, paymentRow } = matched;
      return processDocument(invoiceDocs, paymentRow, errors);
    }).then(result => {
      // Only add valid processed documents to validationResult
      if (result && typeof result === 'object' && Object.keys(result).length > 0 && result.DocDetails) {
        validationResult.push(result);
        console.log(`Successfully processed document: ${result.DocDetails?.D_OrderNumber || 'Unknown'}`);
      } else {
        console.log(`Skipped adding result - invalid or empty document`);
      }
      return validationResult
    }).catch(processingError => {
      // Handle processing errors gracefully
      console.error("Error processing document:", processingError);
      errors.push(`Document processing error: ${processingError}`);
      return validationResult; // Continue with existing results
    }));
    // }).then(result => {
    //   if (Object.keys(result || {}).length > 0) {
    //     validationResult = validationResult.concat([result]);
    //   }
    //   return validationResult
    // }));
  }
  return Promise.all(promiseArray).then(formatRes => {
    return validationResult
  }).catch(e => {
    errors.push("Batch error. Reason: ", `${e}`);
    console.error("Batch error. Reason: ", `${e}`);
    return validationResult;
  }).finally(() => {
    return batchProcess(doc, batchIndex + 1, validationResult, errors, docInfo);
  })
}


/**
 * Fetch all invoices for the payment's related invoice numbers (DocumentNumber = InvoiceNumber).
 * Returns { invoiceDocs, paymentRow } so ReferencesOrder can be built from all matched invoices.
 */
function getMatchedDoc(jsonld, errors) {
  const invoiceNumbers = jsonld["relatedInvoiceNumbers"];
  if (!invoiceNumbers || !Array.isArray(invoiceNumbers) || invoiceNumbers.length === 0) {
    errors.push("Payment row has no relatedInvoiceNumbers (DocumentNumber)");
    return Promise.resolve(null);
  }

  const paramID = config["c_config"]["paramid"];
  const projectID = config["c_config"]["projectid"];
  const smID = config["invoice"]["smID"];
  const stateTo = config["invoice"]["stateTo"];

  const fetchOne = (invNum) => {
    const filterOptions = { "DocDetails.D_OrderNumber": invNum };
    return getDocument(filterOptions, smID, stateTo, paramID, projectID, workspace, portal)
      .then(result => {
        if (!isValidArray(result)) {
          errors.push(`Unable to get the Invoice Details for Invoice: ${invNum}`);
          return null;
        }
        return result[0];
      })
      .catch(error => {
        errors.push(`Error fetching invoice ${invNum}: ${error}`);
        console.error("Error in getMatchedDoc for", invNum, error);
        return null;
      });
  };

  return Promise.all(invoiceNumbers.map(fetchOne)).then((invoiceDocs) => {
    const valid = invoiceDocs.filter(Boolean);
    if (valid.length === 0) {
      errors.push(`No matched invoices found for payment (DocumentNumbers: ${invoiceNumbers.join(", ")})`);
      return null;
    }
    console.log(`Matched ${valid.length} invoice(s) for payment: [${invoiceNumbers.join(", ")}]`);
    return { invoiceDocs: valid, paymentRow: jsonld };
  });
}

/**
 * DocumentNumber = Invoice number per row.
 * ClearingDocno = Payment number.
 * When the same payment (same ClearingDocno) has multiple rows with different DocumentNumbers (invoice numbers),
 * we group them and later set ReferencesOrder from all matched invoices.
 * Valid rows must have: TransactionType === "Invoice", ClearingDocno present, DocumentNumber (invoice number) present.
 */
function mergeDocument(documents, errors) {
  const mergedDocuments = [];
  const flat = Array.isArray(documents) && documents.length > 0 && Array.isArray(documents[0])
    ? documents.flat()
    : (Array.isArray(documents) ? documents : [documents]);

  // Group rows by payment key: same ClearingDocno = same payment (multiple invoice numbers per payment)
  const byPaymentKey = new Map();
  for (let i = 0; i < flat.length; i++) {
    const doc = flat[i];
    if (!doc || typeof doc !== 'object') {
      errors.push(`Row ${i}: Invalid document (missing or not an object); skipped`);
      continue;
    }

    // TransactionType must be "Invoice"
    const transactionType = doc["TransactionType"] != null ? String(doc["TransactionType"]).trim() : "";
    if (transactionType !== "Invoice") {
      // errors.push(`Row ${i}: TransactionType must be "Invoice", got "${transactionType || "(empty)"}"; skipped`);
      continue;
    }

    // DocumentNumber = Invoice number (required)
    const documentNumber = doc["DocumentNumber"] != null && doc["DocumentNumber"] !== ''
      ? String(doc["DocumentNumber"]).trim()
      : null;
    if (documentNumber == null) {
      errors.push(`Row ${i}: DocumentNumber (Invoice number) is missing or empty; skipped`);
      continue;
    }

    // ClearingDocno = Payment number (required)
    const clearingDocno = doc["ClearingDocno"] != null && doc["ClearingDocno"] !== ''
      ? String(doc["ClearingDocno"]).trim()
      : null;
    if (clearingDocno == null) {
      errors.push(`Row ${i}: ClearingDocno (Payment number) is missing or empty for Invoice ${documentNumber}; skipped`);
      continue;
    }

    const paymentKey = clearingDocno;

    if (!byPaymentKey.has(paymentKey)) {
      byPaymentKey.set(paymentKey, { row: { ...doc }, invoiceNumbers: [] });
    }
    const group = byPaymentKey.get(paymentKey);
    if (!group.invoiceNumbers.includes(documentNumber)) {
      group.invoiceNumbers.push(documentNumber);
    }
  }

  for (const [, group] of byPaymentKey) {
    const doc = group.row;
    doc["relatedInvoiceNumbers"] = group.invoiceNumbers;
    mergedDocuments.push(doc);
    console.log(`Payment group: ClearingDocno=${doc["ClearingDocno"]}, InvoiceNumbers=[${group.invoiceNumbers.join(", ")}]`);
  }

  if (mergedDocuments.length === 0) {
    errors.push("No valid payment rows found: each row must have TransactionType 'Invoice', ClearingDocno (Payment number), and DocumentNumber (Invoice number)");
  }

  return mergedDocuments;
}

function processDocument(invoiceDocs, jsonldPayment, errors) {
  return new Promise((resolve, reject) => {
    try {
      const mandatoryFields = ['DocumentDate'];
      const missingFields = mandatoryFields.filter(field => jsonldPayment[field] === undefined || jsonldPayment[field] === null || jsonldPayment[field] === '');
      if (!invoiceDocs || invoiceDocs.length === 0) {
        console.error("[processDocument] No invoice documents provided");
        resolve({});
        return;
      }
      if (missingFields.length > 0) {
        console.error(`[processDocument] Missing mandatory fields: ${missingFields.join(', ')}`);
        resolve({});
        return;
      }
      const firstInvoice = invoiceDocs[0];
      let jsonLd = updateAdditionalProps(jsonldPayment, firstInvoice, {}, errors);
      jsonLd = updateOrderedItems(invoiceDocs, jsonLd, errors);
      jsonLd = updateDocDetails(jsonldPayment, firstInvoice, jsonLd, errors);
      jsonLd = updateReferencesOrder(jsonldPayment, jsonLd, invoiceDocs, errors);
      jsonLd = updateSellerDetails(jsonldPayment, firstInvoice, jsonLd, errors);
      updateBuyerDetails(jsonldPayment, jsonLd, errors)
        .then(updatedJsonLd => updateSystemProperties(jsonldPayment,updatedJsonLd,errors))
        .then(updatedJsonLd => updateRoles(jsonldPayment, updatedJsonLd, errors))
        .then(finalJsonLd => {
          resolve(finalJsonLd);
        }).catch(err => {
          errors.push("Unable to process the document", err);
          reject(err);
        });
    } catch (err) {
      reject(err);
    }
  });
}


function updateAdditionalProps(data, invoiceData, jsonLd, errors) {
  const additionalProperties = {};
  additionalProperties['RangePlanID'] = data['RangePlanID'] || "";
  const firstRef = invoiceData && invoiceData['ReferencesOrder'] && invoiceData['ReferencesOrder'][0];
  additionalProperties['InvoiceNumber'] = invoiceData?.DocDetails?.D_OrderNumber ? invoiceData.DocDetails.D_OrderNumber : "";
  additionalProperties['PONumber'] = firstRef && firstRef['R_OrderNumber'] ? firstRef['R_OrderNumber'] : "";
  jsonLd['AdditionalProperties'] = additionalProperties;
  console.log("[updateAdditionalProps] AdditionalProperties Updated");
  return jsonLd;
}

/**
 * Build ReferencesOrder from all matched invoices (same payment, different invoice numbers).
 * Each invoice is added so the payment correctly references all related invoices.
 */
function updateReferencesOrder(data, jsonLd, invoiceDocs, errors) {
  const referencesOrder = (Array.isArray(invoiceDocs) ? invoiceDocs : [invoiceDocs])
    .filter(inv => inv && (inv["_id"] || (inv.DocDetails && inv.DocDetails.D_OrderNumber)))
    .map(invoiceData => ({
      "R_Identifier": invoiceData["_id"] ? invoiceData["_id"] : "",
      "R_OrderNumber": invoiceData.DocDetails && invoiceData.DocDetails.D_OrderNumber ? invoiceData.DocDetails.D_OrderNumber : "",
      "R_State": config["invoice"]["stateTo"],
      "R_SmID": config["invoice"]["smID"]
    }));
  jsonLd['ReferencesOrder'] = referencesOrder;
  console.log("[updateReferencesOrder] ReferencesOrder Updated with", referencesOrder.length, "invoice reference(s)");
  return jsonLd;
}


/**
 * Concatenate OrderedItems from all matched invoices (same payment, different invoice numbers).
 */
function updateOrderedItems(invoiceDocs, jsonLd, errors) {
  const docs = Array.isArray(invoiceDocs) ? invoiceDocs : [invoiceDocs];
  const orderedItems = docs.reduce((acc, inv) => {
    const items = inv && inv.OrderedItems && Array.isArray(inv.OrderedItems) ? inv.OrderedItems : [];
    return acc.concat(items);
  }, []);
  jsonLd['OrderedItems'] = orderedItems;
  console.log("[updateOrderedItems] OrderedItems Updated, total items:", orderedItems.length);
  return jsonLd;
}

function convertToTimestamp(dateString) {
  if (!dateString) return "";
  const [day, month, year] = dateString.split('-');
  if (day && month && year) {
    const date = new Date(`${year}-${month}-${day}`);
    return date.getTime();
  }
  return dateString;
}

function updateDocDetails(data, invoiceData, jsonLd, errors) {
  let orderedItemsCount = jsonLd.OrderedItems && Array.isArray(jsonLd.OrderedItems) ? jsonLd.OrderedItems.length : 0;
  let totalQuantity = jsonLd.OrderedItems && Array.isArray(jsonLd.OrderedItems)
    ? jsonLd.OrderedItems.reduce((sum, item) => sum + (item.I_Quantity || 0), 0)
    : 0;
  const orderNumber = data["ClearingDocno"] || data["RefDocNumber"] || data["AssignmentNumber"] ||
    (data["relatedInvoiceNumbers"] && data["relatedInvoiceNumbers"][0]) || data["DocumentNumber"] || "";
  const docIdentifier = orderNumber ? `${Crypto.getWeb3Utils().keccak256(orderNumber)}` : "";

  let docDetails = {
    "D_Identifier": docIdentifier,
    "D_OrderedDate": Date.now(),
    "D_ExpiryDate": data["D_ExpiryDate"] ? data["D_ExpiryDate"] : "",
    "D_MinimumPaymentDuePriceCurrency": "INR",
    "D_ExpectedDeliveryDate": data["ExpectedDeliveryDate"] ? data["ExpectedDeliveryDate"] : "",
    "D_PaymentTerms": "",
    "D_Type": null,
    "D_OrderNumber": orderNumber,
    "D_OrderStatus": "",
    "D_TotalPaymentDuePriceCurrency": data["D_TotalPaymentDuePriceCurrency"] ? data["D_TotalPaymentDuePriceCurrency"] : "",
    "D_MinimumPaymentDueMinPrice": 0,
    "D_TotalPaymentDueMinPrice": data["NetAmount"] ? (parseFloat(data["NetAmount"]) || 0) : "",
    "D_PaymentDueDate": "",
    "D_DeliveryAddress": "",
    "D_ItemCount": orderedItemsCount,
    "D_TotalQuantity": totalQuantity,
    "D_OffersAddOn": invoiceData?.DocDetails?.D_OffersAddOn || []
  }

  jsonLd['DocDetails'] = docDetails;
  console.log("[updateDocDetails] DocDetails Updated")
  return jsonLd;
}

function updateSystemProperties(data, jsonLd, errors) {
  // let systemProperties = {
  //   "P_SmID": config["payment"]["smID"],
  //   "P_RecordAdded": new Date().getTime(),
  // }
  let systemProperties = {}
  let buyer = jsonLd['Buyer']
  let seller = jsonLd['Seller']
  let plantIDs = {}
  if (seller && buyer) {
    plantIDs = {
      [buyer.C_Identifier]: [
        `${buyer?.C_PlantID || ""}`
      ],
      [seller.C_Identifier]: [
        `${seller?.C_PlantID || ""}`
      ]
    }
  } else if (buyer) {
    plantIDs = {
      [buyer.C_Identifier]: [
        `${buyer?.C_PlantID || ""}`
      ]
    }
  }
  systemProperties['P_PlantIDs'] = plantIDs
  systemProperties["P_SmID"] = config["payment"]["smID"]
  systemProperties["P_RecordAdded"] = new Date().getTime(),
  jsonLd['SystemProperties'] = systemProperties
  console.log("[updateSystemProperties] SystemProperties Updated")
  return jsonLd;
}

function updateBuyerDetails(data, jsonLd, errors) {
  return getProfileV1(userID).then(result => {
    if (!result) {
      errors.push(`Unable to get the Profile Details for user:${userID}`)
      return Promise.reject("Unable to get the Profile")
    }
    jsonLd['Buyer'] = result
    console.log("[updateBuyerDetails] buyerDetails Updated")
    return jsonLd
  }).catch(err => {
    return err
  })

}

function getProfileV1() {
  return Promise.resolve({
    "C_Identifier": "0xd288f265Ed01c636377ef3bA7cbdD29271A9D00B",
    "C_PenID": "HFXY1900",
    "C_Email": "admin@loomlink.com",
    "C_Organization": "TITAN COMPANY LIMITED",
    "C_Telephone": "",
    "C_Description": "",
    "C_Department": "",
    "C_TaxID": "29AAACT5131A1ZT",
    "C_StreetAddress": "NO.193, INTEGRITY Veerasandra, Electronics City P.O Off Hosur Main Road",
    "C_AddressLocality": "",
    "C_City": "Bangalore",
    "C_Region": "Karnataka",
    "C_PostalCode": "560100"
  })
}

function updateSellerDetails(data, invoiceData, jsonLd, errors) {
  let sellerDetails = invoiceData["Seller"]
  jsonLd["Seller"] = sellerDetails
  return jsonLd
}

function updateRoles(data, jsonLd, errors) {
  let roles = {};
  console.log("jsonLd Buyer:", jsonLd['Buyer']);
  console.log("jsonLd Seller:", jsonLd['Seller']);

  if (jsonLd && jsonLd['Buyer'] && jsonLd['Buyer']['C_Identifier']) {
    console.log("Updating Buyer Details")
    roles['Buyer'] = `@accounts/${jsonLd['Buyer']['C_Identifier']}`;
    roles['Sourcing'] = `@accounts/${jsonLd['Buyer']['C_Identifier']}`;
    roles['Design'] = `@accounts/${jsonLd['Buyer']['C_Identifier']}`;
    roles['Planners'] = `@accounts/${jsonLd['Buyer']['C_Identifier']}`;
    roles['Finance'] = `@accounts/${jsonLd['Buyer']['C_Identifier']}`;
    roles['QC'] = `@accounts/${jsonLd['Buyer']['C_Identifier']}`;
    roles['Warehouse'] = `@accounts/${jsonLd['Buyer']['C_Identifier']}`;
  } else {
    roles['Buyer'] = "";
  }

  if (jsonLd && jsonLd['Seller'] && jsonLd['Seller']['C_Identifier']) {
    console.log("Updating seller roles details", jsonLd['Seller']['C_Identifier'])
    roles['Seller'] = `@accounts/${jsonLd['Seller']['C_Identifier']}`;
    roles['MasterWeaverFin'] = `@accounts/${jsonLd['Seller']['C_Identifier']}`;
  } else {
    roles['Seller'] = "";
  }

  jsonLd["roles"] = roles;
  console.log("[updateRoles] roles Updated:", jsonLd["roles"]);
  return jsonLd;
}


/**
 * Utils
 */

function isValidArray(arr) {
  if (!arr || !Array.isArray(arr) || !arr.length) {
    return false;
  }
  return true;
}