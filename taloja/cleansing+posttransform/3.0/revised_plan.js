const fs = require('fs');
const csv = require('csv-parser');
const config = require('../config.json');
const dateUtils = require('../utils/index')
const csvUtils = require('../utils/csv');
const DI = require("../diservices");
const { MongoClient } = require("mongodb");
const Web3 = require('web3').default;
const web3 = new Web3();

class revised_plan {

    static convertRevisedPlanCustomerCSVToParamCSV(payload, res, importedData,dbname) {
        let revised = {}
        const result = [];
        console.log("convertRevisedPlanCustomerCSVToParamCSV - Function called")
        console.log("payload", JSON.stringify(payload))
        const month = new Date().getUTCMonth();
        const year = new Date().getFullYear();
        let importedMonth = importedData.importedMonth
        let importedYear = importedData.importedYear ? importedData.importedYear : new Date().getFullYear()
        let importedDate = importedData.importedDate ? importedData.importedDate : new Date().getDate()
        const globalRegex = new RegExp('(((RW|W)[0-9])(\s*.+))', 'g');
        return new Promise((resolve, reject) => {
            // fs.createReadStream(req.file.path)
            // .pipe(csv())
            // .on('data', (row) => {
            for (let row of payload) {
                try {
                    row = dateUtils.formatJSONObj(row);
                    if (!row || !Object.keys(row).length) {
                        return reject("Invalid file content")
                    }

                    let weeks = { "W1": "", "W2": "", "W3": "", "W4": "", "W5": "" }
                    const rowKeys = Object.keys(row);
                    for (let rIndex in rowKeys) {
                        try {
                            if (!rowKeys[rIndex]) {
                                continue;
                            }
                            const machedString = [...rowKeys[rIndex].matchAll(globalRegex)];
                            if (!machedString || machedString.length !== 1 || !machedString[0] || machedString[0].length !== 5) {
                                continue;
                            }
                            weeks[machedString[0][2]] = row[rowKeys[rIndex]] ? row[rowKeys[rIndex]] : ""
                        } catch (e) {
                            console.error("Unable to parse the week, Reason: ", e);
                        }
                    }
                    row = Object.assign(row, weeks)
                    let gade = ' '
                    if (row['Grade'])
                        gade = row['Grade']
                    if (row['Gade'])
                        gade = row['Gade']
                    let {
                        'Unit': unit,
                        'Thk': thk,
                        'Width': width,
                        "RW1": w1,
                        "RW2": w2,
                        "RW3": w3,
                        "RW4": w4,
                        "RW5": w5,
                        'Rtotal': total,
                        'Backlog': backlog
                    } = row;
                    if (!width) {
                        width = ""
                    }
                    if (!thk) {
                        thk = ""
                    }
                    // try {
                    let orderedItemsIName = '';
                    if (!unit || !gade) {
                        return null;
                    }
                    width = width && width.trim() && width.replace(",", "")
                    thk = thk ? thk.trim() : ""
                    thk = dateUtils.addZeros(thk)
                    if (unit && unit.trim().toLowerCase() === 'unit 1' && gade && gade.trim().toLowerCase() === 'lpa') {
                        orderedItemsIName = `R468P10-1050H18-0-${width.trim()}-${thk}`;
                    } else if (unit && unit.trim().toLowerCase() === 'p5') {
                        if (gade.trim().toLowerCase() === 'lph hi-uts') {
                            orderedItemsIName = `R46E810-1050H18-0-${width.trim()}-${thk}`;
                        } else if (gade.trim().toLowerCase() === 'v1s') {
                            orderedItemsIName = `R46E810-VISH18-0-${width.trim()}-${thk}`;
                        } else if (gade && gade.trim().toLowerCase() === 'lpa') {
                            orderedItemsIName = `R468P10-1050H18-0-${width.trim()}-${thk}`
                        }
                    }
                    if (!orderedItemsIName) {
                        return;
                    }
                    w1 = (!w1 || w1 == 0) ? 0 : w1 ? w1.trim() == '-' ? 0 : parseInt(w1.replace(/,/g, '')) : 0
                    w2 = (!w2 || w2 == 0) ? 0 : w2 ? w2.trim() == '-' ? 0 : parseInt(w2.replace(/,/g, '')) : 0
                    w3 = (!w3 || w3 == 0) ? 0 : w3 ? w3.trim() == '-' ? 0 : parseInt(w3.replace(/,/g, '')) : 0
                    w4 = (!w4 || w4 == 0) ? 0 : w4 ? w4.trim() == '-' ? 0 : parseInt(w4.replace(/,/g, '')) : 0
                    w5 = (!w5 || w5 == 0) ? 0 : w5 ? w5.trim() == '-' ? 0 : parseInt(w5.replace(/,/g, '')) : 0
                    // w1 = w1.trim() === '-' ? 0 : parseInt(w1.replace(/,/g, ''));
                    // w2 = w2.trim() === '-' ? 0 : parseInt(w2.replace(/,/g, ''));
                    // w3 = w3.trim() === '-' ? 0 : parseInt(w3.replace(/,/g, ''));
                    // w4 = w4.trim() === '-' ? 0 : parseInt(w4.replace(/,/g, ''));
                    // w5 = w5.trim() === '-' ? 0 : parseInt(w5.replace(/,/g, ''));

                    const newRow = {
                        "DocDetails.D_OrderNumber": "RP" + "_" + importedMonth + "_" + importedYear,
                        "DocDetails.D_OrderedDate": dateUtils.getOrderedDateFormat(importedYear, importedMonth, "02"),
                        "ReferencesOrder.R_OrderNumber": "TP" + "_" + importedMonth + "_" + importedYear,
                        'OrderedItems.I_AdditionalProperties.IA_CustomerPlantRef': unit ? unit.trim() : "",
                        'OrderedItems.I_Grade': gade ? gade.trim() : "",
                        'OrderedItems.I_Thickness': thk ? thk : "",
                        'OrderedItems.I_ScheduleQuantity': `${w1},${w2},${w3},${w4},${w5}`,
                        'OrderedItems.I_Width': width ? parseInt(width) : 0,
                        'OrderedItems.I_Week1': w1,
                        'OrderedItems.I_Week2': w2,
                        'OrderedItems.I_Week3': w3,
                        'OrderedItems.I_Week4': w4,
                        'OrderedItems.I_Week5': w5,
                        'Order': thk,
                        'Width': width,
                        'OrderedItems.I_Quantity': total && total.trim() !== '-' ? parseInt(total.replace(/,/g, '')) : 0,
                        'OrderedItems.I_Name': orderedItemsIName,
                        'OrderedItems.I_Number': orderedItemsIName,
                        "EventSchedule.E_StartDate": dateUtils.getSatrtDate(new Date(`${year}-${importedMonth}-${new Date().getDate()}`)),
                        "EventSchedule.E_EndDate": dateUtils.getEndDate(new Date(`${year}-${importedMonth}-${new Date().getDate()}`)),
                        "OrderedItems.I_BacklogQuantity": backlog && backlog.trim() === '-' ? null : backlog ? parseInt(backlog.replace(/,/g, '')) : 0,
                        ...config.revisedplan
                    };
                    console.log(newRow)
                    result.push(newRow);
                } catch (e) {
                    console.log("Error parsing row: ", e);
                    return reject(`Error While Parsing the data Reason: ${e}`);
                }
            }
            if (!result.length) return reject("Invalid JSON File");

            const formattedJson = this.convertJson(result);
            resolve(formattedJson);
        }).then((result) => {
            revised = result;
            // const clonedResult = JSON.parse(JSON.stringify(result));

            const smID = config?.statemachine?.tentativeplan?.smID || "";
            const stateTo = config?.statemachine?.tentativeplan?.stateTo || "";
            const diConfig = config?.statemachine?.tentativeplan?.DI_Config || {};

            const orderNumber = revised?.ReferencesOrder?.[0]?.R_OrderNumber;
            return this.getTentativePlanByOrderNumber(orderNumber,dbname);
        }).then((data) => {
            if (!data || data.length === 0) {
                return Promise.reject("Revised Plan Parent doc Not Found");
            }
            const result = revised;

            const txnid = data[0]?.LocalProperties?.TxnID;

            const smID = config?.statemachine?.revisedplan?.smID || "";
            const stateTo = config?.statemachine?.revisedplan?.stateTo || "";
            const diConfig = config?.statemachine?.revisedplan?.DI_Config || {};

            result.SystemProperties = result.SystemProperties || {};
            result["DocDetails"]["D_Identifier"] = web3.utils.keccak256(result["DocDetails"]["D_OrderNumber"]);
            result._id = result.DocDetails.D_Identifier
            result.SystemProperties.P_SmID = smID;
            result.SystemProperties.P_StateTo = stateTo;
            result.SystemProperties.P_RecordAdded = new Date().getTime();
            result.SystemProperties.P_DocOwner = result.Buyer.C_Identifier;
            result.SystemProperties.P_PlantIDs = {
                [result.Buyer.C_Identifier]: [""],
                [result.Seller.C_Identifier]: [""]
            }
            result.Buyer = config.buyer
            result.Seller = config.seller

            if (!diConfig || !Object.keys(diConfig).length) {
                console.log("Encryption Config is Empty");
                return Promise.reject("Encryption Config is Empty");
            }

            console.log("Calling Encryption");
            return DI.encryptAPI(result, diConfig, smID, stateTo, txnid);
        }).catch(err => {
            return Promise.reject(err)
        })
    }


    static convertJson(flatArray) {
        if (!Array.isArray(flatArray)) return flatArray;

        const structuredResult = {
            DocDetails: {},
            OrderedItems: [],
            ReferencesOrder: [],
            EventSchedule: {},
            Seller: {},
            Buyer: {},
            SystemProperties: {}
        };

        // Utility function to deeply set nested fields
        const setNestedValue = (obj, path, value) => {
            const keys = path.split(".");
            let current = obj;

            for (let i = 0; i < keys.length - 1; i++) {
                const k = keys[i];
                if (!(k in current)) current[k] = {};
                current = current[k];
            }

            current[keys[keys.length - 1]] = value;
        };

        for (const item of flatArray) {
            const orderedItem = {};

            for (const key in item) {
                const value = item[key];

                if (key.startsWith("DocDetails.")) {
                    const subPath = key.slice("DocDetails.".length);
                    setNestedValue(structuredResult.DocDetails, subPath, value);

                } else if (key.startsWith("OrderedItems.")) {
                    const subPath = key.slice("OrderedItems.".length);
                    setNestedValue(orderedItem, subPath, value);

                } else if (key.startsWith("ReferencesOrder.")) {
                    const subPath = key.slice("ReferencesOrder.".length);
                    structuredResult.ReferencesOrder[0] = structuredResult.ReferencesOrder[0] || {};
                    setNestedValue(structuredResult.ReferencesOrder[0], subPath, value);

                } else if (key.startsWith("EventSchedule.")) {
                    const subPath = key.slice("EventSchedule.".length);
                    setNestedValue(structuredResult.EventSchedule, subPath, value);

                } else if (key.startsWith("Seller.")) {
                    const subPath = key.slice("Seller.".length);
                    setNestedValue(structuredResult.Seller, subPath, value);

                } else if (key.startsWith("Buyer.")) {
                    const subPath = key.slice("Buyer.".length);
                    setNestedValue(structuredResult.Buyer, subPath, value);
                }
            }

            if (orderedItem.I_Number) {
                orderedItem.I_Identifier = web3.utils.keccak256(orderedItem.I_Number);
            }

            structuredResult.OrderedItems.push(orderedItem);
        }

        return structuredResult;
    }

    // static convertJson(flatArray) {
    //     if (!Array.isArray(flatArray)) return flatArray;

    //     const structuredResult = {
    //         DocDetails: {},
    //         OrderedItems: [],
    //         ReferencesOrder: [],
    //         EventSchedule: {},
    //         Seller: {},
    //         Buyer: {},
    //         SystemProperties: {}
    //     };

    //     for (const item of flatArray) {
    //         const orderedItem = {};

    //         for (const key in item) {
    //             const value = item[key];

    //             if (key.startsWith("DocDetails.")) {
    //                 const subKey = key.split(".")[1];
    //                 structuredResult.DocDetails[subKey] = value;
    //             } else if (key.startsWith("OrderedItems.")) {
    //                 const subKey = key.split(".")[1];
    //                 orderedItem[subKey] = value;
    //             } else if (key.startsWith("ReferencesOrder.")) {
    //                 const subKey = key.split(".")[1];
    //                 structuredResult.ReferencesOrder[0] = structuredResult.ReferencesOrder[0] || {};
    //                 structuredResult.ReferencesOrder[0][subKey] = value;
    //             } else if (key.startsWith("EventSchedule.")) {
    //                 const subKey = key.split(".")[1];
    //                 structuredResult.EventSchedule[subKey] = value;
    //             } else if (key.startsWith("Seller.")) {
    //                 const subKey = key.split(".")[1];
    //                 structuredResult.Seller[subKey] = value;
    //             } else if (key.startsWith("Buyer.")) {
    //                 const subKey = key.split(".")[1];
    //                 structuredResult.Buyer[subKey] = value;
    //             }
    //         }

    //         structuredResult.OrderedItems.push(orderedItem);
    //     }

    //     return structuredResult;
    // }

/**
 * Retrieves the tentative plan document from the database by order number.
 *
 * @param {string} orderNumber - The order number to search for in the database.
 * @param {string} dbname - The name of the database to connect to.
 * @returns {Promise<Array>} - A promise that resolves to an array of documents matching the order number.
 * @throws {Error} - If the connection to the database fails or if an error occurs during the query.
 */

    static getTentativePlanByOrderNumber(orderNumber,dbname) {
        const uri = config.mongoURI;
        const client = new MongoClient(uri, { useUnifiedTopology: true });

        return client.connect().then(() => {
            const db = client.db(dbname);
            const collection = db.collection(config.statemachine.tentativeplan.collection);
            return collection.find({ "DocDetails.D_OrderNumber": orderNumber }).toArray();
        }).then((data) => {
            return client.close().then(() => data);
        }).catch((err) => {
            return client.close().then(() => Promise.reject(err));
        });
    }

}
module.exports = revised_plan;
