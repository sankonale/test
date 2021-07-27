var globalconfig = require("../config/global.config");
const amqp = require('amqplib');
const mongodbClient = require("./mongodb.service");
const keyVault = require("../utility/keyVaultSupportUtility");
const rbmservice = require("../services/rbmServices");
var ObjectId = require('mongodb').ObjectID;
const logger = require('../utility/projectLoggerUtility');

const queueName = globalconfig.QueueName;

module.exports.connectToRabbitMQ = async () => {
    amqp.connect(globalconfig.RabbitMQUrl).then(function (conn) {
        console.log("In connect");
        process.once('SIGINT', function () { conn.close(); });
        return conn.createChannel().then(function (ch) {
            console.log("In createChannel");
            var ok = ch.assertQueue(queueName);
            ok = ok.then(function (_qok) {
                return ch.consume(queueName, async (msg) => {
                    console.log("In consume");
                    let request = JSON.parse(msg.content.toString());
                    await processQueueRequest(request);
                }, { noAck: true });
            });
        });
    }).catch(function (ex) {
        console.log("In catch");
        logger.error(ex.stack);
    });
};

module.exports.pushToRabbitMQ = function pushToRabbitMQ(data) {
    return amqp.connect(globalconfig.RabbitMQUrl).then(function (conn) {
        return conn.createChannel();
    }).then(function (ch) {
        return ch.assertQueue(queueName).then(function (ok) {
            return ch.sendToQueue(queueName, Buffer.from(data));
        });
    }).catch(function (ex) {
        throw ex;
    });
};

let processQueueRequest = async (data) => {
    switch (data.ACTION_NAME) {
        case "UPSERT_DEVICE_STATUS":
            await processUpsertDeviceStatusRequest(data);
            break;
    }
    return true;
};

let processUpsertDeviceStatusRequest = async (data) => {
    try {
        //check and connect db if not
        await mongodbClient.connectDB();

        if (data.status == "active") {
            for (let i in data.devices) {
                await updateProjectdeviceStatus(data.devices[i]);
            }
        }
        else {

            //call the rbm Get Order API passing the Ordernumber and fetch the SubcriptionNumber from response of this API
            var result = await rbmservice.getOrderDetails(data.orderNumber);
            if (result.success) {

                if (result.order.subscriptions == undefined || result.order.subscriptions == null) {
                    throw new Error("Subscriptions not found for order number :" + data.orderNumber);
                }
                else if (result.order.subscriptions.length > 1) {
                    throw new Error("Multiple subscriptions found for order number :" + data.orderNumber);
                }
                else {

                    let ncOrderNumber = result.order.customFields.ncOrderNum__c;
                    let subscriptionNumber = result.order.subscriptions[0].subscriptionNumber;

                    if (ncOrderNumber == null || ncOrderNumber == undefined || ncOrderNumber == '') {
                        throw new Error("ncOrderNumber not found in rbm order.");
                    }
                    else if (subscriptionNumber == null || subscriptionNumber == undefined || subscriptionNumber == '') {
                        throw new Error("subscriptionNumber not found in rbm order.");
                    }

                    //Using the subscription Id find out the projectId from project table
                    let projectId = await getProjectIdByConditions({ ncOrderNumber: ncOrderNumber });

                    // if ncOrderNumber not in project table then check projectOrder table 
                    if (projectId == null)
                        projectId = await getProjectIdFromProjectOrders({ ncOrderNumber: ncOrderNumber });


                    if (projectId != null) {

                        let productRatePlanIds = [];
                        //Validate model against order
                        result.order.subscriptions[0].orderActions.forEach(orderAction => {
                            if (orderAction.type == "AddProduct") {
                                productRatePlanIds.push(orderAction.addProduct.productRatePlanId);
                            }
                            else {
                                orderAction.createSubscription.subscribeToRatePlans.forEach(element => {
                                    productRatePlanIds.push(element.productRatePlanId);
                                });
                            }
                        });

                        let seagateModelsInOrder = [];
                        for (let p in productRatePlanIds) {
                            let ratePlanResult = await rbmservice.getProductRatePlanDetails(productRatePlanIds[p]);
                            let productCatalogDetails = await getProductDetailsByConditions({ productID: ratePlanResult.ProductId });
                            seagateModelsInOrder.push(productCatalogDetails.seagateModelNumber);
                        }

                        let requestDeviceCount = 0;
                        //check all seagate model in database
                        for (let k in data.devices) {
                            let productId = await getProductIdByConditions({ seagateModelNumber: k });
                            if (productId == null) {
                                throw new Error("Device model number not found in database.");
                            }
                            else if (seagateModelsInOrder.indexOf(k) < 0) {
                                throw new Error("Device model number not found in order.");
                            }
                            else {
                                for (let j in data.devices[k]) {
                                    let serialNumber = data.devices[k][j];
                                    let projectDeviceDetails = await getProjectDeviceDetailsByConditions({ serialnumber: serialNumber, projectid: ObjectId(projectId) });
                                    if (projectDeviceDetails != null && projectDeviceDetails.status == "returned" && data.status == "shipped") {
                                        throw new Error("Device already present in database with returned status.");
                                    }
                                    requestDeviceCount += 1;
                                }
                            }
                        }
                        
                        // let rbmOrderCount = 0;
                        // let subscriptionResponse =await rbmservice.getSubscriptionDetails(subscriptionNumber);
                        // const { ratePlans } = subscriptionResponse;
                        // for (const ratePlanID of productRatePlanIds) {
                        //     const searchResponse =await search(ratePlanID, ratePlans);
                        //     let qty = 0;
                        //     for (const ratePlanCharges of searchResponse.ratePlanCharges) {
                        //         if (ratePlanCharges.model === "PerUnit") {
                        //             qty = ratePlanCharges.quantity;
                        //         }
                        //     }
                        //     rbmOrderCount += qty;
                        // }

                        // //get database device count for that order
                        // let dbOrderDevicesCount = await getOrderDevicesCount(projectId, data.orderNumber);
                        // if ((dbOrderDevicesCount + requestDeviceCount) > rbmOrderCount) {
                        //     throw new Error("Device count match with rbm.");
                        // }

                        for (let i in data.devices) {

                            //get product id from product catalog
                            let productId = await getProductIdByConditions({ seagateModelNumber: i });

                            for (let j in data.devices[i]) {

                                let serialNumber = data.devices[i][j];
                                //let projectDeviceId = null;

                                let projectDeviceDetails = await getProjectDeviceDetailsByConditions({ serialnumber: serialNumber, projectid: ObjectId(projectId) });

                                let projectdevice = {
                                    serialnumber: serialNumber,
                                    status: data.status,
                                    isActive: (data.status == "shipped" || data.status == "returned" ? false : true),
                                    orderReason: data.reason,
                                    trackingNumber: data.trackingNumber,
                                };
                                let todaydate = new Date();
                                if (data.status == "returned")
                                    projectdevice.returnDate = todaydate;
                                else if (data.status == "shipped")
                                    projectdevice.shippedDate = todaydate;

                                if (projectDeviceDetails != null) {
                                    projectdevice.modifiedBy = "system";
                                    projectdevice.modifiedDate = todaydate;
                                    await updateProjectdevice(projectdevice, projectDeviceDetails._id);
                                }
                                else {
                                    projectdevice.projectid = projectId;
                                    projectdevice.rbmOrderID = data.orderNumber;
                                    projectdevice.rbmSubscriptionID = subscriptionNumber;
                                    projectdevice.rbmProductID = productId;
                                    projectdevice.createdBy = "system";
                                    projectdevice.createdDate = new Date();
                                    await createProjectdevice(projectdevice);
                                }
                            }
                        }
                    }
                    else {
                        throw new Error("projectId not found in database.");
                    }
                }
            }
            else {
                throw new Error(JSON.stringify(result.reasons));
            }
        }
    }
    catch (ex) {
        await logger.error(ex.stack, JSON.stringify(data));
    }
};

let createProjectdevice = async (projectdevices) => {
    let dbo = mongodbClient.db;
    return await dbo.collection("projectdevices").insertOne(projectdevices);
};

let getProjectIdByConditions = async (conditions) => {
    let dbo = mongodbClient.db;
    let result = await dbo.collection('project')
        .findOne(conditions);
    return result != null ? result._id : null;
};

let getProjectIdFromProjectOrders = async (conditions) => {
    let dbo = mongodbClient.db;
    let result = await dbo.collection('projectOrders')
        .findOne(conditions);
    return result != null ? result.projectID : null;
};

let getProductIdByConditions = async (conditions) => {
    let dbo = mongodbClient.db;
    let result = await dbo.collection('productCatalog')
        .findOne(conditions);
    return result != null ? result.productID : null;
};

let getProductDetailsByConditions = async (conditions) => {
    let dbo = mongodbClient.db;
    let result = await dbo.collection('productCatalog')
        .findOne(conditions);
    return result;
};

let getProjectDeviceDetailsByConditions = async (conditions) => {
    let dbo = mongodbClient.db;
    let result = await dbo.collection('projectdevices')
        .findOne(conditions);
    return result;
};

let updateProjectdevice = async (data, projectDeviceId) => {
    let dbo = mongodbClient.db;
    let objForUpdate = { $set: data }
    return await dbo.collection('projectdevices')
        .updateMany({ serialnumber: data.serialnumber, _id: ObjectId(projectDeviceId) },
            objForUpdate
        );
};

let updateProjectdeviceStatus = async (serialNumbers) => {
    let dbo = mongodbClient.db;
    return await dbo.collection('projectdevices')
        .updateMany({ serialnumber: { $in: serialNumbers }, status: 'shipped' },
            {
                $set: {
                    status: "active",
                    isActive: true,
                    modifiedBy: "system",
                    modifiedDate: new Date()
                }
            }
        );
};

let getOrderDevicesCount = async (projectid, rbmOrderNumber) => {
    let dbo = mongodbClient.db;
    return await dbo.collection("projectdevices").find({ projectid: projectid, rbmOrderID: rbmOrderNumber })
        .count();
};

let search = async (nameKey, myArray) =>{
    for (let i = 0; i < myArray.length; i++) {
        if (myArray[i].productRatePlanId === nameKey) {
            return myArray[i];
        }
    }
};
