/* This work is licensed under a Creative Commons CCZero 1.0 Universal License.
 * See http://creativecommons.org/publicdomain/zero/1.0/ for more information. */

#include <open62541/plugin/log_stdout.h>
#include <open62541/server.h>
#include <open62541/server_config_default.h>
#include <open62541/server_pubsub.h>
#include <open62541/types.h>
#include <open62541/pubsub.h>

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <time.h>

UA_NodeId connectionIdent, publishedDataSetIdent, writerGroupIdent, dataSetWriterIdent;
UA_Boolean running = true;

/* Node IDs for our telemetry variables */
UA_NodeId tempNodeId, pressNodeId, vibNodeId;
int tick_counter = 0;

static void stopHandler(int sign) {
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "received ctrl-c");
    running = false;
}

static void
addPubSubConnection(UA_Server *server, const char *hub_name, const char *device_id) {
    UA_PubSubConnectionConfig connectionConfig;
    memset(&connectionConfig, 0, sizeof(connectionConfig));
    connectionConfig.name = UA_STRING("Azure IoT Hub MQTT Connection");
    connectionConfig.transportProfileUri = UA_STRING("http://opcfoundation.org/UA-Profile/Transport/pubsub-mqtt-json");
    connectionConfig.enabled = UA_TRUE;
    
    UA_NetworkAddressUrlDataType networkAddressUrl = {UA_STRING_NULL , UA_STRING_NULL};
    char url[256];
    snprintf(url, sizeof(url), "opc.mqtt://127.0.0.1:1883");
    networkAddressUrl.url = UA_String_fromChars(url);
    
    UA_Variant_setScalar(&connectionConfig.address, &networkAddressUrl,
                         &UA_TYPES[UA_TYPES_NETWORKADDRESSURLDATATYPE]);

    connectionConfig.publisherId.idType = UA_PUBLISHERIDTYPE_STRING;
    connectionConfig.publisherId.id.string = UA_String_fromChars(device_id);

    /* MQTT TLS specific properties */
    UA_Boolean useTLS = UA_FALSE;
    UA_KeyValueMap_setScalar(&connectionConfig.connectionProperties, UA_QUALIFIEDNAME(0, "mqttUseTLS"), &useTLS, &UA_TYPES[UA_TYPES_BOOLEAN]);

    UA_String caFile = UA_STRING("/certs/azure_root_ca.pem");
    UA_KeyValueMap_setScalar(&connectionConfig.connectionProperties, UA_QUALIFIEDNAME(0, "mqttCaFilePath"), &caFile, &UA_TYPES[UA_TYPES_STRING]);

    UA_String certFile = UA_STRING("/certs/device.crt");
    UA_KeyValueMap_setScalar(&connectionConfig.connectionProperties, UA_QUALIFIEDNAME(0, "mqttClientCertPath"), &certFile, &UA_TYPES[UA_TYPES_STRING]);

    UA_String keyFile = UA_STRING("/certs/device.key");
    UA_KeyValueMap_setScalar(&connectionConfig.connectionProperties, UA_QUALIFIEDNAME(0, "mqttClientKeyPath"), &keyFile, &UA_TYPES[UA_TYPES_STRING]);

    UA_String mqttClientId = UA_String_fromChars(device_id);
    UA_KeyValueMap_setScalar(&connectionConfig.connectionProperties, UA_QUALIFIEDNAME(0, "mqttClientId"), &mqttClientId, &UA_TYPES[UA_TYPES_STRING]);

    char username[256];
    snprintf(username, sizeof(username), "%s/%s/?api-version=2021-04-12", hub_name, device_id);
    UA_String mqttUsername = UA_String_fromChars(username);
    UA_KeyValueMap_setScalar(&connectionConfig.connectionProperties, UA_QUALIFIEDNAME(0, "mqttUsername"), &mqttUsername, &UA_TYPES[UA_TYPES_STRING]);

    UA_Server_addPubSubConnection(server, &connectionConfig, &connectionIdent);
    UA_KeyValueMap_clear(&connectionConfig.connectionProperties);
    UA_String_clear(&networkAddressUrl.url);
    UA_String_clear(&connectionConfig.publisherId.id.string);
    UA_String_clear(&mqttClientId);
    UA_String_clear(&mqttUsername);
}

static void
addPublishedDataSet(UA_Server *server) {
    UA_PublishedDataSetConfig publishedDataSetConfig;
    memset(&publishedDataSetConfig, 0, sizeof(UA_PublishedDataSetConfig));
    publishedDataSetConfig.publishedDataSetType = UA_PUBSUB_DATASET_PUBLISHEDITEMS;
    publishedDataSetConfig.name = UA_STRING("Demo PDS");
    UA_Server_addPublishedDataSet(server, &publishedDataSetConfig, &publishedDataSetIdent);
}

static void
addVariableNodes(UA_Server *server) {
    UA_VariableAttributes attr = UA_VariableAttributes_default;
    UA_Double init_val = 0.0;
    UA_Variant_setScalar(&attr.value, &init_val, &UA_TYPES[UA_TYPES_DOUBLE]);
    attr.dataType = UA_TYPES[UA_TYPES_DOUBLE].typeId;
    attr.accessLevel = UA_ACCESSLEVELMASK_READ | UA_ACCESSLEVELMASK_WRITE;
    
    /* Temperature */
    attr.displayName = UA_LOCALIZEDTEXT("en-US", "Temperature");
    tempNodeId = UA_NODEID_STRING(1, "Temperature");
    UA_Server_addVariableNode(server, tempNodeId,
                              UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER),
                              UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
                              UA_QUALIFIEDNAME(1, "Temperature"),
                              UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),
                              attr, NULL, NULL);

    /* Pressure */
    attr.displayName = UA_LOCALIZEDTEXT("en-US", "Pressure");
    pressNodeId = UA_NODEID_STRING(1, "Pressure");
    UA_Server_addVariableNode(server, pressNodeId,
                              UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER),
                              UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
                              UA_QUALIFIEDNAME(1, "Pressure"),
                              UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),
                              attr, NULL, NULL);

    /* Vibration */
    attr.displayName = UA_LOCALIZEDTEXT("en-US", "Vibration");
    vibNodeId = UA_NODEID_STRING(1, "Vibration");
    UA_Server_addVariableNode(server, vibNodeId,
                              UA_NODEID_NUMERIC(0, UA_NS0ID_OBJECTSFOLDER),
                              UA_NODEID_NUMERIC(0, UA_NS0ID_ORGANIZES),
                              UA_QUALIFIEDNAME(1, "Vibration"),
                              UA_NODEID_NUMERIC(0, UA_NS0ID_BASEDATAVARIABLETYPE),
                              attr, NULL, NULL);
}

static void
addDataSetFields(UA_Server *server) {
    UA_DataSetFieldConfig dataSetFieldConfig;
    
    /* Temperature Field */
    memset(&dataSetFieldConfig, 0, sizeof(UA_DataSetFieldConfig));
    dataSetFieldConfig.dataSetFieldType = UA_PUBSUB_DATASETFIELD_VARIABLE;
    dataSetFieldConfig.field.variable.fieldNameAlias = UA_STRING("Temperature");
    dataSetFieldConfig.field.variable.promotedField = false;
    dataSetFieldConfig.field.variable.publishParameters.publishedVariable = tempNodeId;
    dataSetFieldConfig.field.variable.publishParameters.attributeId = UA_ATTRIBUTEID_VALUE;
    UA_Server_addDataSetField(server, publishedDataSetIdent, &dataSetFieldConfig, NULL);

    /* Pressure Field */
    memset(&dataSetFieldConfig, 0, sizeof(UA_DataSetFieldConfig));
    dataSetFieldConfig.dataSetFieldType = UA_PUBSUB_DATASETFIELD_VARIABLE;
    dataSetFieldConfig.field.variable.fieldNameAlias = UA_STRING("Pressure");
    dataSetFieldConfig.field.variable.promotedField = false;
    dataSetFieldConfig.field.variable.publishParameters.publishedVariable = pressNodeId;
    dataSetFieldConfig.field.variable.publishParameters.attributeId = UA_ATTRIBUTEID_VALUE;
    UA_Server_addDataSetField(server, publishedDataSetIdent, &dataSetFieldConfig, NULL);

    /* Vibration Field */
    memset(&dataSetFieldConfig, 0, sizeof(UA_DataSetFieldConfig));
    dataSetFieldConfig.dataSetFieldType = UA_PUBSUB_DATASETFIELD_VARIABLE;
    dataSetFieldConfig.field.variable.fieldNameAlias = UA_STRING("Vibration");
    dataSetFieldConfig.field.variable.promotedField = false;
    dataSetFieldConfig.field.variable.publishParameters.publishedVariable = vibNodeId;
    dataSetFieldConfig.field.variable.publishParameters.attributeId = UA_ATTRIBUTEID_VALUE;
    UA_Server_addDataSetField(server, publishedDataSetIdent, &dataSetFieldConfig, NULL);
}

static void
updateDataCallback(UA_Server *server, void *data) {
    tick_counter++;
    UA_Boolean anomaly = (tick_counter % 20 == 0); /* Anomaly every ~40 seconds (20 ticks of 2s) */
    
    UA_Double temp = 40.0 + ((double)rand() / RAND_MAX) * 10.0;
    UA_Double press = 100.0 + ((double)rand() / RAND_MAX) * 20.0;
    UA_Double vib = 0.5 + ((double)rand() / RAND_MAX) * 1.0;
    
    if (anomaly) {
        temp += 20.0 + ((double)rand() / RAND_MAX) * 20.0;
        vib += 3.0 + ((double)rand() / RAND_MAX) * 3.0;
        UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "INJECTING ANOMALY: High Temp/Vib");
    }

    UA_Variant value;
    UA_Variant_init(&value);
    
    UA_Variant_setScalar(&value, &temp, &UA_TYPES[UA_TYPES_DOUBLE]);
    UA_Server_writeValue(server, tempNodeId, value);
    
    UA_Variant_setScalar(&value, &press, &UA_TYPES[UA_TYPES_DOUBLE]);
    UA_Server_writeValue(server, pressNodeId, value);
    
    UA_Variant_setScalar(&value, &vib, &UA_TYPES[UA_TYPES_DOUBLE]);
    UA_Server_writeValue(server, vibNodeId, value);
}

static void
addWriterGroup(UA_Server *server, const char *device_id) {
    UA_WriterGroupConfig writerGroupConfig;
    memset(&writerGroupConfig, 0, sizeof(UA_WriterGroupConfig));
    writerGroupConfig.name = UA_STRING("Demo WriterGroup");
    writerGroupConfig.publishingInterval = 2000; /* publish every 2 seconds */
    writerGroupConfig.writerGroupId = 100;
    writerGroupConfig.encodingMimeType = UA_PUBSUB_ENCODING_JSON;
    
    char topic[256];
    snprintf(topic, sizeof(topic), "devices/%s/messages/events/", device_id);
    
    UA_BrokerWriterGroupTransportDataType brokerTransportSettings;
    memset(&brokerTransportSettings, 0, sizeof(UA_BrokerWriterGroupTransportDataType));
    brokerTransportSettings.queueName = UA_String_fromChars(topic);
    brokerTransportSettings.requestedDeliveryGuarantee = UA_BROKERTRANSPORTQUALITYOFSERVICE_ATMOSTONCE;

    UA_ExtensionObject transportSettings;
    memset(&transportSettings, 0, sizeof(UA_ExtensionObject));
    transportSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    transportSettings.content.decoded.type = &UA_TYPES[UA_TYPES_BROKERWRITERGROUPTRANSPORTDATATYPE];
    transportSettings.content.decoded.data = &brokerTransportSettings;
    writerGroupConfig.transportSettings = transportSettings;

    UA_JsonWriterGroupMessageDataType jsonWGMConfig;
    memset(&jsonWGMConfig, 0, sizeof(UA_JsonWriterGroupMessageDataType));
    jsonWGMConfig.networkMessageContentMask = (UA_JsonNetworkMessageContentMask)(
        UA_JSONNETWORKMESSAGECONTENTMASK_NETWORKMESSAGEHEADER |
        UA_JSONNETWORKMESSAGECONTENTMASK_DATASETMESSAGEHEADER |
        UA_JSONNETWORKMESSAGECONTENTMASK_SINGLEDATASETMESSAGE |
        UA_JSONNETWORKMESSAGECONTENTMASK_PUBLISHERID |
        UA_JSONNETWORKMESSAGECONTENTMASK_DATASETCLASSID);

    UA_ExtensionObject messageSettings;
    memset(&messageSettings, 0, sizeof(UA_ExtensionObject));
    messageSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    messageSettings.content.decoded.type = &UA_TYPES[UA_TYPES_JSONWRITERGROUPMESSAGEDATATYPE];
    messageSettings.content.decoded.data = &jsonWGMConfig;
    writerGroupConfig.messageSettings = messageSettings;

    UA_Server_addWriterGroup(server, connectionIdent, &writerGroupConfig, &writerGroupIdent);
    UA_String_clear(&brokerTransportSettings.queueName);
}

static void
addDataSetWriter(UA_Server *server, const char *device_id) {
    UA_DataSetWriterConfig dataSetWriterConfig;
    memset(&dataSetWriterConfig, 0, sizeof(UA_DataSetWriterConfig));
    dataSetWriterConfig.name = UA_STRING("Demo DataSetWriter");
    dataSetWriterConfig.dataSetWriterId = 62541;
    dataSetWriterConfig.keyFrameCount = 10;

    UA_BrokerDataSetWriterTransportDataType brokerTransportSettings;
    memset(&brokerTransportSettings, 0, sizeof(UA_BrokerDataSetWriterTransportDataType));
    /* Do NOT set queueName here to prevent sending DataSetMetaData (which uses Retain=1 and breaks Azure IoT Hub) */

    UA_ExtensionObject transportSettings;
    memset(&transportSettings, 0, sizeof(UA_ExtensionObject));
    transportSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    transportSettings.content.decoded.type = &UA_TYPES[UA_TYPES_BROKERDATASETWRITERTRANSPORTDATATYPE];
    transportSettings.content.decoded.data = &brokerTransportSettings;
    dataSetWriterConfig.transportSettings = transportSettings;

    UA_JsonDataSetWriterMessageDataType jsonDswMd;
    memset(&jsonDswMd, 0, sizeof(UA_JsonDataSetWriterMessageDataType));
    jsonDswMd.dataSetMessageContentMask = (UA_JsonDataSetMessageContentMask) (
        UA_JSONDATASETMESSAGECONTENTMASK_DATASETWRITERID |
        UA_JSONDATASETMESSAGECONTENTMASK_SEQUENCENUMBER |
        UA_JSONDATASETMESSAGECONTENTMASK_STATUS |
        UA_JSONDATASETMESSAGECONTENTMASK_METADATAVERSION |
        UA_JSONDATASETMESSAGECONTENTMASK_TIMESTAMP);

    UA_ExtensionObject messageSettings;
    memset(&messageSettings, 0, sizeof(UA_ExtensionObject));
    messageSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    messageSettings.content.decoded.type = &UA_TYPES[UA_TYPES_JSONDATASETWRITERMESSAGEDATATYPE];
    messageSettings.content.decoded.data = &jsonDswMd;
    dataSetWriterConfig.messageSettings = messageSettings;

    UA_Server_addDataSetWriter(server, writerGroupIdent, publishedDataSetIdent,
                               &dataSetWriterConfig, &dataSetWriterIdent);
}

int main(void) {
    signal(SIGINT, stopHandler);
    signal(SIGTERM, stopHandler);

    /* Initialize random seed */
    srand((unsigned int)time(NULL));

    const char *hub_name = getenv("IOT_HUB_NAME");
    const char *device_id = getenv("IOT_DEVICE_ID");
    if(!hub_name) hub_name = "iot63018734.azure-devices.net";
    if(!device_id) device_id = "device2";

    UA_Server *server = UA_Server_new();

    addPubSubConnection(server, hub_name, device_id);
    addPublishedDataSet(server);
    
    addVariableNodes(server);
    addDataSetFields(server);
    
    addWriterGroup(server, device_id);
    addDataSetWriter(server, device_id);
    
    UA_Server_enableAllPubSubComponents(server);
    
    /* Add repeating callback to update data every 2 seconds */
    UA_Server_addRepeatedCallback(server, updateDataCallback, NULL, 2000, NULL);
    
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Publisher starting...");
    UA_StatusCode retval = UA_Server_runUntilInterrupt(server);

    UA_Server_delete(server);
    return retval == UA_STATUSCODE_GOOD ? EXIT_SUCCESS : EXIT_FAILURE;
}
