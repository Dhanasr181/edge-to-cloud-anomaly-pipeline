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

UA_NodeId connectionIdent, readerGroupIdent, dataSetReaderIdent;
UA_Boolean running = true;

static void stopHandler(int sign) {
    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "received ctrl-c");
    running = false;
}

static void
addPubSubConnection(UA_Server *server, const char *hub_name, const char *device_id) {
    UA_PubSubConnectionConfig connectionConfig;
    memset(&connectionConfig, 0, sizeof(connectionConfig));
    connectionConfig.name = UA_STRING("Azure IoT Hub MQTT Subscriber Connection");
    connectionConfig.transportProfileUri = UA_STRING("http://opcfoundation.org/UA-Profile/Transport/pubsub-mqtt-json");
    connectionConfig.enabled = UA_TRUE;
    
    UA_NetworkAddressUrlDataType networkAddressUrl = {UA_STRING_NULL , UA_STRING_NULL};
    char url[256];
    snprintf(url, sizeof(url), "opc.mqtt://127.0.0.1:1883");
    networkAddressUrl.url = UA_String_fromChars(url);
    
    UA_Variant_setScalar(&connectionConfig.address, &networkAddressUrl,
                         &UA_TYPES[UA_TYPES_NETWORKADDRESSURLDATATYPE]);

    char client_id[128];
    snprintf(client_id, sizeof(client_id), "%s-sub", device_id);
    connectionConfig.publisherId.idType = UA_PUBLISHERIDTYPE_STRING;
    connectionConfig.publisherId.id.string = UA_String_fromChars(client_id);

    /* MQTT TLS specific properties */
    UA_Boolean useTLS = UA_FALSE;
    UA_KeyValueMap_setScalar(&connectionConfig.connectionProperties, UA_QUALIFIEDNAME(0, "mqttUseTLS"), &useTLS, &UA_TYPES[UA_TYPES_BOOLEAN]);

    UA_String caFile = UA_STRING("/certs/azure_root_ca.pem");
    UA_KeyValueMap_setScalar(&connectionConfig.connectionProperties, UA_QUALIFIEDNAME(0, "mqttCaFilePath"), &caFile, &UA_TYPES[UA_TYPES_STRING]);

    UA_String certFile = UA_STRING("/certs/device.crt");
    UA_KeyValueMap_setScalar(&connectionConfig.connectionProperties, UA_QUALIFIEDNAME(0, "mqttClientCertPath"), &certFile, &UA_TYPES[UA_TYPES_STRING]);

    UA_String keyFile = UA_STRING("/certs/device.key");
    UA_KeyValueMap_setScalar(&connectionConfig.connectionProperties, UA_QUALIFIEDNAME(0, "mqttClientKeyPath"), &keyFile, &UA_TYPES[UA_TYPES_STRING]);

    UA_Server_addPubSubConnection(server, &connectionConfig, &connectionIdent);
    UA_KeyValueMap_clear(&connectionConfig.connectionProperties);
    UA_String_clear(&networkAddressUrl.url);
    UA_String_clear(&connectionConfig.publisherId.id.string);
}

static void
addReaderGroup(UA_Server *server, const char *device_id) {
    UA_ReaderGroupConfig readerGroupConfig;
    memset(&readerGroupConfig, 0, sizeof(UA_ReaderGroupConfig));
    readerGroupConfig.name = UA_STRING("Demo ReaderGroup");
    readerGroupConfig.encodingMimeType = UA_PUBSUB_ENCODING_JSON;
    
    UA_BrokerDataSetReaderTransportDataType brokerTransportSettings;
    memset(&brokerTransportSettings, 0, sizeof(UA_BrokerDataSetReaderTransportDataType));
    char topic[256];
    snprintf(topic, sizeof(topic), "devices/%s/messages/devicebound/#", device_id);
    brokerTransportSettings.queueName = UA_String_fromChars(topic);

    UA_ExtensionObject transportSettings;
    memset(&transportSettings, 0, sizeof(UA_ExtensionObject));
    transportSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    transportSettings.content.decoded.type = &UA_TYPES[UA_TYPES_BROKERDATASETREADERTRANSPORTDATATYPE];
    transportSettings.content.decoded.data = &brokerTransportSettings;
    readerGroupConfig.transportSettings = transportSettings;

    UA_Server_addReaderGroup(server, connectionIdent, &readerGroupConfig, &readerGroupIdent);
}

static void
addDataSetReader(UA_Server *server, const char *device_id) {
    UA_DataSetReaderConfig dataSetReaderConfig;
    memset(&dataSetReaderConfig, 0, sizeof(UA_DataSetReaderConfig));
    dataSetReaderConfig.name = UA_STRING("Demo DataSetReader");

    /* Publisher ID is the device_id sending the message (the hub or another device),
       in cloud-to-device it can be the device_id itself or we leave it empty to receive all */
    UA_Variant_setScalar(&dataSetReaderConfig.publisherId, &device_id, &UA_TYPES[UA_TYPES_STRING]);
    dataSetReaderConfig.writerGroupId = 100;
    dataSetReaderConfig.dataSetWriterId = 62541;

    char topic[256];
    snprintf(topic, sizeof(topic), "devices/%s/messages/devicebound/#", device_id);

    UA_BrokerDataSetReaderTransportDataType brokerTransportSettings;
    memset(&brokerTransportSettings, 0, sizeof(UA_BrokerDataSetReaderTransportDataType));
    brokerTransportSettings.queueName = UA_String_fromChars(topic);

    UA_ExtensionObject transportSettings;
    memset(&transportSettings, 0, sizeof(UA_ExtensionObject));
    transportSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    transportSettings.content.decoded.type = &UA_TYPES[UA_TYPES_BROKERDATASETREADERTRANSPORTDATATYPE];
    transportSettings.content.decoded.data = &brokerTransportSettings;
    dataSetReaderConfig.transportSettings = transportSettings;
    
    UA_JsonDataSetReaderMessageDataType jsonDsrMd;
    memset(&jsonDsrMd, 0, sizeof(UA_JsonDataSetReaderMessageDataType));
    jsonDsrMd.networkMessageContentMask = (UA_JsonNetworkMessageContentMask) (
        UA_JSONNETWORKMESSAGECONTENTMASK_NETWORKMESSAGEHEADER |
        UA_JSONNETWORKMESSAGECONTENTMASK_DATASETMESSAGEHEADER |
        UA_JSONNETWORKMESSAGECONTENTMASK_SINGLEDATASETMESSAGE |
        UA_JSONNETWORKMESSAGECONTENTMASK_PUBLISHERID |
        UA_JSONNETWORKMESSAGECONTENTMASK_DATASETCLASSID);
    jsonDsrMd.dataSetMessageContentMask = (UA_JsonDataSetMessageContentMask) (
        UA_JSONDATASETMESSAGECONTENTMASK_DATASETWRITERID |
        UA_JSONDATASETMESSAGECONTENTMASK_SEQUENCENUMBER |
        UA_JSONDATASETMESSAGECONTENTMASK_STATUS |
        UA_JSONDATASETMESSAGECONTENTMASK_METADATAVERSION |
        UA_JSONDATASETMESSAGECONTENTMASK_TIMESTAMP);

    UA_ExtensionObject messageSettings;
    memset(&messageSettings, 0, sizeof(UA_ExtensionObject));
    messageSettings.encoding = UA_EXTENSIONOBJECT_DECODED;
    messageSettings.content.decoded.type = &UA_TYPES[UA_TYPES_JSONDATASETREADERMESSAGEDATATYPE];
    messageSettings.content.decoded.data = &jsonDsrMd;
    dataSetReaderConfig.messageSettings = messageSettings;

    /* To read JSON properly, we need a target DataSetMetaData (PDS). We omit it for simple generic receive */
    
    UA_Server_addDataSetReader(server, readerGroupIdent, &dataSetReaderConfig, &dataSetReaderIdent);
    UA_String_clear(&brokerTransportSettings.queueName);
}

int main(void) {
    signal(SIGINT, stopHandler);
    signal(SIGTERM, stopHandler);

    const char *hub_name = getenv("AZURE_IOT_HUB_NAME");
    const char *device_id = getenv("AZURE_DEVICE_ID");
    if(!hub_name) hub_name = "iot63018734.azure-devices.net";
    if(!device_id) device_id = "device2";

    UA_Server *server = UA_Server_new();

    addPubSubConnection(server, hub_name, device_id);
    addReaderGroup(server, device_id);
    addDataSetReader(server, device_id);

    UA_Server_enableAllPubSubComponents(server);

    UA_LOG_INFO(UA_Log_Stdout, UA_LOGCATEGORY_SERVER, "Subscriber starting...");
    UA_StatusCode retval = UA_Server_runUntilInterrupt(server);

    UA_Server_delete(server);
    return retval == UA_STATUSCODE_GOOD ? EXIT_SUCCESS : EXIT_FAILURE;
}
