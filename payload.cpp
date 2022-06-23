#include <stdio.h>
#include <string.h>
#include <iostream>

#include "mosquitto_broker.h"
#include "mosquitto_plugin.h"
#include "mosquitto.h"
#include "mqtt_protocol.h"
#include "mosquitto_internal.h"
#include "ParamReader.h"

using namespace std;

#define UNUSED(A) (void)(A)

static mosquitto_plugin_id_t *mosq_pid = NULL;

static int callback_message(int event, void *event_data, void *userdata)
{
	struct mosquitto_evt_message *ed = (mosquitto_evt_message*)event_data;

	UNUSED(event);
	UNUSED(userdata);

	printf("address: %s\n", mosquitto_client_address(ed->client));
	printf("id: %s\n", mosquitto_client_id(ed->client));
	printf("username: %s\n", mosquitto_client_username(ed->client));
	printf("payload: '%.*s'\n", ed->payloadlen, (char *)ed->payload);

	return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_version(int supported_version_count, const int *supported_versions)
{
	int i;

	for(i=0; i<supported_version_count; i++){
		if(supported_versions[i] == 5){
			return 5;
		}
	}
	return -1;
}

int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **user_data, struct mosquitto_opt *opts, int opt_count)
{
	UNUSED(user_data);
	UNUSED(opts);
	UNUSED(opt_count);

	printf("+++++Initialize Plugin ++++++++ \n");
	mosq_pid = identifier;

	ParamReader DetectParams;


        Params<string> endPoint(DetectParams.open("test.txt").strings());
        Params<string> service(DetectParams.open("test.txt").strings());
        Params<string> msg(DetectParams.open("test.txt").strings());
        Params<string> streamId(DetectParams.open("test.txt").strings());
        Params<string> privateKey(DetectParams.open("test.txt").strings());
        Params<string> TenancyID(DetectParams.open("test.txt").strings());
        Params<string> UserID(DetectParams.open("test.txt").strings());
        Params<string> FingerPrint(DetectParams.open("test.txt").strings());


        printf("endPoint is : %s \n", endPoint.get("endPoint", "Not Found").c_str() );
        printf("service is : %s \n", service.get("service", "Not Found").c_str());
        printf("msg is : %s \n", 	msg.get("msg", "Not Found").c_str() );
        printf("streamId is : %s \n", streamId.get("streamId", "Not Found").c_str());
        printf("privateKey is : %s \n", privateKey.get("privateKey", "Not Found").c_str());
        printf("TenancyID is : %s \n", TenancyID.get("TenancyID", "Not Found").c_str() );
        printf("UserID is : %s \n", UserID.get("UserID", "Not Found").c_str());
        printf("FingerPrint is : %s \n",FingerPrint.get("FingerPrint", "Not Found").c_str() );


	return mosquitto_callback_register(mosq_pid, MOSQ_EVT_MESSAGE, callback_message, NULL, NULL);
}

int mosquitto_plugin_cleanup(void *user_data, struct mosquitto_opt *opts, int opt_count)
{
	UNUSED(user_data);
	UNUSED(opts);
	UNUSED(opt_count);

	return mosquitto_callback_unregister(mosq_pid, MOSQ_EVT_MESSAGE, callback_message, NULL);
}
