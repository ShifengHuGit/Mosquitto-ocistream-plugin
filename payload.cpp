#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <iostream>
#include <sys/stat.h>

#include "mosquitto_broker.h"
#include "mosquitto_plugin.h"
#include "mosquitto.h"
#include "mqtt_protocol.h"
#include "mosquitto_internal.h"
#include "ParamReader.h"
#include "librdkafka/rdkafka.h"

using namespace std;

#define UNUSED(A) (void)(A)

// Kafka
static volatile sig_atomic_t run = 1;
rd_kafka_t *rk;                                                               /* Producer instance handle */
rd_kafka_conf_t *conf;                                                        /* Temporary configuration object */
char errstr[512];                                                             /* librdkafka API error reporting buffer */
char buf[512];                                                                /* Message value temporary buffer */
char K_topic[128];

static mosquitto_plugin_id_t *mosq_pid = NULL;

typedef struct AuthConfigSet
{
    string TenancyName;
    string StreamPoolID;
    string bootstrap_servers;
    string security_protocol;
    string sasl_mechanisms;
    string sasl_username;
    string sasl_password;
    string KafkaTopic;
} AuthConfig;

bool isFileExists_stat(string &name)
{
    struct stat buffer;
    return (stat(name.c_str(), &buffer) == 0);
}

AuthConfig loadOCIconfig(string filepath)
{
    if (isFileExists_stat(filepath) != true)
    {
        printf("---Plugin--->> Config file:[%s] not found, please check the plugin file\n", filepath.c_str());
        exit(0);
    }
    const char *file = filepath.c_str();
    ParamReader DetectParams;
    // AuthConfig auth;

    Params<string> Tenancy(DetectParams.open(file).strings());
    Params<string> PoolID(DetectParams.open(file).strings());
    Params<string> Server(DetectParams.open(file).strings());
    Params<string> Protocol(DetectParams.open(file).strings());
    Params<string> mechanisms(DetectParams.open(file).strings());
    Params<string> username(DetectParams.open(file).strings());
    Params<string> Token(DetectParams.open(file).strings());
    Params<string> KafkaTopic(DetectParams.open(file).strings());

    AuthConfig auth = {
        .TenancyName = Tenancy.get("Tenancy", "Not Found"),
        .StreamPoolID = PoolID.get("StreamingPoolID", "Not Found"),
        .bootstrap_servers = Server.get("bootstrap_servers", "Not Found"),
        .security_protocol = Protocol.get("security_protocol", "SASL_SSL"),
        .sasl_mechanisms = mechanisms.get("sasl_mechanisms", "PLAIN"),
        .sasl_username = username.get("sasl_username", "Not Found"),
        .sasl_password = Token.get("sasl_password", "Not Found"),
        .KafkaTopic = KafkaTopic.get("KafkaTopic", "Not Found")};

    /*
     printf("---Plugin--ldconfig--> Tenancy is : %s \n", auth.TenancyName);
     printf("---Plugin--ldconfig--> StreamingPool is : %s \n", auth.StreamPoolID);
     printf("---Plugin--ldconfig--> bootstrap_servers is : %s \n", auth.bootstrap_servers);
     printf("---Plugin--ldconfig--> security_protocol is : %s \n", auth.security_protocol);
     printf("---Plugin--ldconfig--> sasl_mechanisms is : %s \n", auth.sasl_mechanisms);
     printf("---Plugin--ldconfig--> sasl_username is : %s \n", auth.sasl_username);
     printf("---Plugin--ldconfig--> sasl_password is : %s \n", auth.sasl_password);
     printf("---Plugin--ldconfig--> Topic is : %s \n", auth.topic);
 */
    /*
            printf("Tenancy is : %s \n", Tenancy.get("Tenancy", "Not Found").c_str());
            printf("StreamingPool is : %s \n", PoolID.get("StreamingPoolID", "Not Found").c_str());
            printf("bootstrap_servers is : %s \n", Server.get("bootstrap_servers", "Not Found").c_str());
            printf("security_protocol is : %s \n", Protocol.get("security_protocol", "SASL_SSL").c_str());
            printf("sasl_mechanisms is : %s \n", mechanisms.get("sasl_mechanisms", "PLAIN").c_str());
            printf("sasl_username is : %s \n", username.get("sasl_username", "Not Found").c_str());
            printf("sasl_password is : %s \n", Token.get("sasl_password", "Not Found").c_str());
       */

    return auth;
}

static void
dr_msg_cb(rd_kafka_t *rk, const rd_kafka_message_t *rkmessage, void *opaque)
{
    if (rkmessage->err)
        fprintf(stderr, "%% Message delivery failed: %s\n",
                rd_kafka_err2str(rkmessage->err));
    else
        fprintf(stderr,
                "%% Message delivered (%zd bytes, "
                "partition %" PRId32 ")\n",
                rkmessage->len, rkmessage->partition);

    /* The rkmessage is destroyed automatically by librdkafka */
}

//--------------

int initKafka(rd_kafka_conf_t *conf, AuthConfig *authInfo)
{
    AuthConfig auth = *authInfo;

 // printf(">>>>>> Kafka Conf - Topic  : %s \n", topic);
    if (rd_kafka_conf_set(conf, "bootstrap.servers", auth.bootstrap_servers.c_str(), errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }
    /* if (rd_kafka_conf_set(conf, "metadata.broker.list", "cell-1.streaming.ap-tokyo-1.oci.oraclecloud.com:9092", errstr,
                                 sizeof(errstr)) != RD_KAFKA_CONF_OK) {
                   fprintf(stderr, "%s\n", errstr);
                   return 1;
           }
   */
    if (rd_kafka_conf_set(conf, "security.protocol", auth.security_protocol.c_str(), errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }

    if (rd_kafka_conf_set(conf, "sasl.mechanisms", auth.sasl_mechanisms.c_str(), errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }
    string sasluser= auth.TenancyName+"/"+auth.sasl_username+"/"+auth.StreamPoolID;
    
    if (rd_kafka_conf_set(conf, "sasl.username", sasluser.c_str(), errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }
    if (rd_kafka_conf_set(conf, "sasl.password", auth.sasl_password.c_str(), errstr,
                          sizeof(errstr)) != RD_KAFKA_CONF_OK)
    {
        fprintf(stderr, "%s\n", errstr);
        return 1;
    }

    rd_kafka_conf_set_dr_msg_cb(conf, dr_msg_cb);

    rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
    if (!rk)
    {
        fprintf(stderr, "%% Failed to create new producer: %s\n",
                errstr);
        return 1;
    }
    printf("---Plugin--Kafka Initialize successfully!!---\n");
    return 0;

}

static int callback_message(int event, void *event_data, void *userdata)
{
    struct mosquitto_evt_message *ed = (mosquitto_evt_message *)event_data;
    
    char *K_topic = (char *)userdata;

    char *K_Payload;
    UNUSED(event);
    UNUSED(userdata);

   // printf("\n----CallBack-Topic  : %s \n", K_topic);


    printf("address: %s\n", mosquitto_client_address(ed->client));
    printf("id: %s\n", mosquitto_client_id(ed->client));
    printf("username: %s\n", mosquitto_client_username(ed->client));
    printf("M_Topic: %s\n", (char *)(ed->topic));
    printf("payload: '%.*s'\n", ed->payloadlen, (char *)ed->payload);

    size_t len = ed->payloadlen;
    K_Payload = (char *)ed->payload;

    //
    //
    //    Kafka transfer the payload
    //
    //------------------------------
    rd_kafka_resp_err_t err;

retry:
    err = rd_kafka_producev(
        /* Producer handle */
        rk,
        /* Topic name */
        RD_KAFKA_V_TOPIC(K_topic),
        /* Make a copy of the payload. */
        RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
        /* Message value and length */
        RD_KAFKA_V_VALUE(K_Payload, len),
        /* Per-Message opaque, provided in
         * delivery report callback as
         * msg_opaque. */
        RD_KAFKA_V_OPAQUE(NULL),
        /* End sentinel */
        RD_KAFKA_V_END);

    if (err)
    {
        /*
         * Failed to *enqueue* message for producing.
         */
        fprintf(stderr,
                "%% Failed to produce to topic %s: %s\n", K_topic,
                rd_kafka_err2str(err));

        if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL)
        {
            /* If the internal queue is full, wait for
             * messages to be delivered and then retry.
             * The internal queue represents both
             * messages to be sent and messages that have
             * been sent or failed, awaiting their
             * delivery report callback to be called.
             *
             * The internal queue is limited by the
             * configuration property
             * queue.buffering.max.messages */
            rd_kafka_poll(rk,
                          1000 /*block for max 1000ms*/);
            goto retry;
        }
    }
    else
    {
        fprintf(stderr,
                "--Plugin--kafka-sent-> %% Enqueued message (%ld bytes) "
                "for topic %s\n",
                len, K_topic);
    }

    /* A producer application should continually serve
     * the delivery report queue by calling rd_kafka_poll()
     * at frequent intervals.
     * Either put the poll call in your main loop, or in a
     * dedicated thread, or call it after every
     * rd_kafka_produce() call.
     * Just make sure that rd_kafka_poll() is still called
     * during periods where you are not producing any messages
     * to make sure previously produced messages have their
     * delivery report callback served (and any other callbacks
     * you register). */
    rd_kafka_poll(rk, 0 /*non-blocking*/);

    //------------------------------
    //
    //
    //
    //
    return MOSQ_ERR_SUCCESS;
}

int mosquitto_plugin_version(int supported_version_count, const int *supported_versions)
{
    int i;

    for (i = 0; i < supported_version_count; i++)
    {
        if (supported_versions[i] == 5)
        {
            return 5;
        }
    }
    return -1;
}

int mosquitto_plugin_init(mosquitto_plugin_id_t *identifier, void **user_data, struct mosquitto_opt *opts, int opt_count)
{
    AuthConfig auth;
    UNUSED(user_data);
    UNUSED(opts);
    UNUSED(opt_count);


    

    mosq_pid = identifier;

    printf("+++-<>-+++ Initialize OCI Kafka Plugin      +++-<>-+++ \n");
    printf("+++-<>-+++ Loading Configuration from file  +++-<>-+++ \n");
    //
    //   Kafka
    //----------------------------------

    /* Set bootstrap broker(s) as a comma-separated list of
     * host or host:port (default port 9092).
     * librdkafka will use the bootstrap brokers to acquire the full
     * set of brokers from the cluster. */
    auth = loadOCIconfig((string) "ociplugin.cfg");
/*
    printf("---Plugin--ldconfig--> Tenancy is : %s \n", auth.TenancyName.c_str());
    printf("---Plugin--ldconfig--> StreamingPool is : %s \n", auth.StreamPoolID.c_str());
    printf("---Plugin--ldconfig--> bootstrap_servers is : %s \n", auth.bootstrap_servers.c_str());
    printf("---Plugin--ldconfig--> security_protocol is : %s \n", auth.security_protocol.c_str());
    printf("---Plugin--ldconfig--> sasl_mechanisms is : %s \n", auth.sasl_mechanisms.c_str());
    printf("---Plugin--ldconfig--> sasl_username is : %s \n", auth.sasl_username.c_str());
    printf("---Plugin--ldconfig--> sasl_password is : %s \n", auth.sasl_password.c_str());
    printf("---Plugin--ldconfig--> Topic is : %s \n", auth.KafkaTopic.c_str());
    printf(">>>>>> Kafka Conf - Broker : %s \n", auth.bootstrap_servers.c_str());
    printf(">>>>>> Kafka Conf - Topic  : %s \n", auth.KafkaTopic.c_str());
*/
    conf = rd_kafka_conf_new();

    int k_resp = initKafka(conf, &auth);
    if(k_resp != 0)
    {
        printf("---Plugin--Kafka initialize failed ---\n");
        exit(0);
    }
    //K_topic = (char *)auth.KafkaTopic.c_str();
    //printf("\nK_Topci:  %s\n", K_topic);
    strcpy(K_topic, auth.KafkaTopic.c_str());
    printf("\n---Init--K_Topci:  %s\n", K_topic);

    return mosquitto_callback_register(mosq_pid, MOSQ_EVT_MESSAGE, callback_message, NULL, (void *)K_topic);
}

int mosquitto_plugin_cleanup(void *user_data, struct mosquitto_opt *opts, int opt_count)
{
    UNUSED(user_data);
    UNUSED(opts);
    UNUSED(opt_count);

    return mosquitto_callback_unregister(mosq_pid, MOSQ_EVT_MESSAGE, callback_message, NULL);
}
