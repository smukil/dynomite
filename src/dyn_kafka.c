/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

#include "dyn_kafka.h"

static struct kafka_producer kwriter;

/**
 * @brief Message delivery report callback.
 *
 * This callback is called exactly once per message, indicating if
 * the message was succesfully delivered
 * (rkmessage->err == RD_KAFKA_RESP_ERR_NO_ERROR) or permanently
 * failed delivery (rkmessage->err != RD_KAFKA_RESP_ERR_NO_ERROR).
 *
 * The callback is triggered from rd_kafka_poll() and executes on
 * the application's thread.
 */
static void dr_msg_cb (rd_kafka_t *rk,
                       const rd_kafka_message_t *rkmessage, void *opaque) {
  if (rkmessage->err)
    fprintf(stderr, "%% Message delivery failed: %s\n",
            rd_kafka_err2str(rkmessage->err));
  else
    fprintf(stderr,
            "%% Message delivered (%zd bytes, "
            "partition %"PRId32")\n",
            rkmessage->len, rkmessage->partition);

  /* The rkmessage is destroyed automatically by librdkafka */
}

rstatus_t kafka_init(char* bootstrap_broker) {
  char errstr[512];       /* librdkafka API error reporting buffer */
  char buf[512];          /* Message value temporary buffer */

  kwriter.conf = rd_kafka_conf_new();
  kwriter.broker = bootstrap_broker;
  /* Set bootstrap broker(s) as a comma-separated list of
   * host or host:port (default port 9092).
   * librdkafka will use the bootstrap brokers to acquire the full
   * set of brokers from the cluster. */
  if (rd_kafka_conf_set(kwriter.conf, "bootstrap.servers", kwriter.broker,
                        errstr, sizeof(errstr)) != RD_KAFKA_CONF_OK) {
    fprintf(stderr, "%s\n", errstr);
    return DN_ERROR;
  }

  rd_kafka_conf_set_dr_msg_cb(kwriter.conf, dr_msg_cb);

  /*
   * Create producer instance.
   *
   * NOTE: rd_kafka_new() takes ownership of the conf object
   *       and the application must not reference it again after
   *       this call.
   */
  kwriter.rk = rd_kafka_new(RD_KAFKA_PRODUCER, kwriter.conf, errstr, sizeof(errstr));
  if (!kwriter.rk) {
    fprintf(stderr,
            "%% Failed to create new producer: %s\n", errstr);
    return DN_ERROR;
  }
  return DN_OK;
}

rstatus_t produce_crr_msg(const char* topic, char *payload, size_t len) {
  rd_kafka_resp_err_t err;
  int num_retries = 3;
  while (num_retries-- > 0) {
    err = rd_kafka_producev(
            kwriter.rk,
            RD_KAFKA_V_TOPIC(topic),
            // Make a copy of the payload
            RD_KAFKA_V_MSGFLAGS(RD_KAFKA_MSG_F_COPY),
            RD_KAFKA_V_VALUE(payload, len),
            /* Per-Message opaque, provided in
             * delivery report callback as
             * msg_opaque. */
            RD_KAFKA_V_OPAQUE(NULL),
            RD_KAFKA_V_END);

    if (err) {
      fprintf(stderr,
              "%% Failed to produce to topic %s: %s\n",
              topic, rd_kafka_err2str(err));

      if (err == RD_KAFKA_RESP_ERR__QUEUE_FULL) {
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
              rd_kafka_poll(kwriter.rk, 1000/*block for max 1000ms*/);
      }
    } else {

      // We've enqueued the message successfully. Break out of our
      // retry loop.
      fprintf(stderr, "%% Enqueued message (%zd bytes) "
              "for topic %s\n",
              len, topic);
      break;
    }
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
  rd_kafka_poll(kwriter.rk, 0/*non-blocking*/);

  return DN_OK;
}

