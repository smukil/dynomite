/*
 * Dynomite - A thin, distributed replication layer for multi non-distributed
 * storages. Copyright (C) 2014 Netflix, Inc.
 */

/*
 * twemproxy - A fast and lightweight proxy for memcached protocol.
 * Copyright (C) 2011 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _DYN_KAFKA_H_
#define _DYN_KAFKA_H_

#include "../contrib/librdkafka-1.6.0/src/rdkafka.h"

#include "dyn_types.h"

struct kafka_producer {
  rd_kafka_t* rk;
  rd_kafka_conf_t* conf;

  char* broker;
  char* topic;
};

rstatus_t kafka_init(char* bootstrap_broker);

rstatus_t produce_crr_msg(const char* topic, char *payload, size_t len);

#endif
