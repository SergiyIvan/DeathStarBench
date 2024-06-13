#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include "../utils.h"
#include "../utils_memcached.h"
#include "../utils_mongodb.h"
#include "../utils_thrift.h"
#include "../UrlShortenService/UrlShortenHandler.h"
#include "../UserMentionService/UserMentionHandler.h"
#include "nlohmann/json.hpp"
#include "TextHandler.h"

using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TServerSocket;
using namespace social_network;

static memcached_pool_st* url_memcached_client_pool;
static mongoc_client_pool_t* url_mongodb_client_pool;

static memcached_pool_st* user_mention_memcached_client_pool;
static mongoc_client_pool_t* user_mention_mongodb_client_pool;

void sigintHandler(int sig) {
  if (url_memcached_client_pool != nullptr) {
    memcached_pool_destroy(url_memcached_client_pool);
  }
  if (url_mongodb_client_pool != nullptr) {
    mongoc_client_pool_destroy(url_mongodb_client_pool);
  }
  if (user_mention_memcached_client_pool != nullptr) {
    memcached_pool_destroy(user_mention_memcached_client_pool);
  }
  if (user_mention_mongodb_client_pool != nullptr) {
    mongoc_client_pool_destroy(user_mention_mongodb_client_pool);
  }
  exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();
  SetUpTracer("config/jaeger-config.yml", "text-service");
  SetUpTracer("config/jaeger-config.yml", "url-shorten-service");
  SetUpTracer("config/jaeger-config.yml", "user-mention-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) == 0) {
    int port = config_json["text-service"]["port"];

    // -------------------------------- BEGIN URL --------------------------------
    int url_mongodb_conns = config_json["url-shorten-mongodb"]["connections"];
    int url_mongodb_timeout = config_json["url-shorten-mongodb"]["timeout_ms"];
    int url_memcached_conns = config_json["url-shorten-memcached"]["connections"];
    int url_memcached_timeout = config_json["url-shorten-memcached"]["timeout_ms"];

    url_memcached_client_pool = init_memcached_client_pool(config_json, "url-shorten", 32, url_memcached_conns);
    url_mongodb_client_pool = init_mongodb_client_pool(config_json, "url-shorten", url_mongodb_conns);
    if (url_memcached_client_pool == nullptr || url_mongodb_client_pool == nullptr) {
        return EXIT_FAILURE;
    }

    mongoc_client_t* mongodb_client = mongoc_client_pool_pop(url_mongodb_client_pool);
    if (!mongodb_client) {
        LOG(fatal) << "Failed to pop mongoc client";
        return EXIT_FAILURE;
    }
    bool r = false;
    while (!r) {
        r = CreateIndex(mongodb_client, "url-shorten", "shortened_url", true);
        if (!r) {
        LOG(error) << "Failed to create mongodb index, try again";
        sleep(1);
        }
    }
    mongoc_client_pool_push(url_mongodb_client_pool, mongodb_client);

    std::mutex thread_lock;
    UrlShortenHandler url_handler(url_memcached_client_pool, url_mongodb_client_pool, &thread_lock);
    // -------------------------------- END URL --------------------------------
    // -------------------------------- BEGIN USERMENTION --------------------------------
    int user_mention_mongodb_conns = config_json["user-mongodb"]["connections"];
    int user_mention_mongodb_timeout = config_json["user-mongodb"]["timeout_ms"];
    int user_mention_memcached_conns = config_json["user-memcached"]["connections"];
    int user_mention_memcached_timeout = config_json["user-memcached"]["timeout_ms"];

    user_mention_memcached_client_pool = init_memcached_client_pool(config_json, "user", 32, user_mention_memcached_conns);
    user_mention_mongodb_client_pool = init_mongodb_client_pool(config_json, "user", user_mention_mongodb_conns);
    if (user_mention_memcached_client_pool == nullptr || user_mention_mongodb_client_pool == nullptr) {
        return EXIT_FAILURE;
    }

    UserMentionHandler user_mention_handler(user_mention_memcached_client_pool, user_mention_mongodb_client_pool);
    // -------------------------------- END USERMENTION --------------------------------

    std::shared_ptr<TServerSocket> server_socket = get_server_socket(config_json, "0.0.0.0", port);
    TThreadedServer server(
        std::make_shared<TextServiceProcessor>(std::make_shared<TextHandler>(
            &url_handler, &user_mention_handler)),
        server_socket,
        std::make_shared<TFramedTransportFactory>(),
        std::make_shared<TBinaryProtocolFactory>());

    LOG(info) << "Starting the text-service server...";
    server.serve();
  } else
    exit(EXIT_FAILURE);
}
