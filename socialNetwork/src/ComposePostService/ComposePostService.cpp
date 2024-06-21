#include <signal.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/processor/TMultiplexedProcessor.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include <boost/program_options.hpp>

#include "../ClientPool.h"                      // ???
#include "../logger.h"                          // ???
#include "../tracing.h"                         // ???
#include "../../gen-cpp/social_network_types.h" // ???

#include "../utils.h"
#include "../utils_memcached.h"
#include "../utils_mongodb.h"
#include "../utils_redis.h"
#include "../utils_thrift.h"
#include "nlohmann/json.hpp"      // Used by TextService.cpp
#include "ComposePostHandler.h"

#include "../UserService/UserHandler.h"
#include "../SocialGraphService/SocialGraphHandler.h"
#include "../PostStorageService/PostStorageHandler.h"
#include "../HomeTimelineService/HomeTimelineHandler.h"
#include "../UserTimelineService/UserTimelineHandler.h"
#include "../UniqueIdService/UniqueIdHandler.h"
#include "../MediaService/MediaHandler.h"
#include "../UrlShortenService/UrlShortenHandler.h"
#include "../UserMentionService/UserMentionHandler.h"
#include "../TextService/TextHandler.h"


using apache::thrift::protocol::TBinaryProtocolFactory;
using apache::thrift::TMultiplexedProcessor;
using apache::thrift::server::TThreadedServer;
using apache::thrift::transport::TFramedTransportFactory;
using apache::thrift::transport::TServerSocket;
using namespace social_network;

UserHandler* main_user_service(const json &config_json);
SocialGraphHandler* main_social_graph_service(const json &config_json, bool redis_cluster_flag);
PostStorageHandler* main_post_storage_service(const json &config_json);
HomeTimelineHandler* main_home_timeline_service(const json &config_json, bool redis_cluster_flag, PostStorageHandler *post_storage_handler, SocialGraphHandler *social_graph_handler);
UserTimelineHandler* main_user_timeline_service(const json &config_json, bool redis_cluster_flag, PostStorageHandler *post_storage_handler);
UniqueIdHandler* main_unique_id_service(const json &config_json);
MediaHandler* main_media_service(const json &config_json);
UrlShortenHandler* main_url_shorten_service(const json &config_json);
UserMentionHandler* main_user_mention_service(const json &config_json);
TextHandler* main_text_service(const json &config_json, UrlShortenHandler *url_shorten_handler, UserMentionHandler *user_mention_handler);

void sigintHandler(int sig) { exit(EXIT_SUCCESS); }

int main(int argc, char *argv[]) {
  signal(SIGINT, sigintHandler);
  init_logger();

  // Command line options - for HomeTimeline, UserTimeline, SocialGraph.
  namespace po = boost::program_options;
  po::options_description desc("Options");
  desc.add_options()("help", "produce help message")(
      "redis-cluster",
      po::value<bool>()->default_value(false)->implicit_value(true),
      "Enable redis cluster mode");

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (vm.count("help")) {
    std::cout << desc << "\n";
    return 0;
  }

  bool redis_cluster_flag = false;
  if (vm.count("redis-cluster")) {
    if (vm["redis-cluster"].as<bool>()) {
      redis_cluster_flag = true;
    }
  }
  // End parsing command line options.

  SetUpTracer("config/jaeger-config.yml", "compose-post-service");
  SetUpTracer("config/jaeger-config.yml", "social-graph-service");
  SetUpTracer("config/jaeger-config.yml", "user-service");
  SetUpTracer("config/jaeger-config.yml", "post-storage-service");
  SetUpTracer("config/jaeger-config.yml", "home-timeline-service");
  SetUpTracer("config/jaeger-config.yml", "user-timeline-service");
  SetUpTracer("config/jaeger-config.yml", "unique-id-service");
  SetUpTracer("config/jaeger-config.yml", "media-service");
  SetUpTracer("config/jaeger-config.yml", "text-service");
  SetUpTracer("config/jaeger-config.yml", "url-shorten-service");
  SetUpTracer("config/jaeger-config.yml", "user-mention-service");

  json config_json;
  if (load_config_file("config/service-config.json", &config_json) != 0) {
    exit(EXIT_FAILURE);
  }

  // Calling "main functions" of the services.
  SocialGraphHandler* social_graph_handler = main_social_graph_service(config_json, redis_cluster_flag);
  UserHandler* user_handler = main_user_service(config_json);
  // Deferred initialization of services with cross-dependency (User/SocialGraph).
  social_graph_handler->_SetUserService(user_handler);
  user_handler->_SetSocialGraphService(social_graph_handler);
  // Continue calling "main functions".
  PostStorageHandler* post_storage_handler = main_post_storage_service(config_json);
  HomeTimelineHandler* home_timeline_handler = main_home_timeline_service(config_json, redis_cluster_flag, post_storage_handler, social_graph_handler);
  UserTimelineHandler* user_timeline_handler = main_user_timeline_service(config_json, redis_cluster_flag, post_storage_handler);
  UniqueIdHandler* unique_id_handler = main_unique_id_service(config_json);
  MediaHandler* media_handler = main_media_service(config_json);
  UrlShortenHandler* url_shorten_handler = main_url_shorten_service(config_json);
  UserMentionHandler* user_mention_handler = main_user_mention_service(config_json);
  TextHandler* text_handler = main_text_service(config_json, url_shorten_handler, user_mention_handler);

  // Create ComposePostService itself.
  ComposePostHandler* compose_post_handler = new ComposePostHandler(post_storage_handler, user_timeline_handler, user_handler, unique_id_handler, media_handler, text_handler, home_timeline_handler);

  int port = config_json["compose-post-service"]["port"];

  // Register processors for a multiplexed server.
  std::shared_ptr<TMultiplexedProcessor> processor(new TMultiplexedProcessor());
  processor->registerProcessor("ComposePostService", std::make_shared<ComposePostServiceProcessor>(std::shared_ptr<ComposePostHandler>(compose_post_handler)));
  // processor->registerProcessor("ComposePostService", std::make_shared<ComposePostServiceProcessor>(std::make_shared<ComposePostHandler>(post_storage_handler, user_timeline_handler, user_handler, unique_id_handler, media_handler, text_handler, home_timeline_handler)));
  processor->registerProcessor("HomeTimelineService", std::make_shared<HomeTimelineServiceProcessor>(std::shared_ptr<HomeTimelineHandler>(home_timeline_handler)));
  processor->registerProcessor("UserTimelineService", std::make_shared<UserTimelineServiceProcessor>(std::shared_ptr<UserTimelineHandler>(user_timeline_handler)));
  processor->registerProcessor("SocialGraphService", std::make_shared<SocialGraphServiceProcessor>(std::shared_ptr<SocialGraphHandler>(social_graph_handler)));
  processor->registerProcessor("UserService", std::make_shared<UserServiceProcessor>(std::shared_ptr<UserHandler>(user_handler)));

  std::shared_ptr<TServerSocket> server_socket = get_server_socket(config_json, "0.0.0.0", port);
  TThreadedServer server(processor,//std::make_shared<ComposePostServiceProcessor>(std::make_shared<ComposePostHandler>(post_storage_handler, user_timeline_handler, user_handler, unique_id_handler, media_handler, text_handler, home_timeline_handler)),
    server_socket,
    std::make_shared<TFramedTransportFactory>(),
    std::make_shared<TBinaryProtocolFactory>());
  LOG(info) << "Starting the compose-post-service ***multiplexed*** server...";
  server.serve();

  // TODO: call delete on the initialized services.
}

u_int16_t HashMacAddressPid(const std::string &mac) {
  u_int16_t hash = 0;
  std::string mac_pid = mac + std::to_string(getpid());
  for (unsigned int i = 0; i < mac_pid.size(); i++) {
    hash += (mac[i] << ((i & 1) * 8));
  }
  return hash;
}

std::string GetMachineId(std::string &netif) {
  std::string mac_hash;

  std::string mac_addr_filename = "/sys/class/net/" + netif + "/address";
  std::ifstream mac_addr_file;
  mac_addr_file.open(mac_addr_filename);
  if (!mac_addr_file) {
    LOG(fatal) << "Cannot read MAC address from net interface " << netif;
    return "";
  }
  std::string mac;
  mac_addr_file >> mac;
  if (mac == "") {
    LOG(fatal) << "Cannot read MAC address from net interface " << netif;
    return "";
  }
  mac_addr_file.close();

  LOG(info) << "MAC address = " << mac;

  std::stringstream stream;
  stream << std::hex << HashMacAddressPid(mac);
  mac_hash = stream.str();

  if (mac_hash.size() > 3) {
    mac_hash.erase(0, mac_hash.size() - 3);
  } else if (mac_hash.size() < 3) {
    mac_hash = std::string(3 - mac_hash.size(), '0') + mac_hash;
  }
  return mac_hash;
}


UserHandler* main_user_service(const json &config_json) {
  std::string secret = config_json["secret"];
  int mongodb_conns = config_json["user-mongodb"]["connections"];
  int mongodb_timeout = config_json["user-mongodb"]["timeout_ms"];
  int memcached_conns = config_json["user-memcached"]["connections"];
  int memcached_timeout = config_json["user-memcached"]["timeout_ms"];

  memcached_pool_st *memcached_client_pool = init_memcached_client_pool(config_json, "user", 32, memcached_conns);
  mongoc_client_pool_t *mongodb_client_pool = init_mongodb_client_pool(config_json, "user", mongodb_conns);

  if (memcached_client_pool == nullptr || mongodb_client_pool == nullptr) {
    exit(EXIT_FAILURE);
  }
  std::string netif = config_json["user-service"]["netif"];
  std::string machine_id = GetMachineId(netif);
  if (machine_id == "") {
    exit(EXIT_FAILURE);
  }
  LOG(info) << "machine_id = " << machine_id;

  std::mutex* thread_lock = new std::mutex();

  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    exit(EXIT_FAILURE);
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "user", "user_id", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  LOG(info) << "Using user-service...";
  return new UserHandler(thread_lock, machine_id, secret, memcached_client_pool, mongodb_client_pool, nullptr);
}


SocialGraphHandler* main_social_graph_service(const json &config_json, bool redis_cluster_flag) {
  int mongodb_conns = config_json["social-graph-mongodb"]["connections"];
  int mongodb_timeout = config_json["social-graph-mongodb"]["timeout_ms"];
  int redis_cluster_config_flag = config_json["social-graph-redis"]["use_cluster"];
  int redis_replica_config_flag = config_json["social-graph-redis"]["use_replica"];

  mongoc_client_pool_t *mongodb_client_pool = init_mongodb_client_pool(config_json, "social-graph", mongodb_conns);
  if (mongodb_client_pool == nullptr) {
    exit(EXIT_FAILURE);
  }
  if (redis_replica_config_flag && (redis_cluster_config_flag || redis_cluster_flag)) {
      LOG(error) << "Can't start service when Redis Cluster and Redis Replica are enabled at the same time";
      exit(EXIT_FAILURE);
  }
  mongoc_client_t *mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    exit(EXIT_FAILURE);
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "social-graph", "user_id", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  if (redis_cluster_flag || redis_cluster_config_flag) {
    RedisCluster redis_cluster_client_pool = init_redis_cluster_client_pool(config_json, "social-graph");
    LOG(info) << "Using social-graph-service with Redis Cluster support...";
    return new SocialGraphHandler(mongodb_client_pool, &redis_cluster_client_pool, nullptr);
  } else if (redis_replica_config_flag) {
    Redis redis_replica_client_pool = init_redis_replica_client_pool(config_json, "redis-replica");
    Redis redis_primary_client_pool = init_redis_replica_client_pool(config_json, "redis-primary");
    LOG(info) << "Using social-graph-service with Redis replica support...";
    return new SocialGraphHandler(mongodb_client_pool, &redis_replica_client_pool, &redis_primary_client_pool, nullptr);
  } else {
    Redis* redis_client_pool = init_redis_client_pool(config_json, "social-graph");
    LOG(info) << "Using social-graph-service...";
    return new SocialGraphHandler(mongodb_client_pool, redis_client_pool, nullptr);
  }
}

PostStorageHandler* main_post_storage_service(const json &config_json) {
  int mongodb_conns = config_json["post-storage-mongodb"]["connections"];
  int mongodb_timeout = config_json["post-storage-mongodb"]["timeout_ms"];
  int memcached_conns = config_json["post-storage-memcached"]["connections"];
  int memcached_timeout = config_json["post-storage-memcached"]["timeout_ms"];
  memcached_pool_st *memcached_client_pool = init_memcached_client_pool(config_json, "post-storage", 32, memcached_conns);
  mongoc_client_pool_t *mongodb_client_pool = init_mongodb_client_pool(config_json, "post-storage", mongodb_conns);
  if (memcached_client_pool == nullptr || mongodb_client_pool == nullptr) {
    exit(EXIT_FAILURE);
  }
  mongoc_client_t* mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    exit(EXIT_FAILURE);
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "post", "post_id", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  LOG(info) << "Using post-storage-service...";
  return new PostStorageHandler(memcached_client_pool, mongodb_client_pool);
}

HomeTimelineHandler* main_home_timeline_service(const json &config_json, bool redis_cluster_flag, PostStorageHandler *post_storage_handler, SocialGraphHandler *social_graph_handler) {
  int redis_cluster_config_flag = config_json["home-timeline-redis"]["use_cluster"];
  int redis_replica_config_flag = config_json["home-timeline-redis"]["use_replica"];
  if (redis_replica_config_flag && (redis_cluster_config_flag || redis_cluster_flag)) {
      LOG(error) << "Can't start service when Redis Cluster and Redis Replica are enabled at the same time";
      exit(EXIT_FAILURE);
  }

  if (redis_replica_config_flag) {
    Redis redis_replica_client_pool = init_redis_replica_client_pool(config_json, "redis-replica");
    Redis redis_primary_client_pool = init_redis_replica_client_pool(config_json, "redis-primary");
    LOG(info) << "Using home-timeline-service with replicated Redis support...";
    return new HomeTimelineHandler(&redis_replica_client_pool, &redis_primary_client_pool, post_storage_handler, social_graph_handler);
  } else if (redis_cluster_flag || redis_cluster_config_flag) {
    RedisCluster redis_cluster_client_pool = init_redis_cluster_client_pool(config_json, "home-timeline");
    LOG(info) << "Using home-timeline-service with Redis Cluster support...";
    return new HomeTimelineHandler(&redis_cluster_client_pool, post_storage_handler, social_graph_handler);
  } else {
    Redis* redis_client_pool = init_redis_client_pool(config_json, "home-timeline");
    LOG(info) << "Using home-timeline-service...";
    return new HomeTimelineHandler(redis_client_pool, post_storage_handler, social_graph_handler);
  }
}

UserTimelineHandler* main_user_timeline_service(const json &config_json, bool redis_cluster_flag, PostStorageHandler *post_storage_handler) {
  int mongodb_conns = config_json["user-timeline-mongodb"]["connections"];
  int mongodb_timeout = config_json["user-timeline-mongodb"]["timeout_ms"];
  int redis_cluster_config_flag = config_json["user-timeline-redis"]["use_cluster"];
  int redis_replica_config_flag = config_json["user-timeline-redis"]["use_replica"];

  mongoc_client_pool_t* mongodb_client_pool = init_mongodb_client_pool(config_json, "user-timeline", mongodb_conns);
  if (mongodb_client_pool == nullptr) {
    exit(EXIT_FAILURE);
  }
  if (redis_replica_config_flag && (redis_cluster_config_flag || redis_cluster_flag)) {
    LOG(error) << "Can't start service when Redis Cluster and Redis Replica are enabled at the same time";
    exit(EXIT_FAILURE);
  }

  mongoc_client_t* mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    exit(EXIT_FAILURE);
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "user-timeline", "user_id", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);

  if (redis_cluster_flag || redis_cluster_config_flag) {
    RedisCluster redis_client_pool = init_redis_cluster_client_pool(config_json, "user-timeline");
    LOG(info) << "Using user-timeline-service with Redis Cluster support...";
    return new UserTimelineHandler(&redis_client_pool, mongodb_client_pool, post_storage_handler);
  } else if (redis_replica_config_flag) {
    Redis redis_replica_client_pool = init_redis_replica_client_pool(config_json, "redis-replica");
    Redis redis_primary_client_pool = init_redis_replica_client_pool(config_json, "redis-primary");
    LOG(info) << "Using user-timeline-service with replicated Redis support...";
    return new UserTimelineHandler(&redis_replica_client_pool, &redis_primary_client_pool, mongodb_client_pool, post_storage_handler);
  } else {
    Redis* redis_client_pool = init_redis_client_pool(config_json, "user-timeline");
    LOG(info) << "Using user-timeline-service...";
    return new UserTimelineHandler(redis_client_pool, mongodb_client_pool, post_storage_handler);
  }
}

UniqueIdHandler* main_unique_id_service(const json &config_json) {
  std::string netif = config_json["unique-id-service"]["netif"];
  std::string machine_id = GetMachineId(netif);
  if (machine_id == "") {
    exit(EXIT_FAILURE);
  }
  LOG(info) << "machine_id = " << machine_id;

  std::mutex* thread_lock = new std::mutex();
  LOG(info) << "Using unique-id-service...";
  return new UniqueIdHandler(thread_lock, machine_id);
}

MediaHandler* main_media_service(const json &config_json) {
  LOG(info) << "Using media-service...";
  return new MediaHandler();
}

UrlShortenHandler* main_url_shorten_service(const json &config_json) {
  int mongodb_conns = config_json["url-shorten-mongodb"]["connections"];
  int mongodb_timeout = config_json["url-shorten-mongodb"]["timeout_ms"];
  int memcached_conns = config_json["url-shorten-memcached"]["connections"];
  int memcached_timeout = config_json["url-shorten-memcached"]["timeout_ms"];

  memcached_pool_st* memcached_client_pool = init_memcached_client_pool(config_json, "url-shorten", 32, memcached_conns);
  mongoc_client_pool_t* mongodb_client_pool = init_mongodb_client_pool(config_json, "url-shorten", mongodb_conns);
  if (memcached_client_pool == nullptr || mongodb_client_pool == nullptr) {
    exit(EXIT_FAILURE);
  }
  mongoc_client_t* mongodb_client = mongoc_client_pool_pop(mongodb_client_pool);
  if (!mongodb_client) {
    LOG(fatal) << "Failed to pop mongoc client";
    exit(EXIT_FAILURE);
  }
  bool r = false;
  while (!r) {
    r = CreateIndex(mongodb_client, "url-shorten", "shortened_url", true);
    if (!r) {
      LOG(error) << "Failed to create mongodb index, try again";
      sleep(1);
    }
  }
  mongoc_client_pool_push(mongodb_client_pool, mongodb_client);
  std::mutex* thread_lock = new std::mutex();
  LOG(info) << "Using url-shorten-service...";
  return new UrlShortenHandler(memcached_client_pool, mongodb_client_pool, thread_lock);
}

UserMentionHandler* main_user_mention_service(const json &config_json) {
  int mongodb_conns = config_json["user-mongodb"]["connections"];
  int mongodb_timeout = config_json["user-mongodb"]["timeout_ms"];
  int memcached_conns = config_json["user-memcached"]["connections"];
  int memcached_timeout = config_json["user-memcached"]["timeout_ms"];

  memcached_pool_st* memcached_client_pool = init_memcached_client_pool(config_json, "user", 32, memcached_conns);
  mongoc_client_pool_t* mongodb_client_pool = init_mongodb_client_pool(config_json, "user", mongodb_conns);
  if (memcached_client_pool == nullptr || mongodb_client_pool == nullptr) {
    exit(EXIT_FAILURE);
  }
  LOG(info) << "Using user-mention-service...";
  return new UserMentionHandler(memcached_client_pool, mongodb_client_pool);
}

TextHandler* main_text_service(const json &config_json, UrlShortenHandler *url_shorten_handler, UserMentionHandler *user_mention_handler) {
  LOG(info) << "Using text-service...";
  return new TextHandler(url_shorten_handler, user_mention_handler);
}
