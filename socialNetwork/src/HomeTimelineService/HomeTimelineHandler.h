#ifndef SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_
#define SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_

#include <sw/redis++/redis++.h>

#include <future>
#include <iostream>
#include <string>

#include "../../gen-cpp/HomeTimelineService.h"
#include "../../gen-cpp/PostStorageService.h"
#include "../../gen-cpp/SocialGraphService.h"
#include "../ClientPool.h"
#include "../ThriftClient.h"
#include "../logger.h"
#include "../tracing.h"

using namespace sw::redis;
namespace social_network {
class HomeTimelineHandler : public HomeTimelineServiceIf {
 public:
  HomeTimelineHandler(Redis *,
                      PostStorageServiceIf *,
                      SocialGraphServiceIf *);


  HomeTimelineHandler(Redis *,Redis *,
      PostStorageServiceIf*,
      SocialGraphServiceIf*);


  HomeTimelineHandler(RedisCluster *,
                      PostStorageServiceIf *,
                      SocialGraphServiceIf *);
  ~HomeTimelineHandler() override = default;

  bool IsRedisReplicationEnabled();

  void ReadHomeTimeline(std::vector<Post> &, int64_t, int64_t, int, int,
                        const std::map<std::string, std::string> &) override;

  void WriteHomeTimeline(int64_t, int64_t, int64_t, int64_t,
                         const std::vector<int64_t> &,
                         const std::map<std::string, std::string> &) override;

 private:
     Redis *_redis_replica_pool;
     Redis *_redis_primary_pool;
     Redis *_redis_client_pool;
     RedisCluster *_redis_cluster_client_pool;
     PostStorageServiceIf *_post_service;
     SocialGraphServiceIf *_social_graph_service;
};

HomeTimelineHandler::HomeTimelineHandler(
    Redis *redis_pool,
    PostStorageServiceIf *post_service,
    SocialGraphServiceIf *social_graph_service) {
    _redis_primary_pool = nullptr;
    _redis_replica_pool = nullptr;
    _redis_client_pool = redis_pool;
    _redis_cluster_client_pool = nullptr;
    _post_service = post_service;
    _social_graph_service = social_graph_service;
}

HomeTimelineHandler::HomeTimelineHandler(
    RedisCluster *redis_pool,
    PostStorageServiceIf *post_service,
    SocialGraphServiceIf *social_graph_service) {
    _redis_primary_pool = nullptr;
    _redis_replica_pool = nullptr;
    _redis_client_pool = nullptr;
    _redis_cluster_client_pool = redis_pool; 
    _post_service = post_service;
    _social_graph_service = social_graph_service;
}

HomeTimelineHandler::HomeTimelineHandler(
    Redis *redis_replica_pool,
    Redis *redis_primary_pool,
    PostStorageServiceIf *post_service,
    SocialGraphServiceIf *social_graph_service) {
    _redis_primary_pool = redis_primary_pool;
    _redis_replica_pool = redis_replica_pool;
    _redis_client_pool = nullptr;
    _redis_cluster_client_pool = nullptr;
    _post_service = post_service;
    _social_graph_service = social_graph_service;
}

bool HomeTimelineHandler::IsRedisReplicationEnabled() {
    return (_redis_primary_pool || _redis_replica_pool);
}

void HomeTimelineHandler::WriteHomeTimeline(
    int64_t req_id, int64_t post_id, int64_t user_id, int64_t timestamp,
    const std::vector<int64_t> &user_mentions_id,
    const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "write_home_timeline_server", {opentracing::ChildOf(parent_span->get())});

  // Find followers of the user
  auto followers_span = opentracing::Tracer::Global()->StartSpan(
      "get_followers_client", {opentracing::ChildOf(&span->context())});
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  opentracing::Tracer::Global()->Inject(followers_span->context(), writer);

  // Updated for a local call.
  std::vector<int64_t> followers_id;
  _social_graph_service->GetFollowers(followers_id, req_id, user_id, writer_text_map);

  followers_span->Finish();

  std::set<int64_t> followers_id_set(followers_id.begin(), followers_id.end());
  followers_id_set.insert(user_mentions_id.begin(), user_mentions_id.end());

  // Update Redis ZSet
  // Zset key: follower_id, Zset value: post_id_str, Zset score: timestamp_str
  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "write_home_timeline_redis_update_client",
      {opentracing::ChildOf(&span->context())});
  std::string post_id_str = std::to_string(post_id);

  {
    if (_redis_client_pool) {
      auto pipe = _redis_client_pool->pipeline(false);
      for (auto &follower_id : followers_id_set) {
        pipe.zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
      }
      try {
        auto replies = pipe.exec();
      } catch (const Error &err) {
        LOG(error) << err.what();
        throw err;
      }
    }
    
    else if (IsRedisReplicationEnabled()) {
        auto pipe = _redis_primary_pool->pipeline(false);
        for (auto& follower_id : followers_id_set) {
            pipe.zadd(std::to_string(follower_id), post_id_str, timestamp,
                UpdateType::NOT_EXIST);
        }
        try {
            auto replies = pipe.exec();
        }
        catch (const Error& err) {
            LOG(error) << err.what();
            throw err;
        }
    }
    
    else {
      // Create multi-pipeline that match with shards pool
      std::map<std::shared_ptr<ConnectionPool>, std::shared_ptr<Pipeline>> pipe_map;
      auto *shards_pool = _redis_cluster_client_pool->get_shards_pool();

      for (auto &follower_id : followers_id_set) {
        auto conn = shards_pool->fetch(std::to_string(follower_id));
        auto pipe = pipe_map.find(conn);
        if(pipe == pipe_map.end()) {//Not found, create new pipeline and insert
          auto new_pipe = std::make_shared<Pipeline>(_redis_cluster_client_pool->pipeline(std::to_string(follower_id), false));
          pipe_map.insert(make_pair(conn, new_pipe));
          auto *_pipe = new_pipe.get();
          _pipe->zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
        }else{//Found, use exist pipeline
          std::pair<std::shared_ptr<ConnectionPool>, std::shared_ptr<Pipeline>> found = *pipe;
          auto *_pipe = found.second.get();
          _pipe->zadd(std::to_string(follower_id), post_id_str, timestamp,
                  UpdateType::NOT_EXIST);
        }
      }
      // LOG(info) <<"followers_id_set items:" << followers_id_set.size()<<"; pipeline items:" << pipe_map.size();
      try {
        for(auto const &it : pipe_map) {
          auto _pipe = it.second.get();
          _pipe->exec();
        }

      } catch (const Error &err) {
        LOG(error) << err.what();
        throw err;
      }
    }
  }
  redis_span->Finish();
}


void HomeTimelineHandler::ReadHomeTimeline(
    std::vector<Post> &_return, int64_t req_id, int64_t user_id, int start_idx,
    int stop_idx, const std::map<std::string, std::string> &carrier) {
  // Initialize a span
  TextMapReader reader(carrier);
  std::map<std::string, std::string> writer_text_map;
  TextMapWriter writer(writer_text_map);
  auto parent_span = opentracing::Tracer::Global()->Extract(reader);
  auto span = opentracing::Tracer::Global()->StartSpan(
      "read_home_timeline_server", {opentracing::ChildOf(parent_span->get())});
  opentracing::Tracer::Global()->Inject(span->context(), writer);

  if (stop_idx <= start_idx || start_idx < 0) {
    return;
  }

  auto redis_span = opentracing::Tracer::Global()->StartSpan(
      "read_home_timeline_redis_find_client",
      {opentracing::ChildOf(&span->context())});

  std::vector<std::string> post_ids_str;
  try {
    if (_redis_client_pool) {
      _redis_client_pool->zrevrange(std::to_string(user_id), start_idx,
                                    stop_idx - 1,
                                    std::back_inserter(post_ids_str));
    }
    else if (IsRedisReplicationEnabled()) {
        _redis_replica_pool->zrevrange(std::to_string(user_id), start_idx,
                                       stop_idx - 1,
                                       std::back_inserter(post_ids_str));
    }
    
    else {
      _redis_cluster_client_pool->zrevrange(std::to_string(user_id), start_idx,
                                            stop_idx - 1,
                                            std::back_inserter(post_ids_str));
    }
  } catch (const Error &err) {
    LOG(error) << err.what();
    throw err;
  }
  redis_span->Finish();

  std::vector<int64_t> post_ids;
  for (auto &post_id_str : post_ids_str) {
    post_ids.emplace_back(std::stoul(post_id_str));
  }

  // Updated for a local call.
  _post_service->ReadPosts(_return, req_id, post_ids, writer_text_map);

  span->Finish();
}

}  // namespace social_network

#endif  // SOCIAL_NETWORK_MICROSERVICES_SRC_HOMETIMELINESERVICE_HOMETIMELINEHANDLER_H_
