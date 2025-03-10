#pragma once
#include <boost/asio.hpp>
#include <unordered_map>
#include <thread>
#include <future>
#include <raft-kv/common/status.h>
#include <raft-kv/raft/proto.h>
#include <msgpack.hpp>

namespace kv {

int string_match_len(const char* pattern, int patternLen,
                     const char* string, int stringLen, int nocase);

struct RedisCommitData {
  static const uint8_t kCommitSet = 0;
  static const uint8_t kCommitDel = 1;

  uint8_t type;
  std::vector<std::string> strs;  // (key and value) or key
  MSGPACK_DEFINE (type, strs);
};

// 提交给 raft 层的一个结构体
struct RaftCommit {

  RaftCommit() {}

  uint32_t node_id;  // 哪个节点的提交
  uint32_t commit_id;  // 提交的 id
  RedisCommitData redis_data;  // 具体的操作（set or delete）和具体的数据 (key value or key)
  MSGPACK_DEFINE (node_id, commit_id, redis_data);
};

typedef std::function<void(const Status&)> StatusCallback;
typedef std::shared_ptr<std::vector<uint8_t>> SnapshotDataPtr;  // 快照数据的指针
typedef std::function<void(SnapshotDataPtr)> GetSnapshotCallback;

class RaftNode;
class RedisStore {
 public:
  explicit RedisStore(RaftNode* server, std::vector<uint8_t> snap, uint16_t port);

  ~RedisStore();

  // 停止 RedisStore 中的通信模块，等待 worker_ 线程结束
  void stop() {
    io_service_.stop();
    if (worker_.joinable()) {
      worker_.join();
    }
  }

  void start(std::promise<pthread_t>& promise);

  bool get(const std::string& key, std::string& value) {
    auto it = key_values_.find(key);
    if (it != key_values_.end()) {
      value = it->second;
      return true;
    } else {
      return false;
    }
  }

  void set(std::string key, std::string value, const StatusCallback& callback);

  void del(std::vector<std::string> keys, const StatusCallback& callback);

  void get_snapshot(const GetSnapshotCallback& callback);

  void recover_from_snapshot(SnapshotDataPtr snap, const StatusCallback& callback);

  void keys(const char* pattern, int len, std::vector<std::string>& keys);

  void read_commit(proto::EntryPtr entry);

 private:
  void start_accept();

  RaftNode* server_;
  boost::asio::io_service io_service_;  // io 模块
  boost::asio::ip::tcp::acceptor acceptor_;
  std::thread worker_;
  std::unordered_map<std::string, std::string> key_values_;  // 实际的 kv 储存
  uint32_t next_request_id_;
  std::unordered_map<uint32_t, StatusCallback> pending_requests_;  // 待处理的 request，k: request_id, v:处理函数
};

}
