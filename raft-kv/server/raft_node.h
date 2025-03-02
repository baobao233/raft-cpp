#pragma once
#include <string>
#include <stdint.h>
#include <memory>
#include <vector>
#include <raft-kv/transport/transport.h>
#include <raft-kv/raft/node.h>
#include <raft-kv/server/redis_store.h>
#include <raft-kv/wal/wal.h>
#include <raft-kv/snap/snapshotter.h>

namespace kv {

// 应用层和外部打交道时候的接口
class RaftNode : public RaftServer {
 public:
  static void main(uint64_t id, const std::string& cluster, uint16_t port);

  explicit RaftNode(uint64_t id, const std::string& cluster, uint16_t port);

  ~RaftNode() final;

  void stop();

  void propose(std::shared_ptr<std::vector<uint8_t>> data, const StatusCallback& callback);

  void process(proto::MessagePtr msg, const StatusCallback& callback) final;

  void is_id_removed(uint64_t id, const std::function<void(bool)>& callback) final;

  void report_unreachable(uint64_t id) final;

  void report_snapshot(uint64_t id, SnapshotStatus status) final;

  uint64_t node_id() const final { return id_; }

  bool publish_entries(const std::vector<proto::EntryPtr>& entries);
  void entries_to_apply(const std::vector<proto::EntryPtr>& entries, std::vector<proto::EntryPtr>& ents);
  void maybe_trigger_snapshot();

 private:
  void start_timer();
  void pull_ready_events();
  Status save_snap(const proto::Snapshot& snap);
  void publish_snapshot(const proto::Snapshot& snap);

  // replay_WAL replays WAL entries into the raft instance.
  void replay_WAL();
  // open_WAL opens a WAL ready for reading.
  void open_WAL(const proto::Snapshot& snap);

  void schedule();

  uint16_t port_;
  pthread_t pthread_id_;
  boost::asio::io_service io_service_;
  boost::asio::deadline_timer timer_;
  uint64_t id_;
  std::vector<std::string> peers_;  // 集群中所有节点的地址
  uint64_t last_index_;  // 节点中 entries 最后的 index
  proto::ConfStatePtr conf_state_;
  uint64_t snapshot_index_;
  uint64_t applied_index_;

  MemoryStoragePtr storage_;  // storage 层的具体实现，目前是通过 redis 来做的
  std::unique_ptr<Node> node_;  // raft层唯一的入口
  TransporterPtr transport_;  // 网络通信的具体实现
  std::shared_ptr<RedisStore> redis_server_;  
  std::vector<uint8_t> snap_data_;
  std::string snap_dir_;
  uint64_t snap_count_;
  std::unique_ptr<Snapshotter> snapshotter_; // 快照模块，unique_ptr

  std::string wal_dir_;
  WAL_ptr wal_;
};
typedef std::shared_ptr<RaftNode> RaftNodePtr;

}