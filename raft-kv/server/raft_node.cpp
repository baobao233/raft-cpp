#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <future>
#include <raft-kv/server/raft_node.h>
#include <raft-kv/common/log.h>

namespace kv {

static uint64_t defaultSnapCount = 100000;
static uint64_t snapshotCatchUpEntriesN = 100000;

RaftNode::RaftNode(uint64_t id, const std::string& cluster, uint16_t port)
    : port_(port),
      pthread_id_(0),
      timer_(io_service_),
      id_(id),
      last_index_(0),
      conf_state_(new proto::ConfState()),
      snapshot_index_(0),
      applied_index_(0),
      storage_(new MemoryStorage()),
      snap_count_(defaultSnapCount) {
  boost::split(peers_, cluster, boost::is_any_of(","));  // 将输入的集群信息 split 到 peers_中
  if (peers_.empty()) {
    LOG_FATAL("invalid args %s", cluster.c_str());
  }

  std::string work_dir = "node_" + std::to_string(id);
  snap_dir_ = work_dir + "/snap";
  wal_dir_ = work_dir + "/wal";

  if (!boost::filesystem::exists(snap_dir_)) {  // 判断是否存在 snapshot 储存路径
    boost::filesystem::create_directories(snap_dir_);  // 不存在则创建
  }

  snapshotter_.reset(new Snapshotter(snap_dir_));  // 初始化快照模块

  bool wal_exists = boost::filesystem::exists(wal_dir_);  // wal 储存路径是否存在

  replay_WAL();  // 根据 WAL 恢复

  Config c;  // 设置配置启动 raft 算法层
  c.id = id;
  c.election_tick = 10;
  c.heartbeat_tick = 1;
  c.storage = storage_;
  c.applied = 0;
  c.max_size_per_msg = 1024 * 1024;
  c.max_committed_size_per_ready = 0;
  c.max_uncommitted_entries_size = 1 << 30;
  c.max_inflight_msgs = 256;
  c.check_quorum = true;
  c.pre_vote = true;
  c.read_only_option = ReadOnlySafe;  // 只允许 leader回复
  c.disable_proposal_forwarding = false;

  Status status = c.validate();

  if (!status.is_ok()) {
    LOG_FATAL("invalid configure %s", status.to_string().c_str());
  }

  if (wal_exists) {  // 并非第一次启动，直接按照配置 restart 即可
    node_.reset(Node::restart_node(c));
  } else {  // 第一次启动，需要配置peers
    std::vector<PeerContext> peers;
    for (size_t i = 0; i < peers_.size(); ++i) {
      peers.push_back(PeerContext{.id = i + 1});
    }
    node_.reset(Node::start_node(c, peers));  // 第一次启动
  }
}

RaftNode::~RaftNode() {
  LOG_DEBUG("stopped");
  if (transport_) {  // 通知通信层表示 stop
    transport_->stop();
    transport_ = nullptr;
  }
}

void RaftNode::start_timer() {
  timer_.expires_from_now(boost::posix_time::millisec(100));  // 定义一个 100ms 的定时任务
  timer_.async_wait([this](const boost::system::error_code& err) {  // 同步调用以下逻辑
    if (err) {
      LOG_ERROR("timer waiter error %s", err.message().c_str());
      return;
    }

    this->start_timer();
    this->node_->tick();  // 调用节点的 tick 函数
    this->pull_ready_events();  // 推送 ready 事件检查是否有新状态
  });
}

// 推送 ready 事件
void RaftNode::pull_ready_events() {
  assert(pthread_id_ == pthread_self());
  while (node_->has_ready()) { // 检查是否有新的状态发生变化
    auto rd = node_->ready();  // 通知 raft 层已经收到消息，并且将 raft 层的软状态和硬状态封窗成了 ReadyPtr 返回来到本层
    if (!rd->contains_updates()) {
      LOG_WARN("ready not contains updates");
      return;
    }

    wal_->save(rd->hard_state, rd->entries);  // 往 wal 追加并且落盘

    if (!rd->snapshot.is_empty()) {  // raft层传上来的 rd 中，快照也不为空，则落盘快照
      Status status = save_snap(rd->snapshot);
      if (!status.is_ok()) {
        LOG_FATAL("save snapshot error %s", status.to_string().c_str());
      }
      storage_->apply_snapshot(rd->snapshot);  // 通知与 raft 相关的持久化层储存entry，压缩 entry ，应用快照，创建快照，储存快照等操作
      publish_snapshot(rd->snapshot);          // 根据 snapshot 恢复 redis 中的 key_value_，并且根据回传的 snapshot 更新snapshot_index_，
    }

    if (!rd->entries.empty()) {  // 通知存储层需要持久化的 entry
      storage_->append(rd->entries);
    }
    if (!rd->messages.empty()) {  // 调用传输模块传输信息
      transport_->send(rd->messages);
    }

    if (!rd->committed_entries.empty()) {    // 包含大部分节点已提交日志，需要推进应用到状态机上
      std::vector<proto::EntryPtr> ents;
      entries_to_apply(rd->committed_entries, ents);  
      if (!ents.empty()) {
        publish_entries(ents);
      }
    }
    maybe_trigger_snapshot();
    node_->advance(rd);  // 通知 raft 表示已经完成推进 apply
  }
}

// 先落盘 snapshot 类型的 WAL，再调用 snapshot 模块落盘snapshot
Status RaftNode::save_snap(const proto::Snapshot& snap) {
  // must save the snapshot index to the WAL before saving the
  // snapshot to maintain the invariant that we only Open the
  // wal at previously-saved snapshot indexes.
  Status status;

  WAL_Snapshot wal_snapshot;
  wal_snapshot.index = snap.metadata.index;
  wal_snapshot.term = snap.metadata.term;

  status = wal_->save_snapshot(wal_snapshot);  //  落盘 wal
  if (!status.is_ok()) {
    return status;
  }

  status = snapshotter_->save_snap(snap);  // 落盘真正的 snapshot
  if (!status.is_ok()) {
    LOG_FATAL("save snapshot error %s", status.to_string().c_str());
  }
  return status;

  return wal_->release_to(snap.metadata.index);
}

void RaftNode::publish_snapshot(const proto::Snapshot& snap) {
  if (snap.is_empty()) {
    return;
  }

  LOG_DEBUG("publishing snapshot at index %lu", snapshot_index_);

  if (snap.metadata.index <= applied_index_) {
    LOG_FATAL("snapshot index [%lu] should > progress.appliedIndex [%lu] + 1", snap.metadata.index, applied_index_);
  }

  //trigger to load snapshot
  proto::SnapshotPtr snapshot(new proto::Snapshot());
  snapshot->metadata = snap.metadata;
  SnapshotDataPtr data(new std::vector<uint8_t>(snap.data));

  *(this->conf_state_) = snapshot->metadata.conf_state;
  snapshot_index_ = snapshot->metadata.index; // 由raft层回传的snapshot更新snapshot_index_
  applied_index_ = snapshot->metadata.index;  // 由raft层回传的snapshot更新applied_index_

  // 同时根据raft 层传回的 snapshot 更新 redis
  redis_server_->recover_from_snapshot(data, [snapshot, this](const Status& status) {
    //由redis线程回调
    if (!status.is_ok()) {
      LOG_FATAL("recover from snapshot error %s", status.to_string().c_str());
    }

    LOG_DEBUG("finished publishing snapshot at index %lu", snapshot_index_);
  });

}

// 该函数的主要作用是确保 WAL 目录存在，并根据给定的快照信息打开或创建 WAL 文件。
void RaftNode::open_WAL(const proto::Snapshot& snap) {
  if (!boost::filesystem::exists(wal_dir_)) {
    boost::filesystem::create_directories(wal_dir_);
    WAL::create(wal_dir_);
  }

  WAL_Snapshot walsnap;
  walsnap.index = snap.metadata.index;
  walsnap.term = snap.metadata.term;
  LOG_INFO("loading WAL at term %lu and index %lu", walsnap.term, walsnap.index);

  wal_ = WAL::open(wal_dir_, walsnap);
}

// 节点启动的时候根据 WAL 文件加载节点状态，entry  等信息
void RaftNode::replay_WAL() {
  LOG_DEBUG("replaying WAL of member %lu", id_);

  proto::Snapshot snapshot;
  Status status = snapshotter_->load(snapshot); // 从 disk 的 snapshot 文件中加载 snapshot
  if (!status.is_ok()) {
    if (status.is_not_found()) {
      LOG_INFO("snapshot not found for node %lu", id_);
    } else {
      LOG_FATAL("error loading snapshot %s", status.to_string().c_str());
    }
  } else {
    storage_->apply_snapshot(snapshot);  // 根据 disk 中的快照数据应用到storage_上
  }

  open_WAL(snapshot);  // 根据 snapshot 打开 wal 文件
  assert(wal_ != nullptr);

  proto::HardState hs;
  std::vector<proto::EntryPtr> ents;
  status = wal_->read_all(hs, ents);  // 从每个 wal 文件中读取 HS 和加载 entries
  if (!status.is_ok()) {
    LOG_FATAL("failed to read WAL %s", status.to_string().c_str());
  }

  storage_->set_hard_state(hs);  // 加载HS

  // append to storage so raft starts at the right place in log
  storage_->append(ents);  // 添加从WAL 加载回来的entry

  // send nil once lastIndex is published so client knows commit channel is current
  if (!ents.empty()) {
    last_index_ = ents.back()->index;
  } else {
    snap_data_ = std::move(snapshot.data);  // 记录从WAL加载的 snapshot
  }
}

// 具体entry操作的分发
bool RaftNode::publish_entries(const std::vector<proto::EntryPtr>& entries) {
  for (const proto::EntryPtr& entry : entries) {
    switch (entry->type) {
      case proto::EntryNormal: {  // 普通日志
        if (entry->data.empty()) {
          // ignore empty messages
          break;
        }
        redis_server_->read_commit(entry);
        break;
      }

      case proto::EntryConfChange: {  // 配置变更日志
        proto::ConfChange cc;
        try {
          msgpack::object_handle oh = msgpack::unpack((const char*) entry->data.data(), entry->data.size());
          oh.get().convert(cc);
        }
        catch (std::exception& e) {
          LOG_ERROR("invalid EntryConfChange msg %s", e.what());
          continue;
        }
        conf_state_ = node_->apply_conf_change(cc);  // 调用 node 进行对应集群配置的更改

        switch (cc.conf_change_type) { 
          case proto::ConfChangeAddNode: // 如果是节点添加信息
            if (!cc.context.empty()) {
              std::string str((const char*) cc.context.data(), cc.context.size());  // address
              transport_->add_peer(cc.node_id, str);  // 开启一个 node 的通信模块，并且发送一则 debug 消息校验是否成功
            }
            break;
          case proto::ConfChangeRemoveNode:  // 如果是节点移除消息
            if (cc.node_id == id_) {  // 如果移除的是自己
              LOG_INFO("I've been removed from the cluster! Shutting down.");
              return false;
            }
            transport_->remove_peer(cc.node_id); // 还没实现
          default: {
            LOG_INFO("configure change %d", cc.conf_change_type);
          }
        }
        break;
      }
      default: {
        LOG_FATAL("unknown type %d", entry->type);
        return false;
      }
    }

    // 更新已经应用的 entry
    applied_index_ = entry->index;

    // replay has finished
    if (entry->index == this->last_index_) {
      LOG_DEBUG("replay has finished");
    }
  }
  return true;
}

// 返回可以 apply 的 entries
void RaftNode::entries_to_apply(const std::vector<proto::EntryPtr>& entries, std::vector<proto::EntryPtr>& ents) {
  if (entries.empty()) {
    return;
  }

  uint64_t first = entries[0]->index;  // 可以 apply 的第一个 index
  if (first > applied_index_ + 1) {   
    LOG_FATAL("first index of committed entry[%lu] should <= progress.appliedIndex[%lu]+1", first, applied_index_);
  }
  // 可以 apply 的第一个 index 必须和已经应用到状态机上的 index 刚好拼上（也就是下一条）或者有重叠，才能说明可以 apply 的日志条目是能接的上的
  if (applied_index_ - first + 1 < entries.size()) {  
    ents.insert(ents.end(), entries.begin() + applied_index_ - first + 1, entries.end());
  }
}

// 触发 snapshot 的时间点
void RaftNode::maybe_trigger_snapshot() {
  if (applied_index_ - snapshot_index_ <= snap_count_) {  // 已经 apply 的 index距离上次 snapshot 的 index 还没超过设定的 snap_count_，则不触发
    return;
  }

  LOG_DEBUG("start snapshot [applied index: %lu | last snapshot index: %lu], snapshot count[%lu]",
            applied_index_,
            snapshot_index_,
            snap_count_);

  // 启动一个异步任务，从 redies 中获取一整个 kv 信息快照数据
  std::promise<SnapshotDataPtr> promise;
  std::future<SnapshotDataPtr> future = promise.get_future();
  redis_server_->get_snapshot(std::move([&promise](const SnapshotDataPtr& data) {
    promise.set_value(data);
  }));

  future.wait();  // 等待 redis 传入一整个 kv
  SnapshotDataPtr snapshot_data = future.get();  // snapshot 的 data 是整个 kv 集

  proto::SnapshotPtr snap;
  Status status = storage_->create_snapshot(applied_index_, conf_state_, *snapshot_data, snap);
  if (!status.is_ok()) {
    LOG_FATAL("create snapshot error %s", status.to_string().c_str());
  }

  status = save_snap(*snap);  // 保存快照
  if (!status.is_ok()) {
    LOG_FATAL("save snapshot error %s", status.to_string().c_str());
  }

  uint64_t compactIndex = 1;
  if (applied_index_ > snapshotCatchUpEntriesN) {
    compactIndex = applied_index_ - snapshotCatchUpEntriesN;
  }
  status = storage_->compact(compactIndex);  // 压缩此前的快照
  if (!status.is_ok()) {
    LOG_FATAL("compact error %s", status.to_string().c_str());
  }
  LOG_INFO("compacted log at index %lu", compactIndex);
  snapshot_index_ = applied_index_;  // 更新快照 index
}

// 启动 raft_node
void RaftNode::schedule() {
  pthread_id_ = pthread_self();  // 获取当前的线程 id

  proto::SnapshotPtr snap;
  Status status = storage_->snapshot(snap);  // 获取当前的快照
  if (!status.is_ok()) {
    LOG_FATAL("get snapshot failed %s", status.to_string().c_str());
  }

  // 加载 snapshot 配置
  *conf_state_ = snap->metadata.conf_state;
  snapshot_index_ = snap->metadata.index;
  applied_index_ = snap->metadata.index;

  redis_server_ = std::make_shared<RedisStore>(this, std::move(snap_data_), port_);  // 创建 redis 实例
  std::promise<pthread_t> promise;
  std::future<pthread_t> future = promise.get_future();
  redis_server_->start(promise);
  future.wait();  //  获取启动当前 raft node 的线程 id
  pthread_t id = future.get();
  LOG_DEBUG("server start [%lu]", id);

  start_timer();
  io_service_.run();
}

// redisSession 发现需要执行的命令是 set 或者 del 的时候会调用 redisStore 的 set 或者 del 命令，
// 并且传入命令完成后执行的回调函数 callback，redisStore 然后会注册请求，并且调用 RaftNode 模块中的 propose 方法
// 将这个请求通过通信模块 io_service 传到 raft_ 层进行记录一笔正常的日志
void RaftNode::propose(std::shared_ptr<std::vector<uint8_t>> data, const StatusCallback& callback) {
  if (pthread_id_ != pthread_self()) {  // 如果不是当前线程处理，需要经过 io_service 转发，将转发函数传入 post 中
    io_service_.post([this, data, callback]() {
      Status status = node_->propose(std::move(*data));  // raft层会返回一个 status 表示这次操作的成功或者失败
      callback(status);  // 执行回调函数
      pull_ready_events(); // 回调函数执行完毕之后，raftNode自己调用pull_ready_events()推动 raftNode 检查是否需要将 raft 层新的状信息持久化，里面是一个 while 死循环
    });
  } else {  // 当前线程直接处理
    Status status = node_->propose(std::move(*data));
    callback(status);
    pull_ready_events();
  }
}

// 调用 raft 层的 step 函数处理 message，具体和上面的 process() 差不多
void RaftNode::process(proto::MessagePtr msg, const StatusCallback& callback) {
  if (pthread_id_ != pthread_self()) {
    io_service_.post([this, msg, callback]() {
      Status status = this->node_->step(msg);  // raft 层 处理，处理后的状态如 committedIndex，snapshot，apply entry 的更新会以ReadyPtr穿回来
      callback(status);
      pull_ready_events();  // 推动持久化层对 raft 回传的消息操作
    });
  } else {
    Status status = this->node_->step(msg);
    callback(status);
    pull_ready_events();
  }
}

void RaftNode::is_id_removed(uint64_t id, const std::function<void(bool)>& callback) {
  LOG_DEBUG("no impl yet");
  callback(false);
}

void RaftNode::report_unreachable(uint64_t id) {
  LOG_DEBUG("no impl yet");
}

void RaftNode::report_snapshot(uint64_t id, SnapshotStatus status) {
  LOG_DEBUG("no impl yet");
}

static RaftNodePtr g_node = nullptr;

void on_signal(int) {
  LOG_INFO("catch signal");
  if (g_node) {
    g_node->stop();
  }
}

void RaftNode::main(uint64_t id, const std::string& cluster, uint16_t port) {
  ::signal(SIGINT, on_signal);
  ::signal(SIGHUP, on_signal);
  g_node = std::make_shared<RaftNode>(id, cluster, port);  // 启动一个main，相当于启动了一个节点

  g_node->transport_ = Transport::create(g_node.get(), g_node->id_);  // 创建通信模块
  std::string& host = g_node->peers_[id - 1];
  g_node->transport_->start(host);  // run

  for (uint64_t i = 0; i < g_node->peers_.size(); ++i) {
    uint64_t peer = i + 1;
    if (peer == g_node->id_) {
      continue;
    }
    g_node->transport_->add_peer(peer, g_node->peers_[i]);  // 添加 peer 节点记录
  }

  g_node->schedule();  // 调度
}

// 停止redisStore 和 transport_ 模块结束
void RaftNode::stop() {
  LOG_DEBUG("stopping");
  redis_server_->stop();  // 停止 redisStore

  if (transport_) {
    transport_->stop(); // 同样是调用io_service_.stop()，但后面会释放指针指向
    transport_ = nullptr;
  }
  io_service_.stop();
}

}
