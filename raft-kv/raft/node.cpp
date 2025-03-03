#include <raft-kv/raft/node.h>
#include <raft-kv/common/log.h>

namespace kv {

// 给定 conf 和 同伴节点 创建一个新的节点
Node* Node::start_node(const Config& conf, const std::vector<PeerContext>& peers) {
  return new RawNode(conf, peers);
}

// 根据 conf 重启 node
Node* Node::restart_node(const Config& conf) {
  return new RawNode(conf);
}

RawNode::RawNode(const Config& conf, const std::vector<PeerContext>& peers) {
  raft_ = std::make_shared<Raft>(conf);  // 根据 conf 持有一个 raft_ 层的结构

  uint64_t last_index = 0;
  Status status = conf.storage->last_index(last_index); // 获取该节点日志中的最后一个索引last_index
  if (!status.is_ok()) {
    LOG_FATAL("%s", status.to_string().c_str());
  }

  // If the log is empty, this is a new RawNode (like StartNode); otherwise it's
  // restoring an existing RawNode (like RestartNode).
  if (last_index == 0) {  // 如果last_index为空，则表明是一个新节点
    raft_->become_follower(1, 0);  // 设置为一个跟随者，term 为 1， leader 为 0

    std::vector<proto::EntryPtr> entries;

    for (size_t i = 0; i < peers.size(); ++i) {
      auto& peer = peers[i];
      proto::ConfChange cs = proto::ConfChange{  // 添加配置更改的结构体
          .id = 0,
          .conf_change_type = proto::ConfChangeAddNode,
          .node_id = peer.id,  // 同伴的 id
          .context = peer.context,  // 同伴的 ctx
      };

      std::vector<uint8_t> data = cs.serialize();  // 序列化配置更改的结构体变成data

      // 将序列化后的数据 data 移动到 entry 中，并且push到 vector 中
      proto::EntryPtr entry(new proto::Entry());
      entry->type = proto::EntryConfChange;
      entry->term = 1;  // 新节点的term
      entry->index = i + 1;  // 新节点的日志 index
      entry->data = std::move(data);
      entries.push_back(entry);
    }

    raft_->raft_log_->append(entries);  // 追加日志到 unstable 中
    raft_->raft_log_->committed_ = entries.size();  // 更新自己的 committed Index

    for (auto& peer : peers) {
      raft_->add_node(peer.id);  // 通知同伴节点每一个都将添加此新节点，节点的状态 先为非 learner
    }
  }

  // Set the initial hard and soft states after performing all initialization.
  prev_soft_state_ = raft_->soft_state();
  if (last_index == 0) {  // 新节点无硬状态
    prev_hard_state_ = proto::HardState();
  } else {  // 旧节点可以获取硬状态
    prev_hard_state_ = raft_->hard_state();
  }
}

// 重启节点调用的就是RawNode(const Config& conf)，不需要加入 peers 参数，重启的时候直接通过 Hard State可以知道 node 有谁，learner 有谁
RawNode::RawNode(const Config& conf) {

  uint64_t last_index = 0;
  Status status = conf.storage->last_index(last_index);
  if (!status.is_ok()) {
    LOG_FATAL("%s", status.to_string().c_str());
  }

  raft_ = std::make_shared<Raft>(conf);

  // Set the initial hard and soft states after performing all initialization.
  prev_soft_state_ = raft_->soft_state();
  if (last_index == 0) {
    prev_hard_state_ = proto::HardState();
  } else {
    prev_hard_state_ = raft_->hard_state();
  }
}

// 绑定 raft_ 的 tick 函数
void RawNode::tick() {
  raft_->tick();
}

// 通过 MsgHup 发起选举
Status RawNode::campaign() {
  proto::MessagePtr msg(new proto::Message());
  msg->type = proto::MsgHup;
  return raft_->step(std::move(msg));
}

// 一笔正常日志，data 实际就是 redisSession 接收到的一个 set 或者 del 命令时候，
// 经过redisSession -> redisStore -> io_service -> RawNode -> raft_ 层层传递后，pack起来的数据
Status RawNode::propose(std::vector<uint8_t> data) {
  proto::MessagePtr msg(new proto::Message());
  msg->type = proto::MsgProp;
  msg->from = raft_->id_;
  msg->entries.emplace_back(proto::EntryNormal, 0, 0, std::move(data)); // 将日志条目放入 entries 中，Entry{type, term, index,data}

  return raft_->step(std::move(msg));
}

// 一笔状态集群更改日志
Status RawNode::propose_conf_change(const proto::ConfChange& cc) {
  proto::MessagePtr msg(new proto::Message());
  msg->type = proto::MsgProp;
  msg->entries.emplace_back(proto::EntryConfChange, 0, 0, cc.serialize());
  return raft_->step(std::move(msg));
}

Status RawNode::step(proto::MessagePtr msg) {
  // ignore unexpected local messages receiving over network
  if (msg->is_local_msg()) {  // 忽略本地之间发送的 message，因为本地的消息不应该用网络进行传输，而是内部传输
    return Status::invalid_argument("raft: cannot step raft local message");
  }

  ProgressPtr progress = raft_->get_progress(msg->from);
  if (progress || !msg->is_response_msg()) {  // 如果能在 progress 中找到对应的节点，说明消息来自于集群中的一个已知节点或者是一个非请求类信息，因此进行 step 处理
    return raft_->step(msg);
  }
  return Status::invalid_argument("raft: cannot step as peer not found");
}

// 通过node调用raft 层的函数“告知” raft 层我已经把ready中的信息都处理掉了
ReadyPtr RawNode::ready() {
  ReadyPtr rd = std::make_shared<Ready>(raft_, prev_soft_state_, prev_hard_state_);
  raft_->msgs_.clear(); // 清除待发送的消息，表明如果我们调用了 ready 的方法，肯定是我已经处理了raft 层发给我的所有消息，也就是调用了 advance()
  raft_->reduce_uncommitted_size(rd->committed_entries);  // 清除已经提交的 message
  return rd;
}

// 检查 RawNode 是否有需要处理的状态更新、消息、日志条目或快照。通过这些检查，系统可以决定是否需要调用 ready 方法来获取并处理这些更新。
bool RawNode::has_ready() {
  assert(prev_soft_state_);  // 确保软状态不为空
  if (!raft_->soft_state()->equal(*prev_soft_state_)) {  // 软状态和之前的是否一样，不一样则返回 true
    return true;
  }
  proto::HardState hs = raft_->hard_state();  // 硬状态
  if (!hs.is_empty_state() && !hs.equal(prev_hard_state_)) {  // 硬状态和之前的是否一样，不一样则返回 true
    return true;
  }

  proto::SnapshotPtr snapshot = raft_->raft_log_->unstable_->snapshot_;  // 是否有 snapshot， 有则返回true

  if (snapshot && !snapshot->is_empty()) {  // snapshot 不为空
    return true;
  }
  if (!raft_->msgs_.empty() || !raft_->raft_log_->unstable_entries().empty()
      || raft_->raft_log_->has_next_entries()) {  // 如果有待发送的消息、不稳定的日志条目，或有下一个日志条目，返回 true，表示有新的消息或日志需要处理。
    return true;
  }

  return !raft_->read_states_.empty();  // 如果有未处理的读状态，也返回 true
}

// raft层会改变节点soft_state、hard_state、所有节点的可以 apply 的位置等信息
// 这些信息会通过 ready 结构体反馈到 RawNode 上，node 调用advance()会发现Ready被改变了，
// 然后 node 会给你日剧Ready 中被改变的信息然后调用 raft 层改变其raft_log 中各种的信息，比如 apply 的位置，已经持久化日志的位置，已经快照的位置等
void RawNode::advance(ReadyPtr rd) {
  if (rd->soft_state) {
    prev_soft_state_ = rd->soft_state;

  }
  if (!rd->hard_state.is_empty_state()) {
    prev_hard_state_ = rd->hard_state;
  }

  // If entries were applied (or a snapshot), update our cursor for
  // the next Ready. Note that if the current HardState contains a
  // new Commit index, this does not mean that we're also applying
  // all of the new entries due to commit pagination by size.
  uint64_t index = rd->applied_cursor();
  if (index > 0) {
    raft_->raft_log_->applied_to(index);
  }

  if (!rd->entries.empty()) {
    auto& entry = rd->entries.back();
    raft_->raft_log_->stable_to(entry->index, entry->term);
  }

  if (!rd->snapshot.is_empty()) {
    raft_->raft_log_->stable_snap_to(rd->snapshot.metadata.index);
  }

  if (!rd->read_states.empty()) {
    raft_->read_states_.clear();
  }
}

proto::ConfStatePtr RawNode::apply_conf_change(const proto::ConfChange& cc) {
  proto::ConfStatePtr state(new proto::ConfState());
  if (cc.node_id == 0) {  // 如果 node_id 为 0，表明只是返回集群的 node 和 learner，并非真的进行集群状态变更
    raft_->nodes(state->nodes);  
    raft_->learner_nodes(state->learners);
    return state;
  }

  switch (cc.conf_change_type) {
    case proto::ConfChangeAddNode: {  // 添加一个普通节点，!learner
      raft_->add_node_or_learner(cc.node_id, false);
      break;
    }
    case proto::ConfChangeAddLearnerNode: {  // 添加一个learner
      raft_->add_node_or_learner(cc.node_id, true);
      break;
    }
    case proto::ConfChangeRemoveNode: {  // 删除一个节点
      raft_->remove_node(cc.node_id);
      break;
    }
    case proto::ConfChangeUpdateNode: {
      LOG_DEBUG("ConfChangeUpdate");
      break;
    }
    default: {
      LOG_FATAL("unexpected conf type");
    }
  }
  // 返回集群配置变更后的 node 和 learner
  raft_->nodes(state->nodes);
  raft_->learner_nodes(state->learners);
  return state;
}

// 转移 leader
void RawNode::transfer_leadership(uint64_t lead, ino64_t transferee) {
  // 手动设置“from”和“to”，以便领导者可以自愿转移其领导权
  proto::MessagePtr msg(new proto::Message());
  msg->type = proto::MsgTransferLeader;
  msg->from = transferee;  // 尝试转移的新leader
  msg->to = lead;  // 旧leader

  Status status = raft_->step(std::move(msg));
  if (!status.is_ok()) {
    LOG_WARN("transfer_leadership %s", status.to_string().c_str());
  }
}

// 读请求
Status RawNode::read_index(std::vector<uint8_t> rctx) {
  proto::MessagePtr msg(new proto::Message());
  msg->type = proto::MsgReadIndex;
  msg->entries.emplace_back(proto::MsgReadIndex, 0, 0, std::move(rctx));  // 读请求并不需要像日志复制一样需要 index 和 term，因此设置为 0
  return raft_->step(std::move(msg));
}

RaftStatusPtr RawNode::raft_status() {
  LOG_DEBUG("no impl yet");
  return nullptr;
}

// 报告不可达消息
void RawNode::report_unreachable(uint64_t id) {
  proto::MessagePtr msg(new proto::Message());
  msg->type = proto::MsgUnreachable;
  msg->from = id;

  Status status = raft_->step(std::move(msg));
  if (!status.is_ok()) {
    LOG_WARN("report_unreachable %s", status.to_string().c_str());
  }
}

// 汇报来自 id 的快照状态
void RawNode::report_snapshot(uint64_t id, SnapshotStatus status) {
  bool rej = (status == SnapshotFailure);  // rej = true if snapshot failure
  proto::MessagePtr msg(new proto::Message());
  msg->type = proto::MsgSnapStatus;
  msg->from = id;
  msg->reject = rej;

  Status s = raft_->step(std::move(msg));
  if (!s.is_ok()) {
    LOG_WARN("report_snapshot %s", s.to_string().c_str());
  }
}

void RawNode::stop() {

}

}