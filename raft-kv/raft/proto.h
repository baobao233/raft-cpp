#pragma once
#include <stdint.h>
#include <vector>
#include <msgpack.hpp>

namespace kv {

namespace proto {

typedef uint8_t MessageType;

const MessageType MsgHup = 0;  // 收到此消息表示当前节点需要进入选举的状态，那么就是说本节点发现在选举超时时间内没有收到leader心跳消息，所以需要发起选举
const MessageType MsgBeat = 1; // 用于让leader保持与集群中其他节点的连接。Raft 协议规定，leader需要定期发送心跳消息以防止选举超时。
const MessageType MsgProp = 2; // 当领导者收到日志条目提议时，它需要将日志条目添加到日志中，并广播该条目。此时需要检查一些特殊情况，例如当前是否正在进行领导转移。
const MessageType MsgApp = 3;  // follower 收到了 leader 发来的日志同步请求，follower 需要将日志同步到本地，并回复 leader。
const MessageType MsgAppResp = 4; // 当 follower 回复领导者的日志同步请求时，领导者需要更新 follower 的同步进度。如果同步失败，领导者需要回退到前一个日志条目。
const MessageType MsgVote = 5;  // 收到了来自candidate的投票请求
const MessageType MsgVoteResp = 6; // candidate 收到的其他节点对MsgVote 的投票结果
const MessageType MsgSnap = 7;
const MessageType MsgHeartbeat = 8;      // leader 响应ReadIndex 类型的信息时需要向其余节点发送MsgHeartbeat类型的信息，希望自己能回复读请求
const MessageType MsgHeartbeatResp = 9;  // leader 响应ReadIndex 类型的信息时需要通过其余节点发回MsgHeartbeatResp类型的信息证明 leader具备回复 ReadIndex 请求的能力
const MessageType MsgUnreachable = 10;
const MessageType MsgSnapStatus = 11;
const MessageType MsgCheckQuorum = 12; // Raft 集群中的每个节点都需要周期性地检查是否满足法定人数要求。如果某个节点（包括领导者）发现当前集群无法满足法定人数（quorum），它将退位成为 follower。
const MessageType MsgTransferLeader = 13; // 由应用层中的 raftNode 调用transfer_leadership函数，手动设置 from 和 to，令 leader 可以转移leader 身份
const MessageType MsgTimeoutNow = 14;     // leader 收到应用层发来的MsgTransferLeader消息的时候，会给 follower 发送MsgTimeoutNow消息，进行领导权转让
const MessageType MsgReadIndex = 15; // 该节点接收到了一个读请求。 Raft 协议支持只读操作，领导者需要处理来自客户端的读取请求。领导者需要确保至少提交一个日志条目才能处理该请求。
const MessageType MsgReadIndexResp = 16;  // 读请求的响应类型

// 预选举，目的是为了解决网络分区时，小分区节点始终无法收到 leader 的心跳信息，但是又没有办法获得大多数节点的投票，导致无限自增 term 的无意义选举过程。
// 因此，candidate 想发起真正选举之前会进行一轮预选举，当能收到多数派的节点响应时，才去真正发起选举
const MessageType MsgPreVote = 17;  
const MessageType MsgPreVoteResp = 18;  // 作为 candidate 收到其他节点对 MsgPreVote 的投票结果

const MessageType MsgTypeSize = 19;

const char* msg_type_to_string(MessageType type);

typedef uint8_t EntryType;

const EntryType EntryNormal = 0;
const EntryType EntryConfChange = 1;

const char* entry_type_to_string(EntryType type);

// Entry 是写请求或者配置变更的预写日志包装起来的一个结构体，同时也表示我们需要同步给 follower 的日志
struct Entry {
  Entry()
      : type(EntryNormal),
        term(0),
        index(0) {}

  explicit Entry(Entry&& entry)
      : type(entry.type),
        term(entry.term),
        index(entry.index),
        data(std::move(entry.data)) {

  }

  kv::proto::Entry& operator=(const kv::proto::Entry& entry) = default;
  Entry(const Entry& entry) = default;

  explicit Entry(EntryType type, uint64_t term, uint64_t index, std::vector<uint8_t> data)
      : type(type),
        term(term),
        index(index),
        data(std::move(data)) {}

  uint32_t serialize_size() const;

  // 单纯数据的大小
  uint32_t payload_size() const {
    return static_cast<uint32_t>(data.size());
  }

  bool operator==(const Entry& entry) const {
    return type == entry.type && term == entry.term && index == entry.index && data == entry.data;
  }
  bool operator!=(const Entry& entry) const {
    return !(*this == entry);
  }

  EntryType type;
  uint64_t term;
  uint64_t index;
  std::vector<uint8_t> data;
  MSGPACK_DEFINE (type, term, index, data);
};
typedef std::shared_ptr<Entry> EntryPtr;

struct ConfState {
  bool operator==(const ConfState& cs) const {
    return nodes == cs.nodes && learners == cs.learners;
  }

  std::vector<uint64_t> nodes;  // 集群中的节点
  std::vector<uint64_t> learners;  // 集群中的 learner
  MSGPACK_DEFINE (nodes, learners);
};
typedef std::shared_ptr<ConfState> ConfStatePtr;

struct SnapshotMetadata {
  SnapshotMetadata()
      : index(0),
        term(0) {
  }

  bool operator==(const SnapshotMetadata& meta) const {
    return conf_state == meta.conf_state && index == meta.index && term == meta.term;
  }

  ConfState conf_state;  //  配置信息
  uint64_t index;  // 保存的日志条目起始 index
  uint64_t term;  // 保存的日志条目起始 term
  MSGPACK_DEFINE (conf_state, index, term);
};

struct Snapshot {
  Snapshot() = default;

  explicit Snapshot(const std::vector<uint8_t>& data)
      : data(data) {
  }

  bool equal(const Snapshot& snap) const;

  bool is_empty() const {
    return metadata.index == 0;
  }
  std::vector<uint8_t> data;
  SnapshotMetadata metadata;
  MSGPACK_DEFINE (data, metadata);
};
typedef std::shared_ptr<Snapshot> SnapshotPtr;

struct Message {
  Message()
      : type(MsgHup),
        to(0),  // 消息来自哪个节点
        from(0),  // 消息去往哪个节点
        term(0),  // from 节点当前的任期
        log_term(0),  // leader发送 MsgApp时会带上这笔日志的前一笔日志的term 和 index，为的是让 follower 检查自己最后一条日志是否和 leader 发过来的 log_term 和 index一致
        index(0),  // 同上
        commit(0),  // from 中已提交日志的索引，也就是说 leader 与 follower 通信的时候会每次都把自己已提交日志的索引发送过去
        reject(false),  // follower 是否拒绝 leader 的日志同步请求 或者 表示该节点对 candidate 的投票结果
        reject_hint(0) {  

  }

  bool operator==(const Message& msg) const {
    return type == msg.type && to == msg.to && from == msg.from && term == msg.term
        && log_term == msg.log_term && index == msg.index
        && entries == msg.entries && commit == msg.commit
        && snapshot.equal(msg.snapshot) && reject == msg.reject
        && reject_hint == msg.reject_hint && context == msg.context;
  }

  bool is_local_msg() const;

  bool is_response_msg() const;

  MessageType type;
  uint64_t to;
  uint64_t from;
  uint64_t term;
  uint64_t log_term;
  uint64_t index;  // 具体含义与消息有关，如果是 MsgApp 的话，index 保存的leader 发送请求同步日志的前一条日志的 index；如果是 MsgAppResp，那么 index 保存的是提示 leader 节点我已经在这个位置上同步了你发给我的预写日志
  std::vector<Entry> entries;
  uint64_t commit;
  Snapshot snapshot;
  bool reject;
  uint64_t reject_hint; // follower 拒绝 leader 的日志同步请求时，会把自己的最后一条日志的term和index发送给 leader，leader 会根据这个索引来决定下一次同步的起始位置
  std::vector<uint8_t> context;
  MSGPACK_DEFINE (type, to, from, term, log_term, index, entries, commit, snapshot, reject, reject_hint, context);
};
typedef std::shared_ptr<Message> MessagePtr;

struct HardState {
  HardState()
      : term(0),
        vote(0),
        commit(0) {
  }

  bool is_empty_state() const {
    return term == 0 && vote == 0 && commit == 0;
  }

  bool equal(const HardState& hs) const {
    return term == hs.term && vote == hs.vote && commit == hs.commit;
  }

  uint64_t term;  // 当前任期
  uint64_t vote;  // 投票给谁
  uint64_t commit;  // commit index
  MSGPACK_DEFINE (term, vote, commit);
};

const uint8_t ConfChangeAddNode = 0;
const uint8_t ConfChangeRemoveNode = 1;
const uint8_t ConfChangeUpdateNode = 2;
const uint8_t ConfChangeAddLearnerNode = 3;

struct ConfChange {
  static void from_data(const std::vector<uint8_t>& data, ConfChange& cc);
  uint64_t id;
  uint8_t conf_change_type;
  uint64_t node_id;  // 节点的 id
  std::vector<uint8_t> context;
  MSGPACK_DEFINE (id, conf_change_type, node_id, context);
  std::vector<uint8_t> serialize() const;
};
typedef std::shared_ptr<ConfChange> ConfChangePtr;

}
}