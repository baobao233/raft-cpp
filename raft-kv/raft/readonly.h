#pragma once
#include <vector>
#include <raft-kv/raft/proto.h>
#include <raft-kv/raft/config.h>

namespace kv {

// ReadState provides state for read only query.
// It's caller's responsibility to call read_index first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// request_ctx
struct ReadState {
  bool equal(const ReadState& rs) const {
    if (index != rs.index) {
      return false;
    }
    return request_ctx == rs.request_ctx;
  }
  uint64_t index;
  std::vector<uint8_t> request_ctx;
};

struct ReadIndexStatus {
  proto::Message req;  // 封装读请求的请求消息体
  uint64_t index;  // 接收到了读请求时当前leader 已经提交的索引

  // leader 在回复读请求时候需要有一个“自证身份”的行为，由于网络分区的原因，此时 leader 可能是滞后的，
  // 新 leader 已经上位而自己不自知，因此在回应 readIndex 之前需要进行一轮广播，统计leader收到的 ack 数量，判断自己有没有
  // 集齐了多数派的响应，如果有，就可以正常回复
  std::unordered_set<uint64_t> acks;  

};
typedef std::shared_ptr<ReadIndexStatus> ReadIndexStatusPtr;

// 宏观处理读请求的模块，在这个模块中可能会同时有多个读请求的到来，因此需要一个队列来存储这些读请求
struct ReadOnly {
  explicit ReadOnly(ReadOnlyOption option)
      : option(option) {}

  // last_pending_request_ctx returns the context of the last pending read only
  // request in readonly struct.
  void last_pending_request_ctx(std::vector<uint8_t>& ctx);

  uint32_t recv_ack(const proto::Message& msg);

  std::vector<ReadIndexStatusPtr> advance(const proto::Message& msg);

  void add_request(uint64_t index, proto::MessagePtr msg);

  ReadOnlyOption option;
  std::unordered_map<std::string, ReadIndexStatusPtr> pending_read_index; // 通过 unordered_map 维护每个读请求当前处理的一个进度
  std::vector<std::string> read_index_queue; // 多个读请求的到来的时候通过该队列来维护这些读请求
};
typedef std::shared_ptr<ReadOnly> ReadOnlyPtr;

}
