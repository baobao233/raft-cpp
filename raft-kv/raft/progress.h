#pragma once
#include <memory>
#include <vector>

namespace kv {

enum ProgressState {
  ProgressStateProbe = 0,
  ProgressStateReplicate = 1,
  ProgressStateSnapshot = 2
};

const char* progress_state_to_string(ProgressState state);

class InFlights {
 public:
  explicit InFlights(uint64_t max_inflight_msgs)
      : start(0),
        count(0),
        size(static_cast<uint32_t>(max_inflight_msgs)) {}

  // 重置滑动窗口
  void reset() {
    start = 0;
    count = 0;
  }

  // message 的数量和 buffer 的大小相等的时候
  bool is_full() const {
    return size == count;
  }

  void add(uint64_t inflight);

  // freeTo frees the inflights smaller or equal to the given `to` flight.
  void free_to(uint64_t to);

  void free_first_one();

  // the starting index in the buffer
  uint32_t start;
  // number of inflights in the buffer
  uint32_t count;

  // the size of the buffer
  uint32_t size;

  // 环形缓冲区包含一条message中最后一个预写日志的索引。
  std::vector<uint64_t> buffer;
};

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.

// 一个 Progress 对象代表了一个 follower 的进度，即 leader 维护所有 follower 的进度，包括 match 和 next 等信息。
// 通过这两个信息，leader 可以知道哪些预写日志是已经被大多数节点所接受了，可以作为一个 commitIndex
class Progress {
 public:
  explicit Progress(uint64_t max_inflight)
      : match(0),  // 该节点已经同步日志的索引
        next(0),  // 下一个向节点同步时的日志索引
        state(ProgressState::ProgressStateProbe),
        paused(false),
        pending_snapshot(0),
        recent_active(false),
        inflights(new InFlights(max_inflight)),
        is_learner(false) {

  }

  void become_replicate();

  void become_probe();

  void become_snapshot(uint64_t snapshoti);

  void reset_state(ProgressState state);

  std::string string() const;

  bool is_paused() const;

  void set_pause() {
    this->paused = true;
  }

  void resume() {
    this->paused = false;
  }

  // maybe_update returns false if the given n index comes from an outdated message.
  // Otherwise it updates the progress and returns true.
  bool maybe_update(uint64_t n);

  void optimistic_update(uint64_t n) {
    next = n + 1;
  }

  // maybe_decr_to returns false if the given to index comes from an out of order message.
  // Otherwise it decreases the progress next index to min(rejected, last) and returns true.
  bool maybe_decreases_to(uint64_t rejected, uint64_t last);

  // need_snapshot_abort returns true if snapshot progress's match
  // is equal or higher than the pending_snapshot.
  bool need_snapshot_abort() const;

  // 重新把 pending_snapshot 置为 0
  void snapshot_failure() {
    pending_snapshot = 0;
  }

  uint64_t match; // 该节点已经同步日志的索引
  uint64_t next;  // 下一个向节点同步时的日志索引
  // state defines how the leader should interact with the follower.
  //
  // When in ProgressStateProbe, leader sends at most one replication message
  // per heartbeat interval. It also probes actual progress of the follower.
  //
  // When in ProgressStateReplicate, leader optimistically increases next
  // to the latest entry sent after sending replication message. This is
  // an optimized state for fast replicating log entries to the follower.
  //
  // When in ProgressStateSnapshot, leader should have sent out snapshot
  // before and stops sending any replication message.
  ProgressState state;

  // paused is used in ProgressStateProbe.
  // When Paused is true, raft should pause sending replication message to this peer.
  bool paused;
  // pending_snapshot is used in ProgressStateSnapshot.
  // If there is a pending snapshot, the pendingSnapshot will be set to the
  // index of the snapshot. If pendingSnapshot is set, the replication process of
  // this Progress will be paused. raft will not resend snapshot until the pending one
  // is reported to be failed.
  uint64_t pending_snapshot;

  // 如果progress最近处于活动状态，则 recent_active 为 true。如果收到
  // 来自相应follower的任何消息，则表明进度处于活动状态。在选举超时后，recent_active 可以重置为 false。
  bool recent_active;


  // inflights is a sliding window for the inflight messages.
  // Each inflight message contains one or more log entries.
  // The max number of entries per message is defined in raft config as MaxSizePerMsg.
  // Thus inflight effectively limits both the number of inflight messages
  // and the bandwidth each Progress can use.
  // When inflights is full, no more message should be sent.
  // When a leader sends out a message, the index of the last
  // entry should be added to inflights. The index MUST be added
  // into inflights in order.
  // When a leader receives a reply, the previous inflights should
  // be freed by calling inflights.freeTo with the index of the last
  // received entry.

  std::shared_ptr<InFlights> inflights;  // 接收信息的滑动窗口

  // is_learner is true if this progress is tracked for a learner.
  bool is_learner;

};
typedef std::shared_ptr<Progress> ProgressPtr;

}