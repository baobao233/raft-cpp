#pragma once
#include <raft-kv/raft/storage.h>
#include <raft-kv/raft/unstable.h>

namespace kv {

// 对于预写日志的管理
class RaftLog {
 public:
  explicit RaftLog(
    StoragePtr storage,  
    uint64_t max_next_ents_size，
    );

  ~RaftLog();

  static uint64_t unlimited() {
    return std::numeric_limits<uint64_t>::max();
  }

  std::string status_string() const {
    char buffer[64];
    int n = snprintf(buffer,
                     sizeof(buffer),
                     "committed=%lu, applied=%lu, unstable.offset=%lu, unstable.entries=%lu",
                     committed_,
                     applied_,
                     unstable_->offset_,
                     unstable_->entries_.size());
    return std::string(buffer, n);
  }

  // maybe_append returns (0, false) if the entries cannot be appended. O therwise,
  // it returns (last index of new entries, true).
  void maybe_append(uint64_t index,
                    uint64_t log_term,
                    uint64_t committed,
                    std::vector<proto::EntryPtr> entries,
                    uint64_t& last_new_index,
                    bool& ok);

  // return last index
  uint64_t append(std::vector<proto::EntryPtr> entries);

  // find_conflict finds the index of the conflict.
  // It returns the first pair of conflicting entries between the existing
  // entries and the given entries, if there are any.
  // If there is no conflicting entries, and the existing entries contains
  // all the given entries, zero will be returned.
  // If there is no conflicting entries, but the given entries contains new
  // entries, the index of the first new entry will be returned.
  // An entry is considered to be conflicting if it has the same index but
  // a different term.
  // The first entry MUST have an index equal to the argument 'from'.
  // The index of the given entries MUST be continuously increasing.
  uint64_t find_conflict(const std::vector<proto::EntryPtr>& entries);

  // next_entries returns all the available entries for execution.
  // If applied is smaller than the index of snapshot, it returns all committed
  // entries after the index of snapshot.
  void next_entries(std::vector<proto::EntryPtr>& entries) const;

  // has_next_entries returns if there is any available entries for execution. This
  // is a fast check without heavy slice in next_entries.
  bool has_next_entries() const;

  // slice returns a slice of log entries from low through high-1, inclusive.
  Status slice(uint64_t low, uint64_t high, uint64_t max_size, std::vector<proto::EntryPtr>& entries) const;

  // is_up_to_date determines if the given (lastIndex,term) log is more up-to-date
  // by comparing the index and term of the last entries in the existing logs.
  // If the logs have last entries with different terms, then the log with the
  // later term is more up-to-date. If the logs end with the same term, then
  // whichever log has the larger lastIndex is more up-to-date. If the logs are
  // the same, the given log is up-to-date.

  // 如果候选人的最后一条日志的任期大于当前节点的任期 || 候选人的最后一条日志的索引大于等于当前节点的最后一条日志的索引，则说明候选人的日志是最新的
  bool is_up_to_date(uint64_t lasti, uint64_t term) const {
    uint64_t lt = last_term();
    return term > lt || (term == lt && lasti >= last_index());
  }

  std::vector<proto::EntryPtr>& unstable_entries() {
    return unstable_->entries_;
  }

  bool maybe_commit(uint64_t max_index, uint64_t term);

  void restore(proto::SnapshotPtr snapshot);

  Status snapshot(proto::SnapshotPtr& snap) const;

  void applied_to(uint64_t index);

  void stable_to(uint64_t index, uint64_t term) {
    unstable_->stable_to(index, term);
  }

  void stable_snap_to(uint64_t index) {
    unstable_->stable_snap_to(index);
  }

  Status entries(uint64_t index, uint64_t max_size, std::vector<proto::EntryPtr>& entries) const {
    if (index > last_index()) {
      return Status::ok();
    }
    return slice(index, last_index() + 1, max_size, entries); // 返回从 index 到 last_index() + 1 的 entries， 如果日志已经被压缩则返回!ok
  }

  void commit_to(uint64_t to_commit);

  bool match_term(uint64_t index, uint64_t t);

  uint64_t last_term() const;

  Status term(uint64_t index, uint64_t& t) const;

  uint64_t first_index() const;

  uint64_t last_index() const;

  Status must_check_out_of_bounds(uint64_t low, uint64_t high) const;

  void all_entries(std::vector<proto::EntryPtr>& entries);

 public:
   // storage contains all stable entries since the last snapshot.
   // 已经持久化的预写日志模块，但仅仅是query预写日志，因为预写日志的变化不应该由算法层来实现，因此仅仅是提供了一个 storage 的指针
   StoragePtr storage_;

   // unstable contains all unstable entries and snapshot.
   // they will be saved into storage.
   // 还未持久化在 storage 中的预写日志位置，该位置之后的预写日志还在内存中
   UnstablePtr unstable_;  

   // committed is the highest log position that is known to be in
   // stable storage on a quorum of nodes.
   // 记录了已经提交的预写日志的位置 commitIndex
   uint64_t committed_;
   // applied is the highest log position that the application has
   // been instructed to apply to its state machine.
   // Invariant: applied <= committed
   // 记录了已经应用在状态机上的预写日志的位置 applyIndex，applied <= committed是因为一条日志首先要提交成功才能够应用在状态机上
   uint64_t applied_;

   // max_next_ents_size is the maximum number aggregate byte size of the messages
   // returned from calls to nextEnts.
   uint64_t max_next_ents_size_;
};
typedef std::shared_ptr<RaftLog> RaftLogPtr;

}
