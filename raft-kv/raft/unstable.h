#pragma once
#include <raft-kv/raft/proto.h>

namespace kv {

// Unstable.entries[i] has raft log position i+unstable.offset.
// Note that unstable.offset may be less than the highest log
// position in storage; this means that the next write to storage
// might need to truncate the log before persisting unstable.entries.

// unstable存储的是还没有被持久化到 storage 的日志及快照，offset 代表了 entries 中第一条日志的 index
class Unstable {
 public:
  explicit Unstable(uint64_t offset)
      : offset_(offset) {

  }

  // maybe_first_index returns the index of the first possible entry in entries
  // if it has a snapshot.
  void maybe_first_index(uint64_t& index, bool& ok);

  // maybe_last_index returns the last index if it has at least one
  // unstable entry or snapshot.
  void maybe_last_index(uint64_t& index, bool& ok);

  // maybe_term returns the term of the entry at index i, if there
  // is any.
  void maybe_term(uint64_t index, uint64_t& term, bool& ok);

  void stable_to(uint64_t index, uint64_t term);

  void stable_snap_to(uint64_t index);

  void restore(proto::SnapshotPtr snapshot);

  void truncate_and_append(std::vector<proto::EntryPtr> entries);

  void slice(uint64_t low, uint64_t high, std::vector<proto::EntryPtr>& entries);
 public:
  // 非 0 的时候表示，已经有传入的快照
  proto::SnapshotPtr snapshot_;
  // all entries that have not yet been written to storage.
  std::vector<proto::EntryPtr> entries_;  // 还未被持久化进storage中的预写日志
  uint64_t offset_;  // 标识了entries_中的第一个entry的index
};
typedef std::shared_ptr<Unstable> UnstablePtr;

}
