#include <raft-kv/raft/unstable.h>
#include <raft-kv/common/log.h>

namespace kv {

void Unstable::maybe_first_index(uint64_t& index, bool& ok) {
  if (snapshot_) {
    ok = true;
    index = snapshot_->metadata.index + 1;
  } else {
    ok = false;
    index = 0;
  }
}

void Unstable::maybe_last_index(uint64_t& index, bool& ok) {
  if (!entries_.empty()) {  // entries 为空则意味着有可能是执行了快照恢复，清空了 entries
    ok = true;
    index = offset_ + entries_.size() - 1;
    return;
  }
  if (snapshot_) {  // 如果执行了快照恢复，那么 snapshot不为空，则返回 snapshot 中的 index
    ok = true;
    index = snapshot_->metadata.index;
    return;
  }
  index = 0;
  ok = false;
}

void Unstable::maybe_term(uint64_t index, uint64_t& term, bool& ok) {
  term = 0;
  ok = false;

  if (index < offset_) {
    if (!snapshot_) {  // 无 snapshot_
      return;
    }
    if (snapshot_->metadata.index == index) {  // 在 snapshot_ 上找到与之对应的term
      term = snapshot_->metadata.term;
      ok = true;
      return;
    }
    return;
  }

  // 走到这儿表明 index 在 unstable 的 entries_中
  uint64_t last = 0;
  bool last_ok = false;
  maybe_last_index(last, last_ok);
  if (!last_ok) {
    return;
  }
  if (index > last) {
    return;

  }
  ok = true;
  term = entries_[index - offset_]->term;
}

// 传入的 index 表明 entry 已经 stable 到这儿了，校对传入的 term 是否和 unstable 中的该日志的 term 一致，一致则删除此index 之前的 entries_
void Unstable::stable_to(uint64_t index, uint64_t term) {
  uint64_t gt = 0;
  bool ok = false;
  maybe_term(index, gt, ok);

  if (!ok) {
    return;
  }
  // if index < offset, term is matched with the snapshot
  // only update the unstable entries if term is matched with
  // an unstable entry.
  // 如果 index 小于 offset_表明 term 与 snapshot_的 term 相等
  if (gt == term && index >= offset_) {
    uint64_t n = index + 1 - offset_;  // 统计已经 stable 的 entries
    entries_.erase(entries_.begin(), entries_.begin() + n);  // 擦除已经 stable 的entries
    offset_ = index + 1; // 更新 offset_ 为 index + 1，表示这些条目已经稳定，不再是不稳定条目的一部分。
  }
}

// 如果传入的index 和 snapshot 中的 index 相等，表明 snapshot_已经持久化 
void Unstable::stable_snap_to(uint64_t index) {
  if (snapshot_ && snapshot_->metadata.index == index) {
    snapshot_ = nullptr;
  }
}

// 通过快照的信息更新 offset 和 index，并且清空 entries
void Unstable::restore(proto::SnapshotPtr snapshot) {
  offset_ = snapshot->metadata.index + 1;
  entries_.clear();
  snapshot_ = snapshot;  // 储存快照信息
}

void Unstable::truncate_and_append(std::vector<proto::EntryPtr> entries) {
  if (entries.empty()) {
    return;
  }
  uint64_t after = entries[0]->index;
  if (after == offset_ + entries_.size()) {  // 如果第一条 entry 的index刚好等于目前 unstable 末尾的位置
    // directly append
    entries_.insert(entries_.end(), entries.begin(), entries.end());
  } else if (after <= offset_) {  // 如果第一条entry的index是小于当前 offset_ 的就表明在 unstable 中保存的 entry 可能会有缺失，这部分缺失还没有被存储到 storage 中，所以直接覆盖掉 unstable 中的 entries
    // 日志被截断到after
    // 为什么会存在after <= offset_这种情况呢？  主要是由于日志覆盖的现象
    // 1、日志覆盖：在某些情况下，新的日志条目可能完全覆盖当前的不稳定日志条目。这可能发生在领导者重新选举或日志修复过程中，新的领导者可能会发送覆盖旧日志的条目。
    // 2、快照应用：如果节点应用了快照，offset_ 可能会更新为快照的最后一个条目索引。在这种情况下，新的日志条目可能会从快照之后开始。
    // 3、日志恢复：在网络分区或其他异常情况下，节点可能需要修复其日志以与集群中的其他节点保持一致。这可能导致新的日志条目覆盖旧的条目。
    LOG_INFO("replace the unstable entries from index %lu", after);
    offset_ = after;
    entries_ = std::move(entries);
  } else {  // after > offset_
    // truncate to after and copy entries_
    // then append
    LOG_INFO("truncate the unstable entries before index %lu", after);
    std::vector<proto::EntryPtr> entries_slice;
    this->slice(offset_, after, entries_slice); // 将 offset_ 到 after 部分的 entries 放入 entries_slice

    entries_slice.insert(entries_slice.end(), entries.begin(), entries.end()); // 再把 after 之后部分的 entries（即 entries）拼接到 entries_slice 中
    entries_ = std::move(entries_slice);  // 替换 unstable 中的 entries
  }
}

void Unstable::slice(uint64_t low, uint64_t high, std::vector<proto::EntryPtr>& entries) {
  assert(high > low);
  uint64_t upper = offset_ + entries_.size();
  if (low < offset_ || high > upper) {
    LOG_FATAL("unstable.slice[%lu,%lu) out of bound [%lu,%lu]", low, high, offset_, upper);
  }

  entries.insert(entries.end(), entries_.begin() + low - offset_, entries_.begin() + high - offset_);
}

}
