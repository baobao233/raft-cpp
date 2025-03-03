#include <raft-kv/raft/storage.h>
#include <raft-kv/common/log.h>
#include <raft-kv/raft/util.h>

namespace kv {

Status MemoryStorage::initial_state(proto::HardState& hard_state, proto::ConfState& conf_state) {
  hard_state = hard_state_;

  // copy
  conf_state = snapshot_->metadata.conf_state;
  return Status::ok();
}

void MemoryStorage::set_hard_state(proto::HardState& hard_state) {
  std::lock_guard<std::mutex> guard(mutex_);
  hard_state_ = hard_state;
}

Status MemoryStorage::entries(uint64_t low,
                              uint64_t high,
                              uint64_t max_size,
                              std::vector<proto::EntryPtr>& entries) {
  assert(low < high);
  std::lock_guard<std::mutex> guard(mutex_);

  uint64_t offset = entries_[0]->index;
  if (low <= offset) {  // storage 中第一个 entry 的 offset 已经大于 low，说明offset 之前的 entry 已经被压缩，则返回不 ok
    return Status::invalid_argument("requested index is unavailable due to compaction");
  }
  uint64_t last = 0;
  this->last_index_impl(last);

  if (high > last + 1) {
    LOG_FATAL("entries' hi(%lu) is out of bound last_index(%lu)", high, last);
  }
  // only contains dummy entries.
  if (entries_.size() == 1) {
    return Status::invalid_argument("requested entry at index is unavailable");
  }

  for (uint64_t i = low - offset; i < high - offset; ++i) {
    entries.push_back(entries_[i]);
  }
  entry_limit_size(max_size, entries);  // 限制 entry 的大小
  return Status::ok();
}

Status MemoryStorage::term(uint64_t i, uint64_t& term) {
  std::lock_guard<std::mutex> guard(mutex_);  // 使用互斥锁

  uint64_t offset = entries_[0]->index;  // 找到已经持久化日志中的第一笔日志的 index，  称为offset

  if (i < offset) { // 如果该 index 小于 offset 说明，index 所在且之前的日志都已经被压缩了
    return Status::invalid_argument("requested index is unavailable due to compaction");
  }

  if (i - offset >= entries_.size()) {
    return Status::invalid_argument("requested entry at index is unavailable");
  }
  term = entries_[i - offset]->term;  // 从持久层找到 term 并且返回
  return Status::ok();
}

Status MemoryStorage::last_index(uint64_t& index) {
  std::lock_guard<std::mutex> guard(mutex_);
  return last_index_impl(index);
}

Status MemoryStorage::first_index(uint64_t& index) {
  std::lock_guard<std::mutex> guard(mutex_);
  return first_index_impl(index);
}

Status MemoryStorage::snapshot(proto::SnapshotPtr& snapshot) {
  std::lock_guard<std::mutex> guard(mutex_);
  snapshot = snapshot_;
  return Status::ok();
}

// 将 compact index 之前的 entry 都压缩（此处是直接抹去，TODO：真正压缩）
Status MemoryStorage::compact(uint64_t compact_index) {
  std::lock_guard<std::mutex> guard(mutex_);

  uint64_t offset = entries_[0]->index;

  if (compact_index <= offset) {
    return Status::invalid_argument("requested index is unavailable due to compaction");
  }

  uint64_t last_idx;
  this->last_index_impl(last_idx);
  if (compact_index > last_idx) {
    LOG_FATAL("compact %lu is out of bound lastindex(%lu)", compact_index, last_idx);
  }

  uint64_t i = compact_index - offset;
  entries_[0]->index = entries_[i]->index;
  entries_[0]->term = entries_[i]->term;

  entries_.erase(entries_.begin() + 1, entries_.begin() + i + 1);
  return Status::ok();
}

Status MemoryStorage::append(std::vector<proto::EntryPtr> entries) {
  if (entries.empty()) {
    return Status::ok();
  }

  std::lock_guard<std::mutex> guard(mutex_);

  uint64_t first = 0;
  first_index_impl(first);
  uint64_t last = entries[0]->index + entries.size() - 1;

  // shortcut if there is no new entry.
  if (last < first) {
    return Status::ok();
  }

  // truncate compacted entries
  if (first > entries[0]->index) {
    uint64_t n = first - entries[0]->index;
    // first 之前的 entry 已经进入 snapshot, 丢弃
    entries.erase(entries.begin(), entries.begin() + n);
  }

  uint64_t offset = entries[0]->index - entries_[0]->index;  // 计算新的第一条 entry 在 entries_中的位置

  if (entries_.size() > offset) { // 如果这个位置小于 entries_的大小，说明新的 entries 和旧的 entries_之间有重叠
    //MemoryStorage [first, offset] 被保留, offset 之后的丢弃
    entries_.erase(entries_.begin() + offset, entries_.end());  // 去除 offset 之后的 entries_
    entries_.insert(entries_.end(), entries.begin(), entries.end());  // 拼入新的 entry
  } else if (entries_.size() == offset) {  // 说明没有重叠
    entries_.insert(entries_.end(), entries.begin(), entries.end());   // 直接拼入新的 entry
  } else {  // 小于新的 entry 说明之间会有缺失，保证不了一致性，fatal
    uint64_t last_idx;
    last_index_impl(last_idx);
    LOG_FATAL("missing log entry [last: %lu, append at: %lu", last_idx, entries[0]->index);
  }
  return Status::ok();
}

// 根据传入的index 创建快照，并且传回SnapshotPtr
Status MemoryStorage::create_snapshot(uint64_t index,  // storage_会传入 applied 的 index
                                      proto::ConfStatePtr cs,  // 配置状态
                                      std::vector<uint8_t> data,  // 快照传入的数据
                                      proto::SnapshotPtr& snapshot) {  // 待生成的快照
  std::lock_guard<std::mutex> guard(mutex_);

  if (index <= snapshot_->metadata.index) {  // 创建快照的index <  已经存在的 snapshot
    snapshot = std::make_shared<proto::Snapshot>();
    return Status::invalid_argument("requested index is older than the existing snapshot");
  }

  uint64_t offset = entries_[0]->index;
  uint64_t last = 0;
  last_index_impl(last);
  if (index > last) {  // 创建快照的index > 已经吃持久化日志中的最大 index
    LOG_FATAL("snapshot %lu is out of bound lastindex(%lu)", index, last);
  }

  // 创建快照
  snapshot_->metadata.index = index;
  snapshot_->metadata.term = entries_[index - offset]->term;
  if (cs) {
    snapshot_->metadata.conf_state = *cs;
  }
  snapshot_->data = std::move(data);
  snapshot = snapshot_;
  return Status::ok();

}

Status MemoryStorage::apply_snapshot(const proto::Snapshot& snapshot) {
  std::lock_guard<std::mutex> guard(mutex_);

  uint64_t index = snapshot_->metadata.index;
  uint64_t snap_index = snapshot.metadata.index;

  if (index >= snap_index) {  // 现有的 snapshot 快照 index 大于 将要应用的 snapshot 的 index
    return Status::invalid_argument("requested index is older than the existing snapshot");
  }

  snapshot_ = std::make_shared<proto::Snapshot>(snapshot);

  // 重新把 entries 清空，根据 snapshot 重新导入 entry
  entries_.resize(1);
  proto::EntryPtr entry(new proto::Entry());
  entry->term = snapshot_->metadata.term;
  entry->index = snapshot_->metadata.index;
  entries_[0] = std::move(entry);
  return Status::ok();
}

Status MemoryStorage::last_index_impl(uint64_t& index) {
  index = entries_[0]->index + entries_.size() - 1;
  return Status::ok();
}

Status MemoryStorage::first_index_impl(uint64_t& index) {
  index = entries_[0]->index + 1;
  return Status::ok();
}

}