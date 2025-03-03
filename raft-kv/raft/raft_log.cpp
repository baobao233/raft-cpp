#include <raft-kv/raft/raft_log.h>
#include <raft-kv/common/log.h>
#include <raft-kv/raft/util.h>

namespace kv {

RaftLog::RaftLog(StoragePtr storage, uint64_t max_next_ents_size)
    : storage_(std::move(storage)),
      committed_(0),
      applied_(0),
      max_next_ents_size_(max_next_ents_size) {
  assert(storage_);
  uint64_t first;
  auto status = storage_->first_index(first);
  assert(status.is_ok());

  uint64_t last;
  status = storage_->last_index(last);
  assert(status.is_ok());

  unstable_ = std::make_shared<Unstable>(last + 1);

  // Initialize our committed and applied pointers to the time of the last compaction.
  applied_ = committed_ = first - 1;
}
RaftLog::~RaftLog() {

}

// 根据前一笔日志的 index，term 等信息，判断能不能追加日志，如果能，还需要找到冲突的日志，处理冲突的日志，并且 append 到 unstable 中，然后更新 commit 的信息等
void RaftLog::maybe_append(uint64_t index,    // 待同步日志的前一笔预写日志的 index
                           uint64_t log_term, // 待同步日志的前一笔预写日志的 term
                           uint64_t committed,  // leader 节点的 committed index
                           std::vector<proto::EntryPtr> entries,
                           uint64_t &last_new_index,
                           bool &ok)
{
  if (match_term(index, log_term)) { // 如果待同步日志的前一笔预写日志的 term 和 index 匹配
    uint64_t lastnewi = index + entries.size();  // 计算待同步日志的最后一笔预写日志的 index
    uint64_t ci = find_conflict(entries);  // 找到冲突的 index，最后一条冲突 index
    if (ci == 0) {
      // 没有冲突，忽略
    } else if (ci <= committed_) {
      // 如果冲突的索引是已经提交过的预写日志索引，我作为一个 follower 是不能回滚已提交日志的，则报错
      LOG_FATAL("entry %lu conflict with committed entry [committed(%lu)]", ci, committed_);
    } else {
      // 有冲突且不报错的默认情况下走到以下分支
      assert(ci > 0);
      uint64_t offset = index + 1;  // 发送过来的起始index
      uint64_t n = ci - offset;  // 冲突日志的数量
      entries.erase(entries.begin(), entries.begin() + n);  // 舍弃掉待同步日志中的冲突部分日志
      append(std::move(entries));  // 从查找到的数据索引开始，将这之后的数据放到 unstable 存储中
    }

    // 更新自身的 commit index，并且在 commit_to 函数中校验该值是否合法，如果大于自身节点最后一笔日志的索引则报错
    commit_to(std::min(committed, lastnewi));  
    

    last_new_index = lastnewi;
    ok = true;
    return;
  } else {  // 如果不匹配，则返回 ok 为 false表示我们之间的 log 不匹配
    last_new_index = 0;  
    ok = false;
  }
}

// 通过调用 unstable 中的 truncate_and_append，比对 unstable 中第一条 entry 的 index 和打算 append 进来的 entry 的第一条 entry 大小关系，再最终调整决定 unstable 中的 entry
uint64_t RaftLog::append(std::vector<proto::EntryPtr> entries) {
  if (entries.empty()) {
    return last_index();
  }

  uint64_t after = entries[0]->index - 1;
  if (after < committed_) {  // 第一个日志的 index 不能小于已经 commit 的 index
    LOG_FATAL("after(%lu) is out of range [committed(%lu)]\", after, committed_", after, committed_);
  }

  unstable_->truncate_and_append(std::move(entries));
  return last_index();
}

// 找到日志中冲突日志的最后的 index
uint64_t RaftLog::find_conflict(const std::vector<proto::EntryPtr>& entries) {
  for (const proto::EntryPtr& entry : entries) {
    if (!match_term(entry->index, entry->term)) {  // match 就表示和 storage 中的冲突了，不 match 就表示找到了第一条不冲突日志
      if (entry->index < last_index()) {
        uint64_t t;
        Status status = this->term(entry->index, t);
        LOG_INFO("found conflict at index %lu [existing term: %lu, conflicting term: %lu], %s",
                 entry->index,
                 t,
                 entry->term,
                 status.to_string().c_str());
      }
      return entry->index;
    }
  }
  return 0;
}

// 查找 apply 到 commit 之间的entry
void RaftLog::next_entries(std::vector<proto::EntryPtr>& entries) const {
  uint64_t off = std::max(applied_ + 1, first_index());
  if (committed_ + 1 > off) { // 如果 committed_ + 1 大于 off，则表示有未应用的日志
    Status status = slice(off, committed_ + 1, max_next_ents_size_, entries);
    if (!status.is_ok()) {
      LOG_FATAL("unexpected error when getting unapplied entries");
    }
  }
}

// 检查是否存在尚未apply的已提交日志条目。
bool RaftLog::has_next_entries() const {
  uint64_t off = std::max(applied_ + 1, first_index()); // 下一个需要apply 的 index， 这一步确保 off 不小于日志的第一个有效索引，防止访问无效的日志条目。
  return committed_ + 1 > off;
}

// 校验 commitIndex 和 term 去更新 commit
bool RaftLog::maybe_commit(uint64_t max_index, uint64_t term) {
  if (max_index > committed_) {
    uint64_t t;
    this->term(max_index, t);  // 通过 unstable 和 storage 校对该日志的 term 是否与我 leader 当前的一致，一致则可提交
    if (t == term) {
      commit_to(max_index);
      return true;
    }
  }
  return false;
}

// 通过快照恢复
void RaftLog::restore(proto::SnapshotPtr snapshot) {
  LOG_INFO("log starts to restore snapshot [index: %lu, term: %lu]",
           snapshot->metadata.index,
           snapshot->metadata.term);
  committed_ = snapshot->metadata.index;
  unstable_->restore(std::move(snapshot));
}

// 将快照传入 snap 中
Status RaftLog::snapshot(proto::SnapshotPtr& snap) const {
  if (unstable_->snapshot_) {  // 如果已经有传入的快照，则直接返回该快照
    snap = unstable_->snapshot_;
    return Status::ok();
  }

  proto::SnapshotPtr s;
  Status status = storage_->snapshot(s);  // 从应用层中的持久模块获得快照，至于怎么去创建快照是应用层中的持久模块应该负责的
  if (s) {
    snap = s;
  }
  return status;
}

// 更新应用到的 index
void RaftLog::applied_to(uint64_t index) {
  if (index == 0) {
    return;
  }
  if (committed_ < index || index < applied_) {
    LOG_ERROR("applied(%lu) is out of range [prevApplied(%lu), committed(%lu)]", index, applied_, committed_);
  }
  applied_ = index;
}

// 从 unstable 和 storage 中返回 low到 high 的entry
Status RaftLog::slice(uint64_t low, uint64_t high, uint64_t max_size, std::vector<proto::EntryPtr>& entries) const {
  Status status = must_check_out_of_bounds(low, high);
  if (!status.is_ok()) {
    return status;
  }
  if (low == high) {
    return Status::ok();
  }

  // 分段查找，在 unstable 和 storage 中都查找 entries，然后合并成完整的 slice entires
  //slice from storage_
  if (low < unstable_->offset_) {  // 如果 unstable 中的第一个entry 的 offset 已经大于起始的low index，则从 storage 中查找从 low 到min(high,offset_)的 entries
    status = storage_->entries(low, std::min(high, unstable_->offset_), max_size, entries);  // 当 storage 找不到 low 的时候说明，low 已经被压缩，返回!ok
    if (!status.is_ok()) {
      return status;
    }

    // check if ents has reached the size limitation
    if (entries.size() < std::min(high, unstable_->offset_) - low) {
      return Status::ok();
    }

  }

  //slice unstable
  if (high > unstable_->offset_) {  // 如果 offset_ < high ，则可以在 unstable 中查找 max(low, offset_) 到 high 的 entries
    std::vector<proto::EntryPtr> unstable;
    unstable_->slice(std::max(low, unstable_->offset_), high, entries);
    entries.insert(entries.end(), unstable.begin(), unstable.end());
  }
  entry_limit_size(max_size, entries);
  return Status::ok();
}

// 更新该节点的 commit Index
void RaftLog::commit_to(uint64_t to_commit) {
  if (committed_ < to_commit) {
    if (last_index() < to_commit) { // 如果将要 commit 的 index 大于当前节点的最后一笔预写日志的 index，则报错
      LOG_FATAL("to_commit(%lu) is out of range [lastIndex(%lu)]. Was the raft log corrupted, truncated, or lost?",
                to_commit,
                last_index());
    }
    committed_ = to_commit;  // 更新 commit Index
  } else {
    //ignore to_commit < committed_
  }
}

// 通过内存 unstable 或者持久化层 storage 中找到该日志的 term 为 term_out
bool RaftLog::match_term(uint64_t index, uint64_t t) {
  uint64_t term_out;
  Status status = this->term(index, term_out);
  if (!status.is_ok()) {
    return false;
  }
  return t == term_out;  // true: 找到冲突日志
}

// 最后一笔日志的 term
uint64_t RaftLog::last_term() const {
  uint64_t t;
  Status status = term(last_index(), t);
  assert(status.is_ok());
  return t;
}

// 在内存 unstable 或者持久化层 storage 中找该 index 对应的 term
Status RaftLog::term(uint64_t index, uint64_t& t) const {
  uint64_t dummy_index = first_index() - 1;
  if (index < dummy_index || index > last_index()) {  // 校验日志 index 是否合法
    // TODO: return an error instead?
    t = 0;
    return Status::ok();
  }

  uint64_t term_index;
  bool ok;

  unstable_->maybe_term(index, term_index, ok);  // 试图在内存中返回该 index 对应的 term
  if (ok) {
    t = term_index;
    return Status::ok();
  }

  Status status = storage_->term(index, term_index);  // 尝试在已经持久化的中找到该term
  if (status.is_ok()) {
    t = term_index;
  }
  return status;
}

uint64_t RaftLog::first_index() const {
  uint64_t index;
  bool ok;
  unstable_->maybe_first_index(index, ok);  // 如果存在快照，snapshot_非 0，从快照中获得当前的第一个索引
  if (ok) {
    return index;
  }

  Status status = storage_->first_index(index);  // 不存在快照则从持久化的日志中取得第一个索引
  assert(status.is_ok());

  return index;
}

uint64_t RaftLog::last_index() const {
  uint64_t index;
  bool ok;
  unstable_->maybe_last_index(index, ok);
  if (ok) {
    return index;
  }

  Status status = storage_->last_index(index);
  assert(status.is_ok());

  return index;
}

void RaftLog::all_entries(std::vector<proto::EntryPtr>& entries) {
  entries.clear();
  Status status = this->entries(first_index(), RaftLog::unlimited(), entries);
  if (status.is_ok()) {
    return;
  }

  // try again if there was a racing compaction
  if (status.to_string()
      == Status::invalid_argument("requested index is unavailable due to compaction").to_string()) {
    this->all_entries(entries);
  }
  LOG_FATAL("%s", status.to_string().c_str());
}

Status RaftLog::must_check_out_of_bounds(uint64_t low, uint64_t high) const {
  assert(high >= low);

  uint64_t first = first_index();

  if (low < first) {
    return Status::invalid_argument("requested index is unavailable due to compaction");
  }

  uint64_t length = last_index() + 1 - first;
  if (low < first || high > first + length) {
    LOG_FATAL("slice[%lu,%lu) out of bound [%lu,%lu]", low, high, first, last_index());
  }
  return Status::ok();

}

}

