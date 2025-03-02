#include <raft-kv/raft/progress.h>
#include <raft-kv/common/log.h>
#include <stdexcept>

namespace kv {

const char* progress_state_to_string(ProgressState state) {
  switch (state) {
    case ProgressStateProbe: {
      return "ProgressStateProbe";
    }
    case ProgressStateReplicate: {
      return "ProgressStateReplicate";
    }
    case ProgressStateSnapshot: {
      return "ProgressStateSnapshot";
    }
    default: {
      LOG_FATAL("unknown state %d", state);
    }
  }
}

// 尝试往接收窗口中增加msg，也就是增加一个msg 中带的 entry中的最后一个 index
void InFlights::add(uint64_t inflight) {
  if (is_full()) {
    LOG_FATAL("cannot add into a full inflights");
  }

  uint64_t next = start + count;

  if (next >= size) {
    next -= size;
  }
  if (next >= buffer.size()) {
    uint32_t new_size = buffer.size() * 2;  // 在 size 的限制内时buffer的大小指数增长，最终不能超过 size
    if (new_size == 0) {
      new_size = 1;
    } else if (new_size > size) {
      new_size = size;
    }
    buffer.resize(new_size);
  }
  buffer[next] = inflight;
  count++;
}

// 将某个 index 之前(包括 index)位置的 inflight 都给清空
void InFlights::free_to(uint64_t to) {
  if (count == 0 || to < buffer[start]) {
    // out of the left side of the window
    return;
  }

  uint32_t idx = start;
  size_t i;
  // 找到在 buffer 中大于 to（entry index) 的位置，那么就会有 i 个小于该 index 的缓冲区空间可以被释放
  for (i = 0; i < count; i++) {
    if (to < buffer[idx]) { 
      break;
    }

    // increase index and maybe rotate
    idx++;

    if (idx >= size) {
      idx -= size;
    }
  }
  // free i inflights and set new start index
  count -= i;
  start = idx;
  if (count == 0) {
    // inflights is empty, reset the start index so that we don't grow the
    // buffer unnecessarily.
    start = 0;
  }
}

// 清空第一个index
void InFlights::free_first_one() {
  free_to(buffer[start]);
}

// 重新回到正常复制状态
void Progress::become_replicate() {
  reset_state(ProgressStateReplicate);
  next = match + 1;  // 更新其 next 为 match 的下一个
}

// 变成探测状态， 1、当follower reject leader 的 MsgApp 时候；2、汇报 leader 某个 follower 快照状态是失败的时候；3、某个 follower 完成快照恢复的时候；4、leader 知道某个 follower MsgUnreachable的时候
void Progress::become_probe() {
  // If the original state is ProgressStateSnapshot, progress knows that
  // the pending snapshot has been sent to this peer successfully, then
  // probes from pendingSnapshot + 1.
  if (state == ProgressStateSnapshot) {  // 如果状态是ProgressStateSnapshot的时候表明该节点已经成功按照 snapshot 恢复，因此 next 更新的时候需要考虑 snapshot
    uint64_t pending = pending_snapshot;
    reset_state(ProgressStateProbe);
    next = std::max(match + 1, pending + 1);
  } else {
    reset_state(ProgressStateProbe);
    next = match + 1;
  }
}

// 接受一个 snapshot 中 entries 起始的 index
void Progress::become_snapshot(uint64_t snapshoti) {
  reset_state(ProgressStateSnapshot);
  pending_snapshot = snapshoti;
}

// 根据传来的状态重置状态
void Progress::reset_state(ProgressState st) {
  paused = false;
  pending_snapshot = 0;
  this->state = st;
  this->inflights->reset();  // 重置能接收信息的滑动窗口
}

std::string Progress::string() const {
  char buffer[256];
  int n = snprintf(buffer,
                   sizeof(buffer),
                   "next = %lu, match = %lu, state = %s, waiting = %d, pendingSnapshot = %lu",
                   next,
                   match,
                   progress_state_to_string(state),
                   is_paused(),
                   pending_snapshot);
  return std::string(buffer, n);
}

// 根据不同的状态判断其是否pause
bool Progress::is_paused() const {
  switch (state) {
    case ProgressStateProbe: {
      return paused;
    }
    case ProgressStateReplicate: {
      return inflights->is_full();
    }
    case ProgressStateSnapshot: {
      return true;
    }
    default: {
      LOG_FATAL("unexpected state");
    }
  }
}

// 根据节点发过来的消息中的index更新该节点 progress 的 match 进度
bool Progress::maybe_update(uint64_t n) {
  bool updated = false;
  if (match < n) {  // 直接更新其 match 进度
    match = n;
    updated = true;
    resume();
  }
  if (next < n + 1) {
    next = n + 1;
  }
  return updated;
}

// param: rejected:拒绝的日志条目    last:拒绝节点发来的该节点最后一条日志条目
bool Progress::maybe_decreases_to(uint64_t rejected, uint64_t last) {
  if (state == ProgressStateReplicate) {  // 如果是处于 leader复制其余节点 progress 信息的状态，也就是正常状态
    // the rejection must be stale if the progress has matched and "rejected"
    // is smaller than "match".
    if (rejected <= match) {  // 如果拒绝的预写日志索引是小于该节点已经同步到的日志索引，则返回错误
      return false;
    }
    // directly decrease next to match + 1
    next = match + 1;  // 更新leader维护该节点的信息中，下一个向该节点同步时的日志索引
    return true;
  }

  // the rejection must be stale if "rejected" does not match next - 1
  if (next - 1 != rejected) {  // progress 中的 next 前一个并不是 rejected 时，该拒绝请求是过时的
    return false;
  }

  // 尝试发送拒绝节点发来的最后一条日志后面的一条日志给该节点
  next = std::min(rejected, last + 1);
  if (next < 1) {
    next = 1;
  }
  resume();
  return true;
}

// 该节点的状态跟上快照给的预写日志 index 时返回 true，这时候中止快照
bool Progress::need_snapshot_abort() const {
  return state == ProgressStateSnapshot && match >= pending_snapshot;
}

}
