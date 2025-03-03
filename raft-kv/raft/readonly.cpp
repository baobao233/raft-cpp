#include <raft-kv/raft/readonly.h>
#include <raft-kv/common/log.h>

namespace kv {

//  从读请求队列中返回最后一个 ctx
void ReadOnly::last_pending_request_ctx(std::vector<uint8_t>& ctx) {
  if (read_index_queue.empty()) {
    return;
  }

  ctx.insert(ctx.end(), read_index_queue.back().begin(), read_index_queue.back().end());
}

// 返回自证身份时候收集到的 ack 数量
uint32_t ReadOnly::recv_ack(const proto::Message& msg) {
  std::string str(msg.context.begin(), msg.context.end());

  auto it = pending_read_index.find(str);
  if (it == pending_read_index.end()) {
    return 0;
  }

  it->second->acks.insert(msg.from);  // 自证身份时候收集回复的 ack
  // add one to include an ack from local node
  return it->second->acks.size() + 1;
}

// 通过调用 advance 将消息中需要处理的读请求返回，并且将队列和 map 中的相应读请求给清除表明已处理
std::vector<ReadIndexStatusPtr> ReadOnly::advance(const proto::Message& msg) {
  std::vector<ReadIndexStatusPtr> rss;
  // 取出 msg 中的 ctx
  std::string ctx(msg.context.begin(), msg.context.end());
  bool found = false;
  uint32_t i = 0;
  // 遍历读请求队列中的读请求
  for (std::string& okctx: read_index_queue) {
    i++;
    auto it = pending_read_index.find(okctx);
    if (it == pending_read_index.end()) {
      LOG_FATAL("cannot find corresponding read state from pending map");
    }
    // 将队列中该msg读请求之前的读请求全都 push 到 rss 中
    rss.push_back(it->second);
    if (okctx == ctx) {
      found = true;
      break;
    }

  }

  if (found) {
    // 去除队列和 map 中的读请求
    read_index_queue.erase(read_index_queue.begin(), read_index_queue.begin() + i);
    for (ReadIndexStatusPtr& rs : rss) {
      std::string str(rs->req.entries[0].data.begin(), rs->req.entries[0].data.end());
      pending_read_index.erase(str);
    }
  }
  return rss;
}

// 为当前读请求创建一个 ctx，并且为当前读请求创建一个 status，并且用 unordermap 记录下来
void ReadOnly::add_request(uint64_t  committed_index, proto::MessagePtr msg) {
  std::string ctx(msg->entries[0].data.begin(), msg->entries[0].data.end());
  auto it = pending_read_index.find(ctx);
  if (it != pending_read_index.end()) {
    return;
  }
  ReadIndexStatusPtr status(new ReadIndexStatus());
  status->index = committed_index; // 把每个读请求来的时候leader自身的 commit Index 保存在status中
  status->req = *msg;
  pending_read_index[ctx] = status;  // 记录当前读请求的 status
  read_index_queue.push_back(ctx);  // 将读请求的 ctx push 到队列中
}

}
