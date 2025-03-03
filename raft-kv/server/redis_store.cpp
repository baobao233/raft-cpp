#include <msgpack.hpp>
#include <raft-kv/server/redis_store.h>
#include <raft-kv/server/raft_node.h>
#include <raft-kv/common/log.h>
#include <raft-kv/server/redis_session.h>

namespace kv {

// see redis keys command  返回的key支持正则匹配
int string_match_len(const char* pattern, int patternLen,
                     const char* string, int stringLen, int nocase) {
  while (patternLen && stringLen) {
    switch (pattern[0]) {
      case '*':
        while (pattern[1] == '*') {
          pattern++;
          patternLen--;
        }
        if (patternLen == 1)
          return 1; /* match */
        while (stringLen) {
          if (string_match_len(pattern + 1, patternLen - 1,
                               string, stringLen, nocase))
            return 1; /* match */
          string++;
          stringLen--;
        }
        return 0; /* no match */
        break;
      case '?':
        if (stringLen == 0)
          return 0; /* no match */
        string++;
        stringLen--;
        break;
      case '[': {
        int not_match, match;

        pattern++;
        patternLen--;
        not_match = pattern[0] == '^';
        if (not_match) {
          pattern++;
          patternLen--;
        }
        match = 0;
        while (1) {
          if (pattern[0] == '\\' && patternLen >= 2) {
            pattern++;
            patternLen--;
            if (pattern[0] == string[0])
              match = 1;
          } else if (pattern[0] == ']') {
            break;
          } else if (patternLen == 0) {
            pattern--;
            patternLen++;
            break;
          } else if (pattern[1] == '-' && patternLen >= 3) {
            int start = pattern[0];
            int end = pattern[2];
            int c = string[0];
            if (start > end) {
              int t = start;
              start = end;
              end = t;
            }
            if (nocase) {
              start = tolower(start);
              end = tolower(end);
              c = tolower(c);
            }
            pattern += 2;
            patternLen -= 2;
            if (c >= start && c <= end)
              match = 1;
          } else {
            if (!nocase) {
              if (pattern[0] == string[0])
                match = 1;
            } else {
              if (tolower((int) pattern[0]) == tolower((int) string[0]))
                match = 1;
            }
          }
          pattern++;
          patternLen--;
        }
        if (not_match)
          match = !match;
        if (!match)
          return 0; /* no match */
        string++;
        stringLen--;
        break;
      }
      case '\\':
        if (patternLen >= 2) {
          pattern++;
          patternLen--;
        }
        /* fall through */
      default:
        if (!nocase) {
          if (pattern[0] != string[0])
            return 0; /* no match */
        } else {
          if (tolower((int) pattern[0]) != tolower((int) string[0]))
            return 0; /* no match */
        }
        string++;
        stringLen--;
        break;
    }
    pattern++;
    patternLen--;
    if (stringLen == 0) {
      while (*pattern == '*') {
        pattern++;
        patternLen--;
      }
      break;
    }
  }
  if (patternLen == 0 && stringLen == 0)
    return 1;
  return 0;
}

// 初始化
RedisStore::RedisStore(RaftNode* server, std::vector<uint8_t> snap, uint16_t port)
    : server_(server),
      acceptor_(io_service_),
      next_request_id_(0) {

  // 初始化如果传入快照，则根据快照把 kv 重置
  if (!snap.empty()) {
    std::unordered_map<std::string, std::string> kv;
    msgpack::object_handle oh = msgpack::unpack((const char*) snap.data(), snap.size());
    try {
      oh.get().convert(kv);
    } catch (std::exception& e) {
      LOG_WARN("invalid snapshot");
    }
    std::swap(kv, key_values_);
  }

  auto address = boost::asio::ip::address::from_string("0.0.0.0");
  auto endpoint = boost::asio::ip::tcp::endpoint(address, port);  // address:port

  // 绑定 socket
  acceptor_.open(endpoint.protocol());
  acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(1));
  acceptor_.bind(endpoint);
  acceptor_.listen();
}

// 析构函数
RedisStore::~RedisStore() {
  if (worker_.joinable()) {
    worker_.join();
  }
}

// 启动 redisStore，promise 返回启动 RedisStore 的线程 id
void RedisStore::start(std::promise<pthread_t>& promise) {
  start_accept();

  worker_ = std::thread([this, &promise]() {
    promise.set_value(pthread_self());
    this->io_service_.run();
  });
}

void RedisStore::start_accept() {
  RedisSessionPtr session(new RedisSession(this, io_service_));

  // 通过 RedisSession 接受请求
  acceptor_.async_accept(session->socket_, [this, session](const boost::system::error_code& error) {
    if (error) {
      LOG_DEBUG("accept error %s", error.message().c_str());
      return;
    }
    this->start_accept();
    session->start();
  });
}

// set 命令的具体操作
void RedisStore::set(std::string key, std::string value, const StatusCallback& callback) {
  uint32_t commit_id = next_request_id_++;  //  返回当前请求 id 并且增加请求的index

  RaftCommit commit;  // 提交给 raft 的结构体
  commit.node_id = static_cast<uint32_t>(server_->node_id());  // 提交 set 请求的节点 id
  commit.commit_id = commit_id;  // 请求id
  commit.redis_data.type = RedisCommitData::kCommitSet;  // commit 的命令设置为 set
  commit.redis_data.strs.push_back(std::move(key));  // push key
  commit.redis_data.strs.push_back(std::move(value));  // push data

  // 把 commit 压缩进缓冲区中
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, commit);
  // 把缓冲区中的数据存进 vector 中
  std::shared_ptr<std::vector<uint8_t>> data(new std::vector<uint8_t>(sbuf.data(), sbuf.data() + sbuf.size()));

  pending_requests_[commit_id] = callback;  // 注册对应请求的回调函数，set 命令完成后需要做的事情

  // RaftNode 推进 raft propose 一笔日志，raft 层处理后返回处理结果 status，根据 status 执行回调函数
  server_->propose(std::move(data), [this, commit_id](const Status& status) {
    io_service_.post([this, status, commit_id]() {
      if (status.is_ok()) {  // ok 则直接返回
        return;
      }

      // 不 ok 则找到对应的请求 id，执行RedisSession 传入的 callback 函数，并且擦除 RedisStore 中的请求记录
      auto it = pending_requests_.find(commit_id);
      if (it != pending_requests_.end()) {
        it->second(status);
        pending_requests_.erase(it);
      }
    });
  });
}

// 与上面set 命令基本一致，对请求进行记录等等，然后调用 RadtNode 的 propose 将 entry 推到 raft_层
void RedisStore::del(std::vector<std::string> keys, const StatusCallback& callback) {
  uint32_t commit_id = next_request_id_++;

  RaftCommit commit;
  commit.node_id = static_cast<uint32_t>(server_->node_id());
  commit.commit_id = commit_id;
  commit.redis_data.type = RedisCommitData::kCommitDel;  // 记录命令类型是 del，与 set 不同
  commit.redis_data.strs = std::move(keys);
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, commit);
  std::shared_ptr<std::vector<uint8_t>> data(new std::vector<uint8_t>(sbuf.data(), sbuf.data() + sbuf.size()));

  pending_requests_[commit_id] = callback;

  server_->propose(std::move(data), [this, commit_id](const Status& status) {
    io_service_.post([commit_id, status, this]() {

      auto it = pending_requests_.find(commit_id);
      if (it != pending_requests_.end()) {
        it->second(status);
        pending_requests_.erase(it);
      }
    });
  });
}

void RedisStore::get_snapshot(const GetSnapshotCallback& callback) {
  io_service_.post([this, callback] {
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, this->key_values_);
    SnapshotDataPtr data(new std::vector<uint8_t>(sbuf.data(), sbuf.data() + sbuf.size()));
    callback(data);  // redis 获取到快照的数据后，根据 raft_node 传入的函数处理 data
  });
}

// 将 snapshot 中的 kv 和 redisStore 中的 key_values_ 交换，从而实现从快照恢复 redisStore 中的 key_values_
void RedisStore::recover_from_snapshot(SnapshotDataPtr snap, const StatusCallback& callback) {
  io_service_.post([this, snap, callback] {
    std::unordered_map<std::string, std::string> kv;
    msgpack::object_handle oh = msgpack::unpack((const char*) snap->data(), snap->size());
    try {
      oh.get().convert(kv);
    } catch (std::exception& e) {
      callback(Status::io_error("invalid snapshot"));
      return;
    }
    std::swap(kv, key_values_);  // 从快照中恢复
    callback(Status::ok());  // 回调函数 ok
  });
}

// 回复所有的 keys，支持正则匹配
void RedisStore::keys(const char* pattern, int len, std::vector<std::string>& keys) {
  for (auto it = key_values_.begin(); it != key_values_.end(); ++it) {
    if (string_match_len(pattern, len, it->first.c_str(), it->first.size(), 0)) {  // 遍历k，符合模式匹配的加入到 keys 中
      keys.push_back(it->first);
    }
  }
}

//  entry中具体对 redis 的操作
void RedisStore::read_commit(proto::EntryPtr entry) {
  auto cb = [this, entry] {
    RaftCommit commit;
    try {
      msgpack::object_handle oh = msgpack::unpack((const char*) entry->data.data(), entry->data.size());
      oh.get().convert(commit);

    }
    catch (std::exception& e) {
      LOG_ERROR("bad entry %s", e.what());
      return;
    }
    RedisCommitData& data = commit.redis_data;

    switch (data.type) {
      case RedisCommitData::kCommitSet: {  // redis 的 set 操作
        assert(data.strs.size() == 2);  // kv 对
        this->key_values_[std::move(data.strs[0])] = std::move(data.strs[1]); // 设置kv
        break;
      }
      case RedisCommitData::kCommitDel: {  // redis 的 delete 操作
        for (const std::string& key : data.strs) {  // 删除kv
          this->key_values_.erase(key);
        }
        break;
      }
      default: {
        LOG_ERROR("not supported type %d", data.type);
      }
    }

    if (commit.node_id == server_->node_id()) {  // 提交节点的 id 为自身
      auto it = pending_requests_.find(commit.commit_id);  // redis 自身也会维护一个 map，存放请求与对应的处理函数
      if (it != pending_requests_.end()) {
        it->second(Status::ok());
        pending_requests_.erase(it);  // 去除该请求，表明已经处理
      }
    }
  };

  // post方法是一个异步方法，调用后会马上返回，它会向io_service请求分发任务，io_serivce会选择一个线程执行。
  io_service_.post(std::move(cb));  // 调用通信模块
}

}

