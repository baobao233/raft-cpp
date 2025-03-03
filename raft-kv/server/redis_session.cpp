#include <raft-kv/server/redis_session.h>
#include <raft-kv/common/log.h>
#include <unordered_map>
#include <raft-kv/server/redis_store.h>
#include <glib.h>

namespace kv {

#define RECEIVE_BUFFER_SIZE (1024 * 512)

namespace shared {

static const char* ok = "+OK\r\n";
static const char* err = "-ERR %s\r\n";
static const char* wrong_type = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
static const char* unknown_command = "-ERR unknown command `%s`\r\n";
static const char* wrong_number_arguments = "-ERR wrong number of arguments for '%s' command\r\n";
static const char* pong = "+PONG\r\n";
static const char* null = "$-1\r\n";

typedef std::function<void(RedisSessionPtr, struct redisReply* reply)> CommandCallback;

// 获取对应操作命令的回调函数，也就是回复函数
static std::unordered_map<std::string, CommandCallback> command_table = {
    {"ping", RedisSession::ping_command},
    {"PING", RedisSession::ping_command},
    {"get", RedisSession::get_command},
    {"GET", RedisSession::get_command},
    {"set", RedisSession::set_command},
    {"SET", RedisSession::set_command},
    {"del", RedisSession::del_command},
    {"DEL", RedisSession::del_command},
    {"keys", RedisSession::keys_command},
    {"KEYS", RedisSession::keys_command},
};

}

// REDIS 序列化回复
static void build_redis_string_array_reply(const std::vector<std::string>& strs, std::string& reply) {
  // redis 命令的文本序列化 比如：set key value会被解析为 *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
  // *3 表示为三个参数，\r\n 表示结束， $4表示后面的字节数， 
  //*2\r\n$4\r\nkey1\r\n$4key2\r\n

  // 将 keys 拼接成序列化回复
  char buffer[64];
  snprintf(buffer, sizeof(buffer), "*%lu\r\n", strs.size());
  reply.append(buffer);

  for (const std::string& str : strs) {
    snprintf(buffer, sizeof(buffer), "$%lu\r\n", str.size());
    reply.append(buffer);

    if (!str.empty()) {
      reply.append(str);
      reply.append("\r\n");
    }
  }
}

// 对 redisStore 的具体操作通过 redisSession 实现
RedisSession::RedisSession(RedisStore* server, boost::asio::io_service& io_service)
    : quit_(false),
      server_(server),
      socket_(io_service),
      read_buffer_(RECEIVE_BUFFER_SIZE),
      reader_(redisReaderCreate()) {
}

// redisSession 通过 hredis SDK 启动读 RedisStore
void RedisSession::start() {
  if (quit_) {
    return;
  }
  auto self = shared_from_this(); // 当一个类使用shared_ptr进行了托管，而这个类的成员函数想把类对象作为参数传给其他函数时可以使用 shraed_from_this()
  auto buffer = boost::asio::buffer(read_buffer_.data(), read_buffer_.size());
  auto handler = [self](const boost::system::error_code& error, size_t bytes) {
    if (bytes == 0) {
      return;
    }
    if (error) {
      LOG_DEBUG("read error %s", error.message().c_str());
      return;
    }

    self->handle_read(bytes);

  };
  socket_.async_read_some(buffer, std::move(handler));  // 异步读
}

// 调用 hredis 的读接口，处理读请求
void RedisSession::handle_read(size_t bytes) {
  // 获取读请求的内容起止位置
  uint8_t* start = read_buffer_.data();
  uint8_t* end = read_buffer_.data() + bytes;
  int err = REDIS_OK;
  std::vector<struct redisReply*> replies;

  while (!quit_ && start < end) {
    uint8_t* p = (uint8_t*) memchr(start, '\n', bytes);
    if (!p) {
      this->start();
      break;
    }

    size_t n = p + 1 - start;
    err = redisReaderFeed(reader_, (const char*) start, n);
    if (err != REDIS_OK) {
      LOG_DEBUG("redis protocol error %d, %s", err, reader_->errstr);
      quit_ = true;
      break;
    }

    struct redisReply* reply = NULL;
    err = redisReaderGetReply(reader_, (void**) &reply);  // 获取回复
    if (err != REDIS_OK) {
      LOG_DEBUG("redis protocol error %d, %s", err, reader_->errstr);
      quit_ = true;
      break;
    }
    if (reply) {
      replies.push_back(reply);  // 将回复压缩进 replies 中
    }

    start += n;
    bytes -= n;
  }
  if (err == REDIS_OK) {
    for (struct redisReply* reply : replies) {
      on_redis_reply(reply);  // 处理每个hredis 的回复
    }
    this->start();
  }

  for (struct redisReply* reply : replies) {
    freeReplyObject(reply);  // 处理完成后释放内存
  }
}

// 校验 redis 的回复 RedisReply
void RedisSession::on_redis_reply(struct redisReply* reply) {
  char buffer[256];
  // redis 回复有错
  if (reply->type != REDIS_REPLY_ARRAY) {
    LOG_WARN("wrong type %d", reply->type);
    send_reply(shared::wrong_type, strlen(shared::wrong_type));
    return;
  }

  if (reply->elements < 1) {
    LOG_WARN("wrong elements %lu", reply->elements);
    int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "");
    send_reply(buffer, n);
    return;
  }

  if (reply->element[0]->type != REDIS_REPLY_STRING) {
    LOG_WARN("wrong type %d", reply->element[0]->type);
    send_reply(shared::wrong_type, strlen(shared::wrong_type));
    return;
  }

  // redis 的回复无错误，校验具体的命令
  std::string command(reply->element[0]->str, reply->element[0]->len);
  auto it = shared::command_table.find(command);  // 获取操作命令
  if (it == shared::command_table.end()) {  // 找不到对应的命令
    int n = snprintf(buffer, sizeof(buffer), shared::unknown_command, command.c_str());
    send_reply(buffer, n);  // 回复“”命令未知”的消息
    return;
  }

  // 调用具体命令的回复函数
  shared::CommandCallback &cb = it->second; 
  cb(shared_from_this(), reply);
}

// 校验缓冲区，并且将需要发送的数据从缓冲区写入 socket，调整缓冲区的大小
void RedisSession::send_reply(const char* data, uint32_t len) {
  uint32_t bytes = send_buffer_.readable_bytes();  // 返回剩余需读的字节
  send_buffer_.put((uint8_t*) data, len);
  if (bytes == 0) {  // 原本没有需要 read 的字节，现在send_buffer_.put()后有了，因此开始发送
    start_send();    // 将数据写入到socket_
  }
}

// 将数据写入到socket_
void RedisSession::start_send() {
  if (!send_buffer_.readable()) {
    return;
  }
  auto self = shared_from_this();
  uint32_t remaining = send_buffer_.readable_bytes();  // 需要剩余需发送的字节
  auto buffer = boost::asio::buffer(send_buffer_.reader(), remaining);  // 需发送字节的数据缓冲区
  auto handler = [self](const boost::system::error_code& error, std::size_t bytes) {
    if (bytes == 0) {
      return;;
    }
    if (error) {
      LOG_DEBUG("send error %s", error.message().c_str());
      return;
    }
    std::string str((const char*) self->send_buffer_.reader(), bytes);
    self->send_buffer_.read_bytes(bytes);  // 调整读取位置
    self->start_send();
  };
  boost::asio::async_write(socket_, buffer, std::move(handler));  // 发送 read_ 到 write_ 之间的数据，也就是写入到 socket_
}

// ping 命令
void RedisSession::ping_command(std::shared_ptr<RedisSession> self, struct redisReply* reply) {
  self->send_reply(shared::pong, strlen(shared::pong));
}

// get 命令
void RedisSession::get_command(std::shared_ptr<RedisSession> self, struct redisReply* reply) {
  assert(reply->type = REDIS_REPLY_ARRAY);  // 确定返回的是一个数组
  assert(reply->elements > 0);  // 确定数组中元素个数＞0
  char buffer[256];

  if (reply->elements != 2) {
    LOG_WARN("wrong elements %lu", reply->elements);
    int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "get");
    self->send_reply(buffer, n);
    return;
  }

  if (reply->element[1]->type != REDIS_REPLY_STRING) {
    LOG_WARN("wrong type %d", reply->element[1]->type);
    self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
    return;
  }

  std::string value;
  std::string key(reply->element[1]->str, reply->element[1]->len);  // 获取 reply 中的查询的 key
  // 通过 hredis 这个 sdk 查询后，我们可以到真正的内存 redisStore 中进行真正的获取了
  bool get = self->server_->get(key, value);  // 从实例的 kv map中获取 value
  if (!get) {
    self->send_reply(shared::null, strlen(shared::null));  // 回复客户端没找到
  } else {
    // 像sprintf()一样格式化字符串和参数。但是，你没必要像sprintf()一样创建和指定一个缓冲区，GLib将这些自动做了
    char* str = g_strdup_printf("$%lu\r\n%s\r\n", value.size(), value.c_str());
    self->send_reply(str, strlen(str));
    g_free(str);  // 释放内存
  }
}

// set 命令的具体操作
void RedisSession::set_command(std::shared_ptr<RedisSession> self, struct redisReply* reply) {
  assert(reply->type = REDIS_REPLY_ARRAY);
  assert(reply->elements > 0);
  char buffer[256];

  if (reply->elements != 3) {
    LOG_WARN("wrong elements %lu", reply->elements);
    int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "set");
    self->send_reply(buffer, n);
    return;
  }

  if (reply->element[1]->type != REDIS_REPLY_STRING || reply->element[2]->type != REDIS_REPLY_STRING) {
    LOG_WARN("wrong type %d", reply->element[1]->type);
    self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
    return;
  }
  std::string key(reply->element[1]->str, reply->element[1]->len);
  std::string value(reply->element[2]->str, reply->element[2]->len);
  // 调用 redis_store 的 set 命令，会添加进请求队列中，并且传入 set 命令处理完的后续需要执行的函数
  self->server_->set(std::move(key), std::move(value), [self](const Status& status) {
    // 判断 set 是否成功
    if (status.is_ok()) {  // 成功则回复 ok
      self->send_reply(shared::ok, strlen(shared::ok));
    } else {  // 不成功则回复错误
      char buff[256];
      int n = snprintf(buff, sizeof(buff), shared::err, status.to_string().c_str());
      self->send_reply(buff, n);
    }
  });
}

// del 命令的具体操作，可以删除多个值
void RedisSession::del_command(std::shared_ptr<RedisSession> self, struct redisReply* reply) {
  assert(reply->type = REDIS_REPLY_ARRAY);
  assert(reply->elements > 0);
  char buffer[256];

  if (reply->elements <= 1) { ;
    int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "del");
    self->send_reply(buffer, n);
    return;
  }

  // 取出 RedisReply 中所有需要删除的key
  std::vector<std::string> keys;
  for (size_t i = 1; i < reply->elements; ++i) {
    redisReply* element = reply->element[i];
    if (element->type != REDIS_REPLY_STRING) {
      self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
      return;
    }

    keys.emplace_back(element->str, element->len);
  }

  // 调用 redisStore 中的 del 命令
  self->server_->del(std::move(keys), [self](const Status& status) {
    if (status.is_ok()) {
      self->send_reply(shared::ok, strlen(shared::ok));
    } else {
      char buff[256];
      int n = snprintf(buff, sizeof(buff), shared::err, status.to_string().c_str());
      self->send_reply(buff, n);
    }
  });
}

// keys 命令
void RedisSession::keys_command(std::shared_ptr<RedisSession> self, struct redisReply* reply) {
  assert(reply->type = REDIS_REPLY_ARRAY);
  assert(reply->elements > 0);
  char buffer[256];

  if (reply->elements != 2) { ;
    int n = snprintf(buffer, sizeof(buffer), shared::wrong_number_arguments, "keys");
    self->send_reply(buffer, n);
    return;
  }

  redisReply* element = reply->element[1];

  if (element->type != REDIS_REPLY_STRING) {
    self->send_reply(shared::wrong_type, strlen(shared::wrong_type));
    return;
  }

  std::vector<std::string> keys;
  self->server_->keys(element->str, element->len, keys);  // 返回符合模式匹配的 keys
  std::string str;
  build_redis_string_array_reply(keys, str);  // 将 keys 取出拼接成序列化回复
  self->send_reply(str.data(), str.size());
}

}
