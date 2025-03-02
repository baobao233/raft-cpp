#pragma once
#include <memory>
#include <boost/asio.hpp>
#include <hiredis/hiredis.h>
#include <raft-kv/common/bytebuffer.h>

namespace kv {

class RedisStore;
class RedisSession : public std::enable_shared_from_this<RedisSession> {
 public:
  explicit RedisSession(RedisStore* server, boost::asio::io_service& io_service);

  ~RedisSession() {
    redisReaderFree(reader_);
  }

  void start();

  void handle_read(size_t bytes);

  void on_redis_reply(struct redisReply* reply);

  void send_reply(const char* data, uint32_t len);

  void start_send();

  static void ping_command(std::shared_ptr<RedisSession> self, struct redisReply* reply);

  static void get_command(std::shared_ptr<RedisSession> self, struct redisReply* reply);

  static void set_command(std::shared_ptr<RedisSession> self, struct redisReply* reply);

  static void del_command(std::shared_ptr<RedisSession> self, struct redisReply* reply);

  static void keys_command(std::shared_ptr<RedisSession> self, struct redisReply* reply);
 public:
  bool quit_;
  RedisStore* server_;   // redisStore
  boost::asio::ip::tcp::socket socket_; // 传输模块，使用redisStore 的 socket_ 私有成员初始化
  std::vector<uint8_t> read_buffer_;  // 读取消息的缓冲区
  redisReader* reader_;
  ByteBuffer send_buffer_;  // 发送消息的缓冲区
};
typedef std::shared_ptr<RedisSession> RedisSessionPtr;

}