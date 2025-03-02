#include <boost/algorithm/string.hpp>
#include <raft-kv/transport/raft_server.h>
#include <raft-kv/common/log.h>
#include <raft-kv/transport/proto.h>
#include <raft-kv/transport/transport.h>
#include <boost/asio.hpp>

namespace kv {

// 服务端会话
class AsioServer;
class ServerSession : public std::enable_shared_from_this<ServerSession> {
 public:
  explicit ServerSession(boost::asio::io_service& io_service, AsioServer* server)
      : socket(io_service),
        server_(server) {

  }

  // 服务端读取TransportMeta结构体
  void start_read_meta() {
    assert(sizeof(meta_) == 5);
    meta_.type = 0;
    meta_.len = 0;
    auto self = shared_from_this();
    auto buffer = boost::asio::buffer(&meta_, sizeof(meta_));  // buffer 用于储存读取到的数据
    auto handler = [self](const boost::system::error_code& error, std::size_t bytes) {  // 读取到 buffer 完成后的 handler，bytes 是读取到的字节数
      if (bytes == 0) {
        return;;
      }
      if (error) {
        LOG_DEBUG("read error %s", error.message().c_str());
        return;
      }

      if (bytes != sizeof(meta_)) {
        LOG_DEBUG("invalid data len %lu", bytes);
        return;
      }
      self->start_read_message(); // 服务端读取 TransportMeta 结构体中的数据
    };

    boost::asio::async_read(socket, buffer, boost::asio::transfer_exactly(sizeof(meta_)), handler); // transfer_exactly准确读取sizeof(meta_)大小的数据到 buffer 中
  }

  // 读取TransportMeta结构体中的数据
  void start_read_message() {
    uint32_t len = ntohl(meta_.len); // TransportMeta内部数据的大小
    if (buffer_.capacity() < len) {
      buffer_.resize(len);
    }

    auto self = shared_from_this();
    auto buffer = boost::asio::buffer(buffer_.data(), len);  // 创建结构体内部数据大小的 buffer
    auto handler = [self, len](const boost::system::error_code& error, std::size_t bytes) {
      assert(len == ntohl(self->meta_.len));
      if (error || bytes == 0) {
        LOG_DEBUG("read error %s", error.message().c_str());
        return;
      }

      if (bytes != len) {
        LOG_DEBUG("invalid data len %lu, %u", bytes, len);
        return;
      }
      self->decode_message(len);  // 解析结构体内部数据
    };
    boost::asio::async_read(socket, buffer, boost::asio::transfer_exactly(len), handler);  // 读取结构体内部数据
  }

  // 解析 meta_ 数据
  void decode_message(uint32_t len) {
    // 根据不同的数据类型不同的处理
    switch (meta_.type) {
      case TransportTypeDebug: {
        assert(len == sizeof(DebugMessage));
        DebugMessage* dbg = (DebugMessage*) buffer_.data();
        assert(dbg->a + 1 == dbg->b);
        //LOG_DEBUG("tick ok");
        break;
      }
      case TransportTypeStream: {  // Message
        proto::MessagePtr msg(new proto::Message());
        try {
          msgpack::object_handle oh = msgpack::unpack((const char*) buffer_.data(), len);
          oh.get().convert(*msg);  // 转化成 raft 中的 message
        }
        catch (std::exception& e) {
          LOG_ERROR("bad message %s, size = %lu, type %s",
                    e.what(),
                    buffer_.size(),
                    proto::msg_type_to_string(msg->type));
          return;
        }
        on_receive_stream_message(std::move(msg));
        break;
      }
      default: {
        LOG_DEBUG("unknown msg type %d, len = %d", meta_.type, ntohl(meta_.len));
        return;
      }
    }

    start_read_meta();
  }

  void on_receive_stream_message(proto::MessagePtr msg);

  boost::asio::ip::tcp::socket socket;
 private:
  AsioServer* server_;
  TransportMeta meta_;
  std::vector<uint8_t> buffer_;
};
typedef std::shared_ptr<ServerSession> ServerSessionPtr;

// IoServer 的具体实现，表示一个服务端，监听请求，（在RaftServer之上包装一层，负责协调RaftServer和io_service之间的调用）
class AsioServer : public IoServer {
 public:
 // 显式初始化
  explicit AsioServer(boost::asio::io_service& io_service,
                      const std::string& host,
                      RaftServer* raft)
      : io_service_(io_service),
        acceptor_(io_service),
        raft_(raft) {
    // 获取 endpoint
    std::vector<std::string> strs;
    boost::split(strs, host, boost::is_any_of(":"));
    if (strs.size() != 2) {
      LOG_DEBUG("invalid host %s", host.c_str());
      exit(0);
    }
    auto address = boost::asio::ip::address::from_string(strs[0]);
    int port = std::atoi(strs[1].c_str());
    auto endpoint = boost::asio::ip::tcp::endpoint(address, port);

    // 作为服务端监听请求
    acceptor_.open(endpoint.protocol());
    acceptor_.set_option(boost::asio::ip::tcp::acceptor::reuse_address(1));
    acceptor_.bind(endpoint);
    acceptor_.listen();
    LOG_DEBUG("listen at %s:%d", address.to_string().c_str(), port);
  }

  ~AsioServer() {

  }

  // transport.cpp 中调用，开启服务端的会话
  void start() final {
    ServerSessionPtr session(new ServerSession(io_service_, this));
    acceptor_.async_accept(session->socket, [this, session](const boost::system::error_code& error) {
      if (error) {
        LOG_DEBUG("accept error %s", error.message().c_str());
        return;
      }

      this->start();
      session->start_read_meta();  // 服务端会话开始读取发送过来的熟悉
    });
  }

  void stop() final {

  }

  // 调用 raftServer 处理 Message 
  void on_message(proto::MessagePtr msg) {
    raft_->process(std::move(msg), [](const Status& status) {
      if (!status.is_ok()) {
        LOG_ERROR("process error %s", status.to_string().c_str());
      }
    });
  }

 private:
  boost::asio::io_service& io_service_;
  boost::asio::ip::tcp::acceptor acceptor_;
  RaftServer* raft_;
};

// 调用 ioServer 调用 raft_ 层传递 message 处理
void ServerSession::on_receive_stream_message(proto::MessagePtr msg) {
  server_->on_message(std::move(msg));
}

std::shared_ptr<IoServer> IoServer::create(void* io_service,
                                           const std::string& host,
                                           RaftServer* raft) {
  std::shared_ptr<AsioServer> server(new AsioServer(*(boost::asio::io_service*) io_service, host, raft));
  return server;
}

}
