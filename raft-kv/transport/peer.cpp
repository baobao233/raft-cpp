#include <boost/algorithm/string.hpp>
#include <raft-kv/transport/peer.h>
#include <raft-kv/common/log.h>
#include <raft-kv/common/bytebuffer.h>
#include <raft-kv/transport/proto.h>
#include <boost/asio.hpp>

namespace kv {

class PeerImpl;
class ClientSession {  // 将其声明为 PeerImpl 的友元，ClientSession可以使用PeerImpl的私有元素，主要是调用 peer 发送数据
 public:
  explicit ClientSession(boost::asio::io_service& io_service, PeerImpl* peer);  // 使用PeerImpl的io_service初始化

  ~ClientSession() {

  }

  // 发送数据
  void send(uint8_t transport_type, const uint8_t* data, uint32_t len) {
    uint32_t remaining = buffer_.readable_bytes();  // 获取现在可写的空间

    TransportMeta meta;
    meta.type = transport_type;  // 传输消息的类型
    meta.len = htonl(len);  // 将主机的unsigned long值转换成网络字节顺序（32位）（一般主机跟网络上传输的字节顺序是不通的，分大小端），函数返回一个网络字节顺序的数字。
    assert(sizeof(TransportMeta) == 5);
    // 先将数据 put 进数据缓冲区中，增加 write_ 位置
    buffer_.put((const uint8_t*) &meta, sizeof(TransportMeta));
    buffer_.put(data, len);
    assert(remaining + sizeof(TransportMeta) + len == buffer_.readable_bytes());

    if (connected_ && remaining == 0) {
      start_write();  // 如果缓冲区满了，则 write 到 socket_中去
    }
  }

  void close_session();

  void start_connect() {
    socket_.async_connect(endpoint_, [this](const boost::system::error_code& err) {  // 检查链接
      if (err) {
        LOG_DEBUG("connect [%lu] error %s", this->peer_id_, err.message().c_str());
        this->close_session();
        return;
      }
      this->connected_ = true;
      LOG_INFO("connected to [%lu]", this->peer_id_);

      if (this->buffer_.readable()) {  // 有可读则 write
        this->start_write();
      }
    });
  }

  // 将 bytes 字节的数据写入到 socket_ 中
  void start_write() {
    if (!buffer_.readable()) {
      return;
    }

    uint32_t remaining = buffer_.readable_bytes();
    auto buffer = boost::asio::buffer(buffer_.reader(), remaining);
    auto handler = [this](const boost::system::error_code& error, std::size_t bytes) {  // 后续处理写的 byte
      if (error || bytes == 0) {
        LOG_DEBUG("send [%lu] error %s", this->peer_id_, error.message().c_str());
        this->close_session();
        return;
      }
      this->buffer_.read_bytes(bytes);  // 调整 read_ 和 write_ 位置
      this->start_write();
    };
    boost::asio::async_write(socket_, buffer, handler); // 异步写，
  }

 private:
  boost::asio::ip::tcp::socket socket_;
  boost::asio::ip::tcp::endpoint endpoint_;
  PeerImpl* peer_;  // Peer 具体实现
  uint64_t peer_id_;  // peer 的 id
  ByteBuffer buffer_;  // 数据缓冲区
  bool connected_;
};

class PeerImpl : public Peer {
 public:
  explicit PeerImpl(boost::asio::io_service& io_service, uint64_t peer, const std::string& peer_str)
      : peer_(peer),
        io_service_(io_service),
        timer_(io_service) {
    std::vector<std::string> strs;
    boost::split(strs, peer_str, boost::is_any_of(":"));  // peer_str is address:port
    if (strs.size() != 2) {
      LOG_DEBUG("invalid host %s", peer_str.c_str());
      exit(0);
    }
    auto address = boost::asio::ip::address::from_string(strs[0]);  // address
    int port = std::atoi(strs[1].c_str());  // port
    endpoint_ = boost::asio::ip::tcp::endpoint(address, port); // 转为endpoint格式
  }

  ~PeerImpl() final {
  }

  void start() final {
    start_timer();
  };

  // 在 transport.cpp 中找到 to 的 peerImp后，调用本peerImp.send()打包消息，并且调用ClientSession 发送消息
  void send(proto::MessagePtr msg) final {
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, *msg);

    do_send_data(TransportTypeStream, (const uint8_t*) sbuf.data(), (uint32_t) sbuf.size());
  }

  void send_snap(proto::SnapshotPtr snap) final {
    LOG_DEBUG("no impl yet");
  }

  void update(const std::string& peer) final {
    LOG_DEBUG("no impl yet");
  }

  uint64_t active_since() final {
    LOG_DEBUG("no impl yet");
    return 0;
  }

  void stop() final {

  }

 private:
  // 调用 ClientSession 发送 data 
  void do_send_data(uint8_t type, const uint8_t* data, uint32_t len) {
    if (!session_) {  // ClientSession 的初始化
      session_ = std::make_shared<ClientSession>(io_service_, this);
      session_->send(type, data, len);
      session_->start_connect();
    } else {
      session_->send(type, data, len);
    }
  }

  // 启动计时
  void start_timer() {
    timer_.expires_from_now(boost::posix_time::seconds(3));
    timer_.async_wait([this](const boost::system::error_code& err) {
      if (err) {
        LOG_ERROR("timer waiter error %s", err.message().c_str());
        return;
      }
      this->start_timer();
    });

    static std::atomic<uint32_t> tick;
    DebugMessage dbg;
    dbg.a = tick++;
    dbg.b = tick++;
    do_send_data(TransportTypeDebug, (const uint8_t*) &dbg, sizeof(dbg));  // 发送一则开启通信模块的 debug 消息
  }

  uint64_t peer_;   // peer id
  boost::asio::io_service& io_service_;
  friend class ClientSession;
  std::shared_ptr<ClientSession> session_;  // 客户端会话
  boost::asio::ip::tcp::endpoint endpoint_;  // address/port
  boost::asio::deadline_timer timer_;
};

// 初始化
ClientSession::ClientSession(boost::asio::io_service& io_service, PeerImpl* peer)
    : socket_(io_service),
      endpoint_(peer->endpoint_),
      peer_(peer),
      peer_id_(peer_->peer_),
      connected_(false) {

}

void ClientSession::close_session() {
  peer_->session_ = nullptr;
}

// 创建一个Peer 实例
std::shared_ptr<Peer> Peer::creat(uint64_t peer, const std::string& peer_str, void* io_service) {
  std::shared_ptr<PeerImpl> peer_ptr(new PeerImpl(*(boost::asio::io_service*) io_service, peer, peer_str));
  return peer_ptr;
}

}
