
#include <string.h>
#include <raft-kv/common/bytebuffer.h>

namespace kv {
static uint32_t MIN_BUFFERING = 4096;

ByteBuffer::ByteBuffer()
    : reader_(0),
      writer_(0),
      buff_(MIN_BUFFERING) {

}

void ByteBuffer::put(const uint8_t* data, uint32_t len) {
  uint32_t left = static_cast<uint32_t>(buff_.size()) - writer_;  // 剩余可写字节
  if (left < len) {  // 扩容
    buff_.resize(buff_.size() * 2 + len, 0);
  }
  memcpy(buff_.data() + writer_, data, len);  // 数据迁移
  writer_ += len;  // 已写字节增加
}

uint32_t ByteBuffer::readable_bytes() const {
  assert(writer_ >= reader_);
  return writer_ - reader_;
}

void ByteBuffer::read_bytes(uint32_t bytes) {
  assert(readable_bytes() >= bytes);
  reader_ += bytes;
  may_shrink_to_fit();
}

// 由于 reder_ == writer_，因此读取追上写，重置
void ByteBuffer::may_shrink_to_fit() {
  if (reader_ == writer_) {
    reader_ = 0;
    writer_ = 0;
  }
}

void ByteBuffer::reset() {
  reader_ = writer_ = 0;
  buff_.resize(MIN_BUFFERING);
  buff_.shrink_to_fit();
}

}

