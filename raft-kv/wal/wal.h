#pragma once
#include <raft-kv/raft/proto.h>
#include <memory>
#include <raft-kv/common/status.h>
#include <stdio.h>

namespace kv {

// 快照的 wal
struct WAL_Snapshot {
  uint64_t index;
  uint64_t term;
  MSGPACK_DEFINE (index, term);
};

typedef uint8_t WAL_type;

#pragma pack(1)
// 数据的 wal
struct WAL_Record {
  WAL_type type;  /*the data type*/
  uint8_t len[3]; // 将data的长度分解成三部分，这样可以在需要时轻松地将长度信息存储为字节数组，便于在不同的系统或网络传输中使用。
  uint32_t crc;   /*crc32 for data*/
  char data[0];
};
#pragma pack()

#define MAX_WAL_RECORD_LEN (0x00FFFFFF)

// 还原数据长度
static inline uint32_t WAL_Record_len(const WAL_Record& record) {
  return uint32_t(record.len[2]) << 16 | uint32_t(record.len[1]) << 8 | uint32_t(record.len[0]) << 0;
}

// 将一个 32 位的长度值分解为三个字节，并存储在 WAL_Record 的 len 数组中。通过这种方式，可以在需要时轻松地将data 的长度信息存储为字节数组，便于在不同的系统或网络传输中使用。
static inline void set_WAL_Record_len(WAL_Record& record, uint32_t len) {
  len = std::min(len, (uint32_t) MAX_WAL_RECORD_LEN);  // 限制传入的长度
  record.len[2] = (len >> 16) & 0x000000FF;  // 储存长度的高位字节，第 16 到 23 位
  record.len[1] = (len >> 8) & 0x000000FF;   // 存储长度的中间字节（第 8 到 15 位）
  record.len[0] = (len >> 0) & 0x000000FF;   // 存储长度的低位字节（第 0 到 7 位）
}

class WAL_File;

class WAL;
typedef std::shared_ptr<WAL> WAL_ptr;
class WAL {
 public:
  static void create(const std::string& dir);

  static WAL_ptr open(const std::string& dir, const WAL_Snapshot& snap);

  ~WAL() = default;

  //After read_all, the WAL will be ready for appending new records.
  Status read_all(proto::HardState& hs, std::vector<proto::EntryPtr>& ents);

  Status save(proto::HardState hs, const std::vector<proto::EntryPtr>& ents);

  Status save_snapshot(const WAL_Snapshot& snap);

  Status save_entry(const proto::Entry& entry);

  Status save_hard_state(const proto::HardState& hs);

  Status cut();

  // release_to releases the wal file, which has smaller index than the given index
  // except the largest one among them.
  // For example, if WAL is holding lock 1,2,3,4,5,6, release_to(4) will release
  // lock 1,2 but keep 3. release_to(5) will release 1,2,3 but keep 4.
  Status release_to(uint64_t index);

  void get_wal_names(const std::string& dir, std::vector<std::string>& names);

  static bool parse_wal_name(const std::string& name, uint64_t* seq, uint64_t* index);

  // names should have been sorted based on sequence number.
  // is_valid_seq checks whether seq increases continuously.
  static bool is_valid_seq(const std::vector<std::string>& names);

  static bool search_index(const std::vector<std::string>& names, uint64_t index, uint64_t* name_index);

 private:
  explicit WAL(const std::string& dir)
      : dir_(dir),
      enti_(0) {
    memset(&start_, 0, sizeof(start_));
  }

  void handle_record_wal_record(WAL_type type,
                                const char* data,
                                size_t data_len,
                                bool& matchsnap,
                                proto::HardState& hs,
                                std::vector<proto::EntryPtr>& ents);

  std::string dir_;
  proto::HardState state_;  // hardstate recorded at the head of WAL
  WAL_Snapshot start_;      // snapshot to start reading
  uint64_t enti_;            // index of the last entry saved to the wal
  std::vector<std::shared_ptr<WAL_File>> files_;  // 维护了所有的 wal 日志

};

}
