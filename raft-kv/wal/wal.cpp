#include <raft-kv/wal/wal.h>
#include <raft-kv/common/log.h>
#include <boost/filesystem.hpp>
#include <fcntl.h>
#include <raft-kv/raft/util.h>
#include <sstream>
#include <inttypes.h>
#include <raft-kv/raft/util.h>

namespace kv {

static const WAL_type wal_InvalidType = 0;
static const WAL_type wal_EntryType = 1;
static const WAL_type wal_StateType = 2;
static const WAL_type wal_CrcType = 3;
static const WAL_type wal_snapshot_Type = 4;
static const int SegmentSizeBytes = 64 * 1000 * 1000; // 64MB

static std::string wal_name(uint64_t seq, uint64_t index) {
  char buffer[64];
  snprintf(buffer, sizeof(buffer), "%016" PRIx64 "-%016" PRIx64 ".wal", seq, index);
  return buffer;
}

class WAL_File {
 public:
  WAL_File(const char* path, int64_t seq)
      : seq(seq),
        file_size(0) {
    fp = fopen(path, "a+");
    if (!fp) {
      LOG_FATAL("fopen error %s", strerror(errno));
    }

    file_size = ftell(fp);
    if (file_size == -1) {
      LOG_FATAL("ftell error %s", strerror(errno));
    }

    if (fseek(fp, 0L, SEEK_SET) == -1) {
      LOG_FATAL("fseek error %s", strerror(errno));
    }
  }

  ~WAL_File() {
    fclose(fp);
  }

  // 截断文件中 offset 之后的文件内容
  void truncate(size_t offset) {
    if (ftruncate(fileno(fp), offset) != 0) {
      LOG_FATAL("ftruncate error %s", strerror(errno));
    }

    if (fseek(fp, offset, SEEK_SET) == -1) {
      LOG_FATAL("fseek error %s", strerror(errno));
    }

    file_size = offset;
    data_buffer.clear();
  }

  // 主要作用是将一个新的日志记录（包括其类型、校验值和长度）以及实际数据追加到 data_buffer 中。
  // 通过这种方式，函数确保每个日志记录都包含完整的元数据和数据，便于后续的存储和处理
  void append(WAL_type type, const uint8_t* data, size_t len) {
    WAL_Record record;
    record.type = type;
    record.crc = compute_crc32((char*) data, len);
    set_WAL_Record_len(record, len);
    uint8_t* ptr = (uint8_t*) &record; // ptr 指向 record 内存中的开头
    data_buffer.insert(data_buffer.end(), ptr, ptr + sizeof(record)); // 将 record 占用的字节数组存入 data_buffer 中
    data_buffer.insert(data_buffer.end(), data, data + len);          // 将传入的 data 数据追加到 data_buffer 的末尾。
  }

  // 将数据缓冲区中的数据落盘，增加已经记录的文件大小，清空数据缓冲区
  void sync() {
    if (data_buffer.empty()) {
      return;
    }

    size_t bytes = fwrite(data_buffer.data(), 1, data_buffer.size(), fp);
    if (bytes != data_buffer.size()) {
      LOG_FATAL("fwrite error %s", strerror(errno));
    }

    file_size += data_buffer.size();
    data_buffer.clear();
  }

  void read_all(std::vector<char>& out) {
    char buffer[1024];
    while (true) {
      size_t bytes = fread(buffer, 1, sizeof(buffer), fp);
      if (bytes <= 0) {
        break;
      }

      out.insert(out.end(), buffer, buffer + bytes);
    }

    fseek(fp, 0L, SEEK_END);
  }

  std::vector<uint8_t> data_buffer; // 按照顺序储存了：1、WAL_Record的指针；2、每个WAL_Record包含的完整的数据
  int64_t seq;
  long file_size;
  FILE* fp;
};

// 该函数的主要作用是创建一个新的 WAL 文件，并在其中初始化一个空的快照记录。
// 通过使用临时文件和重命名操作，确保 WAL 文件的创建过程是安全和一致的。这一步确保 WAL 文件的创建是原子性的，避免在写入过程中出现不一致。
void WAL::create(const std::string& dir) {
  using namespace boost;

  filesystem::path walFile = filesystem::path(dir) / wal_name(0, 0);
  std::string tmpPath = walFile.string() + ".tmp";  // 创建一个临时文件保证其创建时的原子性

  if (filesystem::exists(tmpPath)) {
    filesystem::remove(tmpPath);
  }

  {
    std::shared_ptr<WAL_File> wal(new WAL_File(tmpPath.c_str(), 0));  // 创建一个 WAL 文件并与刚才的文件路径相关联
    WAL_Snapshot snap;
    snap.term = 0;
    snap.index = 0;
    msgpack::sbuffer sbuf;
    msgpack::pack(sbuf, snap);  // 将初始快照打包到流缓冲区中
    wal->append(wal_snapshot_Type, (uint8_t*) sbuf.data(), sbuf.size());  // 添加到数据缓冲区
    wal->sync();
  }

  filesystem::rename(tmpPath, walFile);  // 重命名临时文件确保创建 wal 文件的原子性
}

// 根据给定的快照信息打开并初始化 WAL 文件集合。
WAL_ptr WAL::open(const std::string& dir, const WAL_Snapshot& snap) {
  WAL_ptr w(new WAL(dir));  // 与 wal 目录相关联

  std::vector<std::string> names;
  w->get_wal_names(dir, names);  // 搜索所有以 wal 结尾的文件，将文件名字放在 names中,并且已经根据序列号 seq 排序了
  if (names.empty()) {
    LOG_FATAL("wal not found");
  }

  uint64_t nameIndex;
  if (!WAL::search_index(names, snap.index, &nameIndex)) {  // 搜索与snapshot index 一致或者大于需要搜索的 wal 文件的 index 是第 nameIndex 个
    LOG_FATAL("wal not found");
  }

  std::vector<std::string> check_names(names.begin() + nameIndex, names.end()); // 提取从 nameIndex 开始的文件名子序列
  if (!WAL::is_valid_seq(check_names)) {  // 验证文件序列的有效性
    LOG_FATAL("invalid wal seq");
  }

  for (const std::string& name: check_names) {
    uint64_t seq;
    uint64_t index;
    if (!parse_wal_name(name, &seq, &index)) {  // 解析文件名以获取序列号和索引。
      LOG_FATAL("invalid wal name %s", name.c_str());
    }

    boost::filesystem::path path = boost::filesystem::path(w->dir_) / name;  // 构建文件路径
    std::shared_ptr<WAL_File> file(new WAL_File(path.string().c_str(), seq)); // 创建 WAL_File 对象关联文件路径
    w->files_.push_back(file);  // 将其添加到 w->files_ 列表中维护。
  }

  memcpy(&w->start_, &snap, sizeof(snap));
  return w;
}

// 从 WAL 文件中读取和解析所有日志记录，确保数据的完整性和一致性。
// 它通过验证记录长度和 CRC 校验来检测和处理损坏的记录，并确保至少有一个快照记录存在。
Status WAL::read_all(proto::HardState& hs, std::vector<proto::EntryPtr>& ents) {
  std::vector<char> data; // 存储从每个 WAL_File 读取的数据。
  for (auto file : files_) {  // handle每个WAL_File
    data.clear();
    file->read_all(data); // 将 WAL_File中的数据读取到 data 中
    size_t offset = 0;    // 跟踪当前解析的位置。
    bool matchsnap = false; // 标记是否找到快照记录。

    while (offset < data.size()) {
      size_t left = data.size() - offset;  // 计算剩余长度
      size_t record_begin_offset = offset;

      if (left < sizeof(WAL_Record)) {  // 剩余长度如果小于一个 WAL_Record 的大小，则截断offset之后的内容，并且 break
        file->truncate(record_begin_offset);
        LOG_WARN("invalid record len %lu", left);
        break;
      }

      // 复制 data 中的数据到 record 中
      WAL_Record record;
      memcpy(&record, data.data() + offset, sizeof(record));

      left -= sizeof(record);  // 更新剩余数据量
      offset += sizeof(record);  // 更新新的 offset

      if (record.type == wal_InvalidType) {  // 如果记录类型为 wal_InvalidType，则中断解析。
        break;
      }

      uint32_t record_data_len = WAL_Record_len(record);  // 计算数据长度
      if (left < record_data_len) {  // 剩余数据长度小于记录数据长度，截断文件并记录警告日志。
        file->truncate(record_begin_offset);
        LOG_WARN("invalid record data len %lu, %u", left, record_data_len);
        break;
      }

      char* data_ptr = data.data() + offset;  // 从 data 中提取指向WAL_Record数据的指针
      uint32_t crc = compute_crc32(data_ptr, record_data_len); // 计算从指针开始到数据结束的位置的 CRC32 校验值。

      left -= record_data_len;
      offset += record_data_len;

      if (record.crc != 0 && crc != record.crc) {  // 比对 record 中存的 CRC32 校验值。
        file->truncate(record_begin_offset);
        LOG_WARN("invalid record crc %u, %u", record.crc, crc);
        break;
      }

      // 处理 wal 记录数据
      handle_record_wal_record(record.type, data_ptr, record_data_len, matchsnap, hs, ents);

      if (record.type == wal_snapshot_Type) {  // 如果记录类型为 wal_snapshot_Type，将 matchsnap 设置为 true。
        matchsnap = true;
      }
    }

    if (!matchsnap) {
      LOG_FATAL("wal: snapshot not found");
    }
  }

  return Status::ok();
}

// 处理解析的记录
void WAL::handle_record_wal_record(WAL_type type,
                                   const char* data,
                                   size_t data_len,
                                   bool& matchsnap,
                                   proto::HardState& hs,
                                   std::vector<proto::EntryPtr>& ents) {

  switch (type) {
    case wal_EntryType: {  // 如果是 entry
      proto::EntryPtr entry(new proto::Entry());
      msgpack::object_handle oh = msgpack::unpack(data, data_len);
      oh.get().convert(*entry);  // 解包为 entry

      if (entry->index > start_.index) { // 如果该日志的 index 大于 snapshot 的 index，重置 raft_node中的 entries 容量大小
        ents.resize(entry->index - start_.index - 1);
        ents.push_back(entry);  // 添加 entry
      }

      enti_ = entry->index;  // wal 日志中最后一笔 entry 的 index 被更新为新的 index
      break;
    }

    case wal_StateType: {  // 如果是状态 wal 日志
      msgpack::object_handle oh = msgpack::unpack(data, data_len);
      oh.get().convert(hs);  // 更新其 hard state
      break;
    }

    case wal_snapshot_Type: {  // 检查 wal 中的 snapshot 是否和目前记录的起始位置一样，如果不一样则返回不匹配
      WAL_Snapshot snap;
      msgpack::object_handle oh = msgpack::unpack(data, data_len);
      oh.get().convert(snap);

      if (snap.index == start_.index) {
        if (snap.term != start_.term) {
          LOG_FATAL("wal: snapshot mismatch");
        }
        matchsnap = true;
      }
      break;
    }

    case wal_CrcType: {
      LOG_FATAL("wal crc type");
      break;
    }
    default: {
      LOG_FATAL("invalid record type %d", type);
    }
  }
}

// 追加hs 和 entry 类型的 entry
Status WAL::save(proto::HardState hs, const std::vector<proto::EntryPtr>& ents) {
  // short cut, do not call sync
  if (hs.is_empty_state() && ents.empty()) {
    return Status::ok();
  }

  bool mustSync = is_must_sync(hs, state_, ents.size());
  Status status;

  for (const proto::EntryPtr& entry: ents) {
    status = save_entry(*entry);  // 往最后一个WAL_File中追加 Entry 类型的Record
    if (!status.is_ok()) {
      return status;
    }
  }

  status = save_hard_state(hs); // 往最后一个WAL_File中追加 HS 类型的Record
  if (!status.is_ok()) {
    return status;
  }

  if (files_.back()->file_size < SegmentSizeBytes) {
    if (mustSync) {
      files_.back()->sync();  // 落盘
    }
    return Status::ok();
  }

  return cut();
}

// 调用 sync()落盘
Status WAL::cut() {
  files_.back()->sync();
  return Status::ok();
}

// 追加wal_snapshot_Type类型的 wal，并且落盘
Status WAL::save_snapshot(const WAL_Snapshot& snap) {
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, snap);

  files_.back()->append(wal_snapshot_Type, (uint8_t*)sbuf.data(), sbuf.size());
  if (enti_ < snap.index) {
    enti_ = snap.index;
  }
  files_.back()->sync();
  return Status::ok();
}

// file_末尾追加wal_EntryType类型的 wal
Status WAL::save_entry(const proto::Entry& entry) {
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, entry);  // 打包 entry 到 sbuf 中

  files_.back()->append(wal_EntryType, (uint8_t*)sbuf.data(), sbuf.size());  // 追加 WAL_File 中的record
  enti_ = entry.index;
  return Status::ok();
}

// file_末尾追加wal_StateType类型的 wal
Status WAL::save_hard_state(const proto::HardState& hs) {
  if (hs.is_empty_state()) {
    return Status::ok();
  }
  state_ = hs;

  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, hs);
  files_.back()->append(wal_StateType, (uint8_t*)sbuf.data(), sbuf.size());
  return Status::ok();
}

// 搜索所有以 wal 结尾的文件，将文件名字放在 names中
void WAL::get_wal_names(const std::string& dir, std::vector<std::string>& names) {
  using namespace boost;

  filesystem::directory_iterator end;
  for (boost::filesystem::directory_iterator it(dir); it != end; it++) {
    filesystem::path filename = (*it).path().filename();
    filesystem::path extension = filename.extension();
    if (extension != ".wal") {
      continue;
    }
    names.push_back(filename.string());
  }
  std::sort(names.begin(), names.end(), std::less<std::string>());
}

Status WAL::release_to(uint64_t index) {
  return Status::ok();
}

// 解析 wal 文件名字，文件名：0000000000000000-0000000000000000.wal
bool WAL::parse_wal_name(const std::string& name, uint64_t* seq, uint64_t* index) {
  *seq = 0;
  *index = 0;

  boost::filesystem::path path(name);
  if (path.extension() != ".wal") {
    return false;
  }

  std::string filename = name.substr(0, name.size() - 4); // 去除 .wal
  size_t pos = filename.find('-');  // 找到分界线'-'
  if (pos == std::string::npos) {
    return false;
  }

  try {
    {
      // ‘-’之前的部分作为 seq，从 str 转变为 uint64_t
      std::string str = filename.substr(0, pos);
      std::stringstream ss;
      ss << std::hex << str;
      ss >> *seq;
    }

    {
      // ‘-’之后的部分作为 index，从 str 转变为 uint64_t
      if (pos == filename.size() - 1) {
        return false;
      }
      std::string str = filename.substr(pos + 1, filename.size() - pos - 1);
      std::stringstream ss;
      ss << std::hex << str;
      ss >> *index;
    }
  } catch (...) {
    return false;
  }
  return true;
}

// 遍历所有 wal 文件名字的 seq 是否以 1 为递增，如果是，则validate，返回 true
bool WAL::is_valid_seq(const std::vector<std::string>& names) {
  uint64_t lastSeq = 0;
  for (const std::string& name: names) {
    uint64_t curSeq;
    uint64_t i;
    if (!WAL::parse_wal_name(name, &curSeq, &i)) {  // 解析 seq 和 index
      LOG_FATAL("parse correct name should never fail %s", name.c_str());
    }

    if (lastSeq != 0 && lastSeq != curSeq - 1) {  // 如果序列号不递增，则 false
      return false;
    }
    lastSeq = curSeq;
  }
  return true;
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
bool WAL::search_index(const std::vector<std::string>& names, uint64_t index, uint64_t* name_index) {

  for (size_t i = names.size() - 1; i >= 0; --i) {
    const std::string& name = names[i];
    uint64_t seq;
    uint64_t curIndex;
    if (!parse_wal_name(name, &seq, &curIndex)) {
      LOG_FATAL("invalid wal name %s", name.c_str());
    }

    if (index >= curIndex) {
      *name_index = i; // 返回等于或者大于需要搜索index的是第 i 个 wal
      return true;
    }
    if (i == 0) {
      break;
    }
  }
  *name_index = -1;
  return false;
}

}
