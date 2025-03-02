#include <raft-kv/snap/snapshotter.h>
#include <boost/filesystem.hpp>
#include <raft-kv/common/log.h>
#include <msgpack.hpp>
#include <raft-kv/raft/util.h>
#include <inttypes.h>

namespace kv {

struct SnapshotRecord {
  uint32_t data_len;
  uint32_t crc32;  // 校验
  char data[0];
};

// 根据文件路径加载最新的snapshot
Status Snapshotter::load(proto::Snapshot& snapshot) {
  std::vector<std::string> names;
  get_snap_names(names);

  for (std::string& filename : names) {
    Status status = load_snap(filename, snapshot);  // 不断加载最新的 snapshot
    if (status.is_ok()) {
      return Status::ok();
    }
  }

  return Status::not_found("snap not found");
}

// snapshot 的命名
std::string Snapshotter::snap_name(uint64_t term, uint64_t index) {
  char buffer[64];
  snprintf(buffer, sizeof(buffer), "%016" PRIx64 "-%016" PRIx64 ".snap", term, index);
  return buffer;
}

// 落盘 snapshot
Status Snapshotter::save_snap(const proto::Snapshot& snapshot) {
  Status status;
  msgpack::sbuffer sbuf;
  msgpack::pack(sbuf, snapshot);

  SnapshotRecord* record = (SnapshotRecord*) malloc(sbuf.size() + sizeof(SnapshotRecord));  // malloc snapshotRecord 的内存大小
  record->data_len = sbuf.size();
  record->crc32 = compute_crc32(sbuf.data(), sbuf.size());
  memcpy(record->data, sbuf.data(), sbuf.size());

  char save_path[128];
  // 路径/名称
  snprintf(save_path,
           sizeof(save_path),
           "%s/%s",
           dir_.c_str(),
           snap_name(snapshot.metadata.term, snapshot.metadata.index).c_str());

  FILE* fp = fopen(save_path, "w");
  if (!fp) {
    free(record);
    return Status::io_error(strerror(errno));
  }

  size_t bytes = sizeof(SnapshotRecord) + record->data_len; // 写入字节大小为SnapshotRecord结构体的大小 + record中数据的长度(pack 之后的 snapshot 的大小)
  if (fwrite((void*) record, 1, bytes, fp) != bytes) {  // 校验写入的字节是否和计算的一致
    status = Status::io_error(strerror(errno));
  }
  free(record);
  fclose(fp);

  return status;
}

// 返回所有的快照文件名字在 names 中
void Snapshotter::get_snap_names(std::vector<std::string>& names) {
  using namespace boost;

  filesystem::directory_iterator end;
  for (boost::filesystem::directory_iterator it(dir_); it != end; it++) {
    filesystem::path filename = (*it).path().filename();
    filesystem::path extension = filename.extension();
    if (extension != ".snap") {
      continue;
    }
    names.push_back(filename.string());
  }
  std::sort(names.begin(), names.end(), std::greater<std::string>());
}

// 打开文件校验快照数据是否正常，回传给 snapshot
Status Snapshotter::load_snap(const std::string& filename, proto::Snapshot& snapshot) {
  using namespace boost;
  SnapshotRecord snap_hdr;
  std::vector<char> data;
  filesystem::path path = filesystem::path(dir_) / filename;
  FILE* fp = fopen(path.c_str(), "r");

  if (!fp) {  // 文件不能打开
    goto invalid_snap;
  }

  if (fread(&snap_hdr, 1, sizeof(SnapshotRecord), fp) != sizeof(SnapshotRecord)) {  // 读取文件内容到SnapshotRecord中，并且校验读取到的字节数是否和文件内容相等
    goto invalid_snap;
  }

  if (snap_hdr.data_len == 0 || snap_hdr.crc32 == 0) {  // 检查 snap_hdr 中的 data_len 和 crc32 是否为零。
    goto invalid_snap;
  }

  data.resize(snap_hdr.data_len); // 根据 snap_hdr.data_len 分配数据缓冲区。
  if (fread(data.data(), 1, snap_hdr.data_len, fp) != snap_hdr.data_len) {  // 从文件中读取快照数据到缓冲区。如果读取失败或字节数不匹配，跳转到 invalid_snap 标签。
    goto invalid_snap;
  }

  fclose(fp);  // 关闭文件
  fp = NULL;
  if (compute_crc32(data.data(), data.size()) != snap_hdr.crc32) {  // 计算数据的校验和是否与快照中的校验和一致
    goto invalid_snap;
  }

  try {
    msgpack::object_handle oh = msgpack::unpack((const char*) data.data(), data.size());  // 使用 msgpack 库解包数据
    oh.get().convert(snapshot);  // 并转换为 proto::Snapshot 对象
    return Status::ok();

  } catch (std::exception& e) {
    goto invalid_snap;
  }

invalid_snap:  // snapshot broken
  if (fp) {
    fclose(fp);
  }
  LOG_INFO("broken snapshot %s", path.string().c_str());
  filesystem::rename(path, path.string() + ".broken");
  return Status::io_error("unexpected empty snapshot");
}

}
