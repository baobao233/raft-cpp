#include <boost/algorithm/string.hpp>
#include <raft-kv/raft/raft.h>
#include <raft-kv/common/log.h>
#include <raft-kv/common/slice.h>
#include <raft-kv/raft/util.h>

namespace kv {

static const std::string kCampaignPreElection = "CampaignPreElection";
static const std::string kCampaignElection = "CampaignElection";
static const std::string kCampaignTransfer = "CampaignTransfer";

// 统计日志配置变更数量有多少
static uint32_t num_of_pending_conf(const std::vector<proto::EntryPtr>& entries) {
  uint32_t n = 0;
  for (const proto::EntryPtr& entry: entries) {
    if (entry->type == proto::EntryConfChange) {
      n++;
    }
  }
  return n;
}

Raft::Raft(const Config &c)
    : id_(c.id),
      term_(0),
      vote_(0),
      max_msg_size_(c.max_size_per_msg),                     // limits the max byte size of each append message.
      max_uncommitted_size_(c.max_uncommitted_entries_size), // limits the aggregate byte size of the uncommitted entries that may be appended to a leader's log.
      max_inflight_(c.max_inflight_msgs),                    // limits the max number of inflight messages during optimistic replication phase.
      state_(RaftState::Follower),                           // 初始状态为 follower
      is_learner_(false),
      lead_(0),            // leader id
      lead_transferee_(0), // id of the leader transfer target when its value is not zero.
      pending_conf_index_(0),  // Only one conf change may be pending (in the log, but not yet applied) at a time.
      uncommitted_size_(0),  // an estimate of the size of the uncommitted tail of the Raft log. Used to prevent unbounded log growth.
      read_only_(new ReadOnly(c.read_only_option)),  // read only mode: 1\ ReadOnlySafe 2\ ReadOnlyLeaseBased
      election_elapsed_(0),  // number of ticks since it reached last election_elapsed_ when it is leader or candidate.
      heartbeat_elapsed_(0),  // number of ticks since it reached last heartbeat_elapsed_.
      check_quorum_(c.check_quorum), // check_quorum_ is true if the leader should check quorum activity.
      pre_vote_(c.pre_vote),  // pre_vote_ is true if the Raft node should run for pre-election.
      heartbeat_timeout_(c.heartbeat_tick),  // heartbeat_timeout_ is the number of ticks between heartbeats.
      election_timeout_(c.election_tick),  // election_timeout_ is the number of ticks between elections.
      randomized_election_timeout_(0),  // randomized_election_timeout_ is a random number between [election_timeout_, 2 * election_timeout_ - 1].
      disable_proposal_forwarding_(c.disable_proposal_forwarding),  // disable_proposal_forwarding_ stops the leader from forwarding proposals to the leader transfer target.
      random_device_(0, c.election_tick)  // random_device_ generates a random number in the range [0, election_tick).
{
  raft_log_ = std::make_shared<RaftLog>(c.storage, c.max_committed_size_per_ready);
  proto::HardState hs;
  proto::ConfState cs;
  Status status = c.storage->initial_state(hs, cs);
  if (!status.is_ok()) {
    LOG_FATAL("%s", status.to_string().c_str());
  }

  std::vector<uint64_t> peers = c.peers;
  std::vector<uint64_t> learners = c.learners;

  if (!cs.nodes.empty() || !cs.learners.empty()) {
    if (!peers.empty() || !learners.empty()) {
      // tests; the argument should be removed and these tests should be
      // updated to specify their nodes through a snapshot.
      LOG_FATAL("cannot specify both newRaft(peers, learners) and ConfState.(Nodes, Learners)");
    }
    peers = cs.nodes;
    learners = cs.learners;
  }

  // 遍历集群中的节点，为每个节点创建 Progress
  for (uint64_t peer : peers) {
    ProgressPtr p(new Progress(max_inflight_));
    p->next = 1;
    prs_[peer] = p;
  }

  // 遍历 learners 节点，节点不能同时为 learner 和 peer，并且把 learner 节点加入到 learner_prs_ 中
  for (uint64_t learner :  learners) {
    auto it = prs_.find(learner);
    if (it != prs_.end()) {
      LOG_FATAL("node %lu is in both learner and peer list", learner);
    }

    ProgressPtr p(new Progress(max_inflight_));
    p->next = 1;
    p->is_learner = true;

    learner_prs_[learner] = p;

    if (id_ == learner) {  // 自己也是 learner
      is_learner_ = true;
    }
  }

  if (!hs.is_empty_state()) {  // 如果关于当前有 term，votes 和 commit 的信息，加载状态
    load_state(hs);
  }

  // 如果重启的时候带上了已经应用在状态机上的index，则更新
  if (c.applied > 0) {
    raft_log_->applied_to(c.applied);
  }

  // 首先成为 follower
  become_follower(term_, 0);

  std::string node_str;
  {
    std::vector<std::string> nodes_strs;
    std::vector<uint64_t> node;
    this->nodes(node);  // 收集peers中的所有的节点
    for (uint64_t n : node) {
      nodes_strs.push_back(std::to_string(n));
    }
    node_str = boost::join(nodes_strs, ",");  // 将所有peers以字符串记录在 node_str 中
  }

  LOG_INFO("raft %lu [peers: [%s], term: %lu, commit: %lu, applied: %lu, last_index: %lu, last_term: %lu]",
           id_,
           node_str.c_str(),
           term_,
           raft_log_->committed_,
           raft_log_->applied_,
           raft_log_->last_index(),
           raft_log_->last_term());
}

Raft::~Raft() {

}

void Raft::become_follower(uint64_t term, uint64_t lead) {
  step_ = std::bind(&Raft::step_follower, this, std::placeholders::_1);
  reset(term); // 重置 term，拥有投票数不变，重置心跳消耗时长，选举消耗时长以及 leader 转移置零

  tick_ = std::bind(&Raft::tick_election, this); // 将 tick_ 函数指向 tick_election 函数，tick_election 函数用于一定时间后follower节点发起选举：msg->type = proto::MsgHup;

  lead_ = lead;
  state_ = RaftState::Follower;

  LOG_INFO("%lu became follower at term %lu", id_, term_);
}

void Raft::become_candidate() {
  if (state_ == RaftState::Leader) {
    LOG_FATAL("invalid transition [leader -> candidate]");
  }
  step_ = std::bind(&Raft::step_candidate, this, std::placeholders::_1);
  reset(term_ + 1);  // 变成 candidate 会使得 term 增加 1
  tick_ = std::bind(&Raft::tick_election, this); // 将 tick_ 函数指向 tick_election 函数，tick_election 函数用于一定时间后candidate节点发起选举：msg->type = proto::MsgHup;
  vote_ = id_;  // 投票给自己
  state_ = RaftState::Candidate;
  LOG_INFO("%lu became candidate at term %lu", id_, term_);
}

// 变成 pre-candidate 的时候不会让 term 增加，只是改变节点的状态和 step 函数
void Raft::become_pre_candidate() {
  // 不能从 leader 变成 candidate
  if (state_ == RaftState::Leader) {
    LOG_FATAL("invalid transition [leader -> pre-candidate]");
  }
  // Becoming a pre-candidate changes our step functions and state,
  // but doesn't change anything else. In particular it does not increase
  // r.Term or change r.Vote.
  step_ = std::bind(&Raft::step_candidate, this, std::placeholders::_1);
  votes_.clear();
  tick_ = std::bind(&Raft::tick_election, this); // 将 tick_ 函数指向 tick_election 函数，tick_election 函数用于一定时间后candidate节点发起选举：msg->type = proto::MsgHup;  由 step 函数处理 MsgHup
  lead_ = 0;
  state_ = RaftState::PreCandidate;
  LOG_INFO("%lu became pre-candidate at term %lu", id_, term_);
}

void Raft::become_leader() {
  if (state_ == RaftState::Follower) {
    LOG_FATAL("invalid transition [follower -> leader]");
  }
  step_ = std::bind(&Raft::step_leader, this, std::placeholders::_1);

  reset(term_);
  tick_ = std::bind(&Raft::tick_heartbeat, this);  // 将 tick_ 函数指向 tick_heartbeat 函数，tick_heartbeat 函数用于一定时间后leader节点与其他节点交换心跳信息：msg->type = proto::MsgBeat;
  lead_ = id_;
  state_ = RaftState::Leader;
  // Followers enter replicate mode when they've been successfully probed
  // (perhaps after having received a snapshot as a result). The leader is
  // trivially in this state. Note that r.reset() has initialized this
  // progress with the last index already.
  auto it = prs_.find(id_);
  assert(it != prs_.end());
  it->second->become_replicate();

  // Conservatively set the pendingConfIndex to the last index in the
  // log. There may or may not be a pending config change, but it's
  // safe to delay any future proposals until we commit all our
  // pending log entries, and scanning the entire tail of the log
  // could be expensive.
  pending_conf_index_ = raft_log_->last_index();

  auto empty_ent = std::make_shared<proto::Entry>();

  // 添加一条空日志，开启两阶段提交的一个同步（因为新上任的 leader 必须至少提交一条任期内的预写日志才能执行提交动作）
  if (!append_entry(std::vector<proto::Entry>{*empty_ent})) {
    // This won't happen because we just called reset() above.
    LOG_FATAL("empty entry was dropped");
  }

  // As a special case, don't count the initial empty entry towards the
  // uncommitted log quota. This is because we want to preserve the
  // behavior of allowing one entry larger than quota if the current
  // usage is zero.
  std::vector<proto::EntryPtr> entries{empty_ent};
  reduce_uncommitted_size(entries);
  LOG_INFO("%lu became leader at term %lu", id_, term_);
}

// 竞选
void Raft::campaign(const std::string& campaign_type) {
  uint64_t term = 0;
  proto::MessageType vote_msg = 0;
  if (campaign_type == kCampaignPreElection) { // 如果是预选举
    become_pre_candidate();  // 变成预选举人，只是改变其节点的状态，而不改变任期等信息
    vote_msg = proto::MsgPreVote;
    // PreVote RPCs are sent for the next term before we've incremented r.Term.
    term = term_ + 1;
  } else {
    become_candidate();  // 如果不是预选举，那么就是正常的选举，变成候选人
    vote_msg = proto::MsgVote;
    term = term_;
  }

  if (quorum() == poll(id_, vote_resp_msg_type(vote_msg), true)) {
    // We won the election after voting for ourselves (which must mean that
    // this is a single-node cluster). Advance to the next state.
    if (campaign_type == kCampaignPreElection) {
      campaign(kCampaignElection);
    } else {
      become_leader();
    }
    return;
  }

  // 广播一条竞选消息 MsgPreVote 或者 MsgVote 进行拉票
  for (auto it = prs_.begin(); it != prs_.end(); ++it) {
    if (it->first == id_) {
      continue;
    }

    LOG_INFO("%lu [log_term: %lu, index: %lu] sent %s request to %lu at term %lu",
             id_,
             raft_log_->last_term(),
             raft_log_->last_index(),
             proto::msg_type_to_string(vote_msg),
             it->first,
             term_);

    std::vector<uint8_t> ctx;
    if (campaign_type == kCampaignTransfer) {
      ctx = std::vector<uint8_t>(kCampaignTransfer.begin(), kCampaignTransfer.end());
    }

    // 带上自身节点的信息，自证自己有能力成为 leader，发送请求投票的消息
    proto::MessagePtr msg(new proto::Message());
    msg->term = term;
    msg->to = it->first;
    msg->type = vote_msg;
    msg->index = raft_log_->last_index();
    msg->log_term = raft_log_->last_term();
    msg->context = std::move(ctx);
    send(std::move(msg));
  }
}

// 别人给自己的投票结果统计
uint32_t Raft::poll(uint64_t id, proto::MessageType type, bool v) {
  uint32_t granted = 0;
  if (v) {
    LOG_INFO("%lu received %s from %lu at term %lu", id_, proto::msg_type_to_string(type), id, term_);  //赞成
  } else {
    LOG_INFO("%lu received %s rejection from %lu at term %lu", id_, proto::msg_type_to_string(type), id, term_);  //反对
  }

  auto it = votes_.find(id);  // 查找该 id 是否已经投过票
  if (it == votes_.end()) {  // 如果没有投过票，则记录投票结果，赞成或者反对
    votes_[id] = v;
  }

  for (it = votes_.begin(); it != votes_.end(); ++it) {
    if (it->second) {
      granted++;  // 统计赞成票数
    }
  }
  return granted;  // 返回赞成票数
}

// follower、candidate、leader 节点通用的一些对 msg 类型的处理，如果消息不能在满足以下条件的情况下得到处理，那么就会走到follower、candidate、leader 节点各自的 step 函数中
// 先对 term 进行判断，再对message 进行判断
Status Raft::step(proto::MessagePtr msg) {
  if (msg->term == 0) {
    
  } else if (msg->term > term_) {  // 收到了一条大于自身任期的消息
    // 在租约期内保护当前领导者，防止由于网络延迟或其他原因导致的无效选举请求扰乱集群的稳定性。
    if (msg->type == proto::MsgVote || msg->type == proto::MsgPreVote) {  // 如果是投票消息（无论是正式投票还是预投票）
      bool force = (Slice((const char*) msg->context.data(), msg->context.size()) == Slice(kCampaignTransfer));  // 如果是领导者转移，则是强制投票
      bool in_lease = (check_quorum_ && lead_ != 0 && election_elapsed_ < election_timeout_);                    // in_lease 变量用于判断当前节点是否在租约期内。租约期内意味着节点最近从当前领导者那里收到了心跳消息
      // 如果不是强制投票请求（!force）并且节点在租约期内（in_lease），则节点会忽略该投票请求。
      // 这是因为在租约期内，节点认为当前领导者仍然有效，因此不会更新其任期或授予投票。
      if (!force && in_lease) {
        // If a server receives a RequestVote request within the minimum election timeout
        // of hearing from a current leader, it does not update its term or grant its vote
        LOG_INFO(
            "%lu [log_term: %lu, index: %lu, vote: %lu] ignored %s from %lu [log_term: %lu, index: %lu] at term %lu: lease is not expired (remaining ticks: %d)",
            id_,
            raft_log_->last_term(),
            raft_log_->last_index(),
            vote_,
            proto::msg_type_to_string(msg->type),
            msg->from,
            msg->log_term,
            msg->index,
            term_,
            election_timeout_ - election_elapsed_);
        return Status::ok();
      }
    }
    // 如果是强制投票或者并不在租约内，则需要对投票进行处理
    switch (msg->type) {  // 如果不是预投票消息或者预投票回复，那么会直接变成 follower
      case proto::MsgPreVote:
        // 对于 MsgPreVote 消息，节点不会改变其当前的任期。
        // 这是因为预投票（PreVote）阶段是为了在正式投票前探测是否有可能赢得选举，因此不需要立即更新任期。
        // Never change our term in response to a PreVote
        break;
      case proto::MsgPreVoteResp:
        if (!msg->reject) {
          // 如果收到的是 MsgPreVoteResp（预投票响应）消息，并且该消息没有被拒绝（!msg->reject），则表示预投票请求被接受。
          // 在这种情况下，节点会在获得法定人数（quorum）时增加其任期。如果预投票被拒绝，节点会从拒绝投票的节点中获取新的任期，并在该任期下成为跟随者。
              // We send pre-vote requests with a term in our future. If the
              // pre-vote is granted, we will increment our term when we get a
              // quorum. If it is not, the term comes from the node that
              // rejected our vote so we should become a follower at the new
              // term.
            break;
        }
      default:LOG_INFO("%lu [term: %lu] received a %s message with higher term from %lu [term: %lu]",
                       id_, term_,
                       proto::msg_type_to_string(msg->type),
                       msg->from,
                       msg->term);
        // 更新其任期并将其状态更改为跟随者，同时记录消息发送者为领导者。
        if (msg->type == proto::MsgApp || msg->type == proto::MsgHeartbeat || msg->type == proto::MsgSnap) {
          become_follower(msg->term, msg->from);
        } else { // 对于其他类型的消息，节点也会调用 become_follower，但不记录领导者（传入0）。
          become_follower(msg->term, 0);
        }
    }
  } else if (msg->term < term_) {
    if ((check_quorum_ || pre_vote_) && (msg->type == proto::MsgHeartbeat || msg->type == proto::MsgApp)) {  // 处理低任期的心跳和日志追加消息
      // 如果 check_quorum_ 或 pre_vote_ 为真，并且消息类型是 MsgHeartbeat（心跳）或 MsgApp（日志追加），则节点会发送一个 MsgAppResp（日志追加响应）消息给发送者。
      // 这种情况下，可能是因为网络延迟导致的消息滞后，或者节点在网络分区期间增加了其任期，导致无法赢得选举或重新加入多数派。
      // 通过发送 MsgAppResp，节点通知领导者其仍然活跃，避免因滞后消息导致的任期不必要增加。

      // We have received messages from a leader at a lower term. It is possible
      // that these messages were simply delayed in the network, but this could
      // also mean that this node has advanced its term number during a network
      // partition, and it is now unable to either win an election or to rejoin
      // the majority on the old term. If checkQuorum is false, this will be
      // handled by incrementing term numbers in response to MsgVote with a
      // higher term, but if checkQuorum is true we may not advance the term on
      // MsgVote and must generate other messages to advance the term. The net
      // result of these two features is to minimize the disruption caused by
      // nodes that have been removed from the cluster's configuration: a
      // removed node will send MsgVotes (or MsgPreVotes) which will be ignored,
      // but it will not receive MsgApp or MsgHeartbeat, so it will not create
      // disruptive term increases, by notifying leader of this node's activeness.
      // The above comments also true for Pre-Vote
      //
      // When follower gets isolated, it soon starts an election ending
      // up with a higher term than leader, although it won't receive enough
      // votes to win the election. When it regains connectivity, this response
      // with "pb.MsgAppResp" of higher term would force leader to step down.
      // However, this disruption is inevitable to free this stuck node with
      // fresh election. This can be prevented with Pre-Vote phase.
      proto::MessagePtr m(new proto::Message());
      m->to = msg->from;
      m->type = proto::MsgAppResp;
      send(std::move(m));
    } else if (msg->type == proto::MsgPreVote) { // 收到了一条更小任期的 PreVote 
      // 如果消息类型是 MsgPreVote，节点会拒绝该请求，
      // 并发送 MsgPreVoteResp（预投票响应）消息，标记为拒绝。
      // Before Pre-Vote enable, there may have candidate with higher term,
      // but less log. After update to Pre-Vote, the cluster may deadlock if
      // we drop messages with a lower term.
      LOG_INFO(
          "%lu [log_term: %lu, index: %lu, vote: %lu] rejected %s from %lu [log_term: %lu, index: %lu] at term %lu",
          id_,
          raft_log_->last_term(),
          raft_log_->last_index(),
          vote_,
          proto::msg_type_to_string(msg->type),
          msg->from,
          msg->log_term,
          msg->index,
          term_);
      proto::MessagePtr m(new proto::Message());
      m->to = msg->from;
      m->type = proto::MsgPreVoteResp;
      m->reject = true;
      m->term = term_;
      send(std::move(m));
    } else {
      // ignore other cases
      LOG_INFO("%lu [term: %lu] ignored a %s message with lower term from %lu [term: %lu]",
               id_, term_, proto::msg_type_to_string(msg->type), msg->from, msg->term);
    }
    return Status::ok();
  }

  // 处理其他类型的消息
  switch (msg->type) {
    case proto::MsgHup: {
      if (state_ != RaftState::Leader) {  // 当前的状态不是 leader
        std::vector<proto::EntryPtr> entries;  // 取 applied 到 committed 的 entries 出来
        Status status =
            raft_log_->slice(raft_log_->applied_ + 1,
                             raft_log_->committed_ + 1,
                             RaftLog::unlimited(),
                             entries);
        if (!status.is_ok()) {
          LOG_FATAL("unexpected error getting unapplied entries (%s)", status.to_string().c_str());
        }

        uint32_t pending = num_of_pending_conf(entries);  // 统计applies 到 committed 的日志中变更的日志数量
        if (pending > 0 && raft_log_->committed_ > raft_log_->applied_) {  // 如果存在配置变更的日志，此时不能发起选举
          LOG_WARN(
              "%lu cannot campaign at term %lu since there are still %u pending configuration changes to apply",
              id_,
              term_,
              pending);
          return Status::ok();
        }
        LOG_INFO("%lu is starting a new election at term %lu", id_, term_);
        // follower收到自己发的 MsgHup 消息后根据预选剧标识表示发起的是预选举还是正式选举
        if (pre_vote_) { // 预选举
          campaign(kCampaignPreElection);
        } else {  // 正式选举
          campaign(kCampaignElection);
        }
      } else {
        LOG_DEBUG("%lu ignoring MsgHup because already leader", id_);
      }
      break;
    }
    case proto::MsgVote:
    case proto::MsgPreVote: {
      if (is_learner_) {  // 如果是 learner，则不参与投票
        // TODO: learner may need to vote, in case of node down when confchange.
        LOG_INFO(
            "%lu [log_term: %lu, index: %lu, vote: %lu] ignored %s from %lu [log_term: %lu, index: %lu] at term %lu: learner can not vote",
            id_,
            raft_log_->last_term(),
            raft_log_->last_index(),
            vote_,
            proto::msg_type_to_string(msg->type),
            msg->from,
            msg->log_term,
            msg->index,
            msg->term);
        return Status::ok();
      }
      // We can vote if this is a repeat of a vote we've already cast...
      bool can_vote = 
          (vote_ == msg->from) || // 重复的投票请求
          // ...we haven't voted and we don't think there's a leader yet in this term...
          (vote_ == 0 && lead_ == 0) ||  // 没有投票给该节点并且当前没有 leader
          // ...or this is a PreVote for a future term...
          (msg->type == proto::MsgPreVote && msg->term > term_);  // 针对未来任期的预投票请求
      // ...and we believe the candidate is up to date.
      if (can_vote && this->raft_log_->is_up_to_date(msg->index, msg->log_term)) { // 如果节点可以投票，并且认为候选人的日志是最新的
        LOG_INFO(
            // 节点会记录一条日志，说明它投票给了请求者。
            "%lu [log_term: %lu, index: %lu, vote: %lu] cast %s for %lu [log_term: %lu, index: %lu] at term %lu",
            id_,
            raft_log_->last_term(),
            raft_log_->last_index(),
            vote_,
            proto::msg_type_to_string(msg->type),
            msg->from,
            msg->log_term,
            msg->index,
            term_);
        // When responding to Msg{Pre,}Vote messages we include the term
        // from the message, not the local term. To see why consider the
        // case where a single node was previously partitioned away and
        // it's local term is now of date. If we include the local term
        // (recall that for pre-votes we don't update the local term), the
        // (pre-)campaigning node on the other end will proceed to ignore
        // the message (it ignores all out of date messages).
        // The term in the original message and current local term are the
        // same in the case of regular votes, but different for pre-votes.

        proto::MessagePtr m(new proto::Message());
        m->to = msg->from;
        m->term = msg->term;
        m->type = vote_resp_msg_type(msg->type);  // 回复 PreVoteResp 或者 VoteResp
        send(std::move(m));

        if (msg->type == proto::MsgVote) {
          // Only record real votes.
          election_elapsed_ = 0;  // 重置选举时间
          vote_ = msg->from;  //  如果是正式投票则记录投票给请求者
        }
      } else {  // 不允许投票或者 candidate 日志并非最新时
        LOG_INFO(
            "%lu [log_term: %lu, index: %lu, vote: %lu] rejected %s from %lu [log_term: %lu, index: %lu] at term %lu",
            id_,
            raft_log_->last_term(),
            raft_log_->last_index(),
            vote_,
            proto::msg_type_to_string(msg->type),
            msg->from,
            msg->log_term,
            msg->index,
            term_);

        proto::MessagePtr m(new proto::Message());
        m->to = msg->from;
        m->term = term_;
        m->type = vote_resp_msg_type(msg->type);
        m->reject = true;
        send(std::move(m));
      }

      break;
    }
    default: {  // 其他非投票消息消息，走到各自的 step 函数中具体处理
      return step_(msg);
    }
  }

  return Status::ok();
}

// leader 节点的处理信息的逻辑
Status Raft::step_leader(proto::MessagePtr msg) {
  // These message types do not require any progress for m.From.
  switch (msg->type) {
    case proto::MsgBeat: {  
      bcast_heartbeat();  // 广播心跳信息到集群中，确保集群中的所有节点都知道领导者依然活跃。
      return Status::ok();
    }
    case proto::MsgCheckQuorum:
      if (!check_quorum_active()) {
        LOG_WARN("%lu stepped down to follower since quorum is not active", id_);
        become_follower(term_, 0);  // 变回 follower ，无 leader 状态
      }
      return Status::ok();
    case proto::MsgProp: {  // 接收到一笔写请求
      // 如果预写日志为空，或者当前 leader 不再是配置中的一员时候，该提议被丢弃
      if (msg->entries.empty()) {
        LOG_FATAL("%lu stepped empty MsgProp", id_);
      }
      auto it = prs_.find(id_);  // 查看 progress 中有没有自己的 id，如果没有可能是自己作为 leader 被移除了
      if (it == prs_.end()) {
        // If we are not currently a member of the range (i.e. this node
        // was removed from the configuration while serving as leader),
        // drop any new proposals.
        return Status::invalid_argument("raft proposal dropped");
      }

      // 领导者正在进行领导转移，那么该提议将被丢弃
      if (lead_transferee_ != 0) {
        LOG_DEBUG("%lu [term %lu] transfer leadership to %lu is in progress; dropping proposal",
                  id_,
                  term_,
                  lead_transferee_);
        return Status::invalid_argument("raft proposal dropped");
      }

      for (size_t i = 0; i < msg->entries.size(); ++i) {
        proto::Entry& e = msg->entries[i];
        if (e.type == proto::EntryConfChange) {  // 如果是配置变更消息
          if (pending_conf_index_ > raft_log_->applied_) {  // 已经存在配置变更日志的索引
            LOG_INFO(
                "propose conf %s ignored since pending unapplied configuration [index %lu, applied %lu]",
                proto::entry_type_to_string(e.type),
                pending_conf_index_,
                raft_log_->applied_);
            e.type = proto::EntryNormal;
            e.index = 0;
            e.term = 0;
            e.data.clear();
          } else {
            pending_conf_index_ = raft_log_->last_index() + i + 1;  // 记录配置变更索引的位置
          }
        }
      }

      // 把预写日志追加到一个结构体中
      if (!append_entry(msg->entries)) {
        return Status::invalid_argument("raft proposal dropped");
      }
      // 向其他节点广播追加日志消息
      bcast_append();
      return Status::ok();
    }
    case proto::MsgReadIndex: {  // 接收到一条读请求
      if (quorum() > 1) {  // 如果不是单节点模式，则需要至少提交一个日志才能响应请求
        uint64_t term = 0;
        raft_log_->term(raft_log_->committed_, term);
        if (term != term_) {
          return Status::ok();
        }

        // thinking: use an interally defined context instead of the user given context.
        // We can express this in terms of the term and index instead of a user-supplied value.
        // This would allow multiple reads to piggyback on the same message.
        switch (read_only_->option) {
          case ReadOnlySafe:
            read_only_->add_request(raft_log_->committed_, msg);  // 将读请求添加到 read_index_queue 中去
            bcast_heartbeat_with_ctx(msg->entries[0].data);  // context 是每条读请求的唯一标识，读请求中第一条 entry 的数据就是作为ctx 发送出去，为了自证身份自己作为 leader 可以回复消息，广播一条带有 context 的读请求标识的信息，后续收到其他节点回复 leader 也可以通过context知道这是针对哪个读请求的
            break;
          case ReadOnlyLeaseBased:  // follower 也能回复 readIndex 的设置
            if (msg->from == 0 || msg->from == id_) { // from local member
              read_states_
                  .push_back(ReadState{.index = raft_log_->committed_, .request_ctx = msg->entries[0]
                      .data});
            } else {
              proto::MessagePtr m(new proto::Message());
              m->to = msg->from;  // 转发回follower
              m->type = proto::MsgReadIndexResp;
              m->index = raft_log_->committed_;  // 携带上自己的commitIndex
              m->entries = msg->entries;
              send(std::move(m));
            }
            break;
        }
      } else { // 如果是单节点模式，则直接添加到记录到读状态中表示记录处理读请求
        read_states_.push_back(ReadState{.index = raft_log_->committed_, .request_ctx = msg->entries[0].data});
      }

      return Status::ok();
    }
  }

  // 首先leader 先获得消息来源的progress
  auto pr = get_progress(msg->from);
  if (pr == nullptr) {
    LOG_DEBUG("%lu no progress available for %lu", id_, msg->from);
    return Status::ok();
  }
  switch (msg->type) {
    case proto::MsgAppResp: {  // 接收到一条来自其他节点的日志追加响应
      pr->recent_active = true;  // 标记其为活跃节点

      if (msg->reject) {  // 如果 follower 节点或者 candidate 节点拒绝leader的日志追加请求
        LOG_DEBUG("%lu received msgApp rejection(last_index: %lu) from %lu for index %lu",
                  id_, msg->reject_hint, msg->from, msg->index);
        if (pr->maybe_decreases_to(msg->index, msg->reject_hint)) {  // 根据 reject_hint 尝试修改该节点 progress 中的 next
          LOG_DEBUG("%lu decreased progress of %lu to [%s]", id_, msg->from, pr->string().c_str());
          if (pr->state == ProgressStateReplicate) {  // 如果节点本来处于正常的复制状态，则修改为探测状态
            pr->become_probe();
          }
          send_append(msg->from);  // 重新根据调整过的该节点的 progress 发送同步请求
        }
      } else { // 如果 follower 节点或者 candidate 节点接受leader的日志追加请求
        bool old_paused = pr->is_paused();
        if (pr->maybe_update(msg->index)) { // 更新该节点 progress 的 match 位置，对应的节点更新到了什么位置，下一次发送的预写日志位置等信息。
          if (pr->state == ProgressStateProbe) {  // 如果是探测状态，则更该状态
            pr->become_replicate();  // 更新其节点能够重新成为正常的一员，next = match + 1
          } else if (pr->state == ProgressStateSnapshot && pr->need_snapshot_abort()) {  // 该节点已经根据快照跟上了entry 的 index
            LOG_DEBUG("%lu snapshot aborted, resumed sending replication messages to %lu [%s]",
                      id_,
                      msg->from,
                      pr->string().c_str());
            // Transition back to replicating state via probing state
            // (which takes the snapshot into account). If we didn't
            // move to replicating state, that would only happen with
            // the next round of appends (but there may not be a next
            // round for a while, exposing an inconsistent RaftStatus).
            // 通过探测状态（考虑快照）转换回复制状态。如果我们不转移到复制状态，那只会在下一轮追加时发生（但可能暂时不会有下一轮，从而暴露出不一致的RaftStatus）。
            pr->become_probe();
          } else if (pr->state == ProgressStateReplicate) {
            pr->inflights->free_to(msg->index);  // 调整该 progress 的接收窗口，释放该 index 之前的 message 占用的 buffer 位置
          }

          if (maybe_commit()) {  // 通过检查 leader 维护的 progress 中每个 follower 的进度，如果 leader 可以有新的 commit Index 的进度，则进行广播
            bcast_append();
          } else if (old_paused) {  // 如果该节点因为快照/接受窗口等原因被 pause 了
            // If we were paused before, this node may be missing the
            // latest commit index, so send it.
            send_append(msg->from);
          }
          // We've updated flow control information above, which may
          // allow us to send multiple (size-limited) in-flight messages
          // at once (such as when transitioning from probe to
          // replicate, or when freeTo() covers multiple messages). If
          // we have more entries to send, send as many messages as we
          // can (without sending empty messages for the commit index)
          // 我们更新了上面的流程控制信息，这样我们就可以一次发送多条（大小受限的）飞行中消息（例如从探测到复制的转换，或者freeTo()覆盖多条消息）。如果我们有更多条目要发送，请尽可能发送多条消息（不要为提交索引发送空消息）。
          while (maybe_send_append(msg->from, false)) {
          }
          // 收到了来自 leader 受让人的信息，并且其 match 进度已经赶上，则发消息给 leader 受让人使其发起选举
          if (msg->from == lead_transferee_ && pr->match == raft_log_->last_index()) {
            LOG_INFO("%lu sent MsgTimeoutNow to %lu after received MsgAppResp", id_, msg->from);
            send_timeout_now(msg->from);
          }
        }
      }
    }
      break;
    // leader 接收到其余节点对“我希望回复 ReadIndex 读请求”的表态
    case proto::MsgHeartbeatResp: {
      pr->recent_active = true;
      pr->resume();

      // free one slot for the full inflights window to allow progress.
      if (pr->state == ProgressStateReplicate && pr->inflights->is_full()) {  // 如果接收窗口已经满了，则释放其窗口的第一个 entry index， 因为发送这个 ctx 消息占用了一个 inflight
        pr->inflights->free_first_one();
      }
      if (pr->match < raft_log_->last_index()) {
        send_append(msg->from);
      }

      if (read_only_->option != ReadOnlySafe || msg->context.empty()) {
        return Status::ok();
      }

      uint32_t ack_count = read_only_->recv_ack(*msg); // 通过判断这个消息有没有带上 context，统计其余节点的回应数量
      if (ack_count < quorum()) {  // 如果没有达成多数派，则返回不往下走了
        return Status::ok();
      }

      // 如果已经达成多数派协议，则通过 read_only 模块判断读请求队列中该 ctx 之前有哪些读请求，可以批量回复该读请求之前的所有读请求（并且擦除该 queue 和 map 中的这些读请求）  
      auto rss = read_only_->advance(*msg);
      for (auto& rs : rss) {  // 遍历这部分读请求进度状态
        auto& req = rs->req;  // 在状态中取出请求体
        if (req.from == 0 || req.from == id_) {  // 从应用层传来的消息没有 from
          ReadState read_state = ReadState{.index = rs->index, .request_ctx = req.entries[0].data}; 
          read_states_.push_back(std::move(read_state));  // 记录已经处理读请求
        } else {
          proto::MessagePtr m(new proto::Message());
          m->to = req.from;
          m->type = proto::MsgReadIndexResp; // ps：读取数据的行为是在应用层给客户端响应，这里只是发送一则MsgReadIndexResp类型的信息给应用层（req 是从应用层来的，所以发送的目的地to 就是 from）
          m->index = rs->index;
          m->entries = req.entries;
          send(std::move(m));
        }
      }
    }
      break;
      // 接收到来自raftNode.report_snapshot汇报来自某 id 的快照状态
      case proto::MsgSnapStatus: {
        if (pr->state != ProgressStateSnapshot)
        {
          return Status::ok();
        }
        if (!msg->reject)  //  snapshot 没有失败
        {
          pr->become_probe(); // 重新从 ProgressStateSnapshot 状态变回 ProgressStateProbe
          LOG_DEBUG("%lu snapshot succeeded, resumed sending replication messages to %lu [%s]",
                    id_,
                    msg->from,
                    pr->string().c_str());
        } else {  // snapshot 失败
        pr->snapshot_failure();
        pr->become_probe(); // 重新从 ProgressStateSnapshot 状态变回 ProgressStateProbe
        LOG_DEBUG("%lu snapshot failed, resumed sending replication messages to %lu [%s]",
                  id_,
                  msg->from,
                  pr->string().c_str());
      }
      // If snapshot finish, wait for the msgAppResp from the remote node before sending
      // out the next msgApp.
      // If snapshot failure, wait for a heartbeat interval before next try
      pr->set_pause();  // 汇报完某节点的快照状态后，需要把该节点设置为 pause，然后通过下一次的 msgApp 或者 heartbeat 重新将其变回活跃或者重新快照
      break;
    }
    // 接收到来自 raftNode.report_unreachable 的消息不可达类型的消息
    case proto::MsgUnreachable: {
      // 在乐观复制过程中，如果远程无法访问，则MsgApp丢失的可能性很大。
      if (pr->state == ProgressStateReplicate) {
        pr->become_probe();
      }
      LOG_DEBUG("%lu failed to send message to %lu because it is unreachable [%s]", id_,
                msg->from,
                pr->string().c_str());
      break;
    }
    // leader 收到来自应用层调用transfer_leadership函数转移 leader 的消息
    case proto::MsgTransferLeader: { 
      if (pr->is_learner) { // 如果指定的新leader 是一个 learner，则直接返回，不转移
        LOG_DEBUG("%lu is learner. Ignored transferring leadership", id_);
        return Status::ok();
      }

      uint64_t lead_transferee = msg->from;
      uint64_t last_lead_transferee = lead_transferee_;  // 自身维护的 leader 受让人

      if (last_lead_transferee != 0) { // 判断是否是来自同一个 leader 受让人的转让 message，如果是，则忽略
        if (last_lead_transferee == lead_transferee) {  
          LOG_INFO(
              "%lu [term %lu] transfer leadership to %lu is in progress, ignores request to same node %lu",
              id_,
              term_,
              lead_transferee,
              lead_transferee);
          return Status::ok();
        }
        abort_leader_transfer();  // 重置leader 受让人为 0
        LOG_INFO("%lu [term %lu] abort previous transferring leadership to %lu",
                 id_,
                 term_,
                 last_lead_transferee);
      }

      if (lead_transferee == id_) {  // leader 受让人和当前我自己是同一个人，则不作响应直接返回
        LOG_DEBUG("%lu is already leader. Ignored transferring leadership to self", id_);
        return Status::ok();
      }

      // 转移leader 给 leader 受让人逻辑
      LOG_INFO("%lu [term %lu] starts to transfer leadership to %lu", id_, term_, lead_transferee);
      // Transfer leadership should be finished in one electionTimeout, so reset r.electionElapsed.
      election_elapsed_ = 0;
      lead_transferee_ = lead_transferee;
      if (pr->match == raft_log_->last_index()) {  // 受让人已经拥有全部最新日志，可以直接转让
        send_timeout_now(lead_transferee);
        LOG_INFO("%lu sends MsgTimeoutNow to %lu immediately as %lu already has up-to-date log",
                 id_,
                 lead_transferee,
                 lead_transferee);
      } else {
        send_append(lead_transferee);  //根据受让人的 progress 发送给受让人未补齐的预写日志
      }
      break;
    }
  }
  return Status::ok();
}

Status Raft::step_candidate(proto::MessagePtr msg) {
  // Only handle vote responses corresponding to our candidacy (while in
  // StateCandidate, we may get stale MsgPreVoteResp messages in this term from
  // our pre-candidate state).
  switch (msg->type) {
    // 收到 MsgProp 消息，因为当前是candidate 状态，之所以成为 candidate 状态是因为当前种群肯定是个无主状态，因此会直接丢弃，
    case proto::MsgProp:
      LOG_INFO("%lu no leader at term %lu; dropping proposal", id_, term_);
      return Status::invalid_argument("raft proposal dropped");
    // 收到 MsgApp 消息，说明有节点已经是 leader，所以自动退位
    case proto::MsgApp:
      become_follower(msg->term, msg->from); // term 和 leader 由 发送过来的 msg 指定
      handle_append_entries(std::move(msg));  // 判断自己是否要追加日志
      break;
    case proto::MsgHeartbeat:  // 接收到 leader 希望回复 ReadIndex 的消息，自动退位
      become_follower(msg->term, msg->from);  // always m.Term == r.Term
      handle_heartbeat(std::move(msg));
      break;
    case proto::MsgSnap:  // 收到 leader 发来的快照，自动退位
      become_follower(msg->term, msg->from); // always m.Term == r.Term
      handle_snapshot(std::move(msg));
      break;
    case proto::MsgPreVoteResp:
    case proto::MsgVoteResp: {  // Candidate处理投票结果
      uint64_t gr = poll(msg->from, msg->type, !msg->reject);  // 返回投票赞成的数量
      LOG_INFO("%lu [quorum:%u] has received %lu %s votes and %lu vote rejections",
               id_,
               quorum(),
               gr,
               proto::msg_type_to_string(msg->type),
               votes_.size() - gr);
      if (quorum() == gr) {  // 达到半数以上的赞成票数成为候选人
        if (state_ == RaftState::PreCandidate) {  // 如果当前节点状态是预选举状态
          campaign(kCampaignElection);  // 则发起正式选举
        } else {  //  如果是正式选举状态则成为leader
          assert(state_ == RaftState::Candidate);
          become_leader();  // 成为 leader
          bcast_append();  // 广播
        }
      } else if (quorum() == votes_.size() - gr) {  // 如果当前节点收到的反对票数等于半数以上的节点
        // pb.MsgPreVoteResp contains future term of pre-candidate
        // m.Term > r.Term; reuse r.Term
        become_follower(term_, 0);  // 收到多数派拒绝，自动退位，并且 term 不变
      }
      break;
    }
    // candidate 收到领导人转让的 TimeoutNow 消息时，直接忽略
    case proto::MsgTimeoutNow: {
      LOG_DEBUG("%lu [term %lu state %d] ignored MsgTimeoutNow from %lu",
                id_,
                term_,
                state_,
                msg->from);
    }
  }
  return Status::ok();
}

// 发送给应用层让应用层执行通信模块
void Raft::send(proto::MessagePtr msg) {
  msg->from = id_;
  // 对消息的校验，不同类型的消息有能不能携带term的限制；如果是投票类型的 req 或者 resp 消息，term 必须是非 0 的，此外的消息类型，term 必须是 0
  if (msg->type == proto::MsgVote || msg->type == proto::MsgVoteResp || msg->type == proto::MsgPreVote
      || msg->type == proto::MsgPreVoteResp) {  // 投票类信息
    if (msg->term == 0) {
      // All {pre-,}campaign messages need to have the term set when
      // sending.
      // - MsgVote: m.Term is the term the node is campaigning for,
      //   non-zero as we increment the term when campaigning.
      // - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
      //   granted, non-zero for the same reason MsgVote is
      // - MsgPreVote: m.Term is the term the node will campaign,
      //   non-zero as we use m.Term to indicate the next term we'll be
      //   campaigning for
      // - MsgPreVoteResp: m.Term is the term received in the original
      //   MsgPreVote if the pre-vote was granted, non-zero for the
      //   same reasons MsgPreVote is
      LOG_FATAL("term should be set when sending %s", proto::msg_type_to_string(msg->type));

    }
  } else {  // 非投票类信息
    if (msg->term != 0) {
      LOG_FATAL("term should not be set when sending %d (was %lu)", msg->type, msg->term);
    }
    // do not attach term to MsgProp, MsgReadIndex
    // proposals are a way to forward to the leader and
    // should be treated as local message.
    // MsgReadIndex is also forwarded to leader.
    if (msg->type != proto::MsgProp && msg->type != proto::MsgReadIndex) {
      msg->term = term_;
    }
  }
  msgs_.push_back(std::move(msg));
}

// 根据快照中的节点配置信息重新更新每个节点的 progress 信息；需要区分正常节点和 follower 节点
void Raft::restore_node(const std::vector<uint64_t>& nodes, bool is_learner) {
  for (uint64_t node: nodes) {
    uint64_t match = 0;  // 恢复的时候初始 match 为 0
    uint64_t next = raft_log_->last_index() + 1;
    if (node == id_) {
      match = next - 1;
      is_learner_ = is_learner;
    }
    set_progress(node, match, next, is_learner);
    LOG_INFO("%lu restored progress of %lu [%s]", id_, node, get_progress(id_)->string().c_str());
  }
}

// 如果能在 progress 中找到自己，则说明该节点是可 promote 的
bool Raft::promotable() const {
  auto it = prs_.find(id_);
  return it != prs_.end();
}

// 根据is_learner 决定添加新节点应该是node或者learner
void Raft::add_node_or_learner(uint64_t id, bool is_learner) {
  ProgressPtr pr = get_progress(id);
  if (pr == nullptr) {  // 新节点
    set_progress(id, 0, raft_log_->last_index() + 1, is_learner);
  } else {  // 旧节点

    if (is_learner && !pr->is_learner) {  // 如果尝试把正常节点变为 learner，会 ignore
      // can only change Learner to Voter
      LOG_INFO("%lu ignored addLearner: do not support changing %lu from raft peer to learner.", id_, id);
      return;
    }

    if (is_learner == pr->is_learner) {  // 如果旧节点的状态和新设置的状态一致，则也 ignore
      // Ignore any redundant addNode calls (which can happen because the
      // initial bootstrapping entries are applied twice).
      return;
    }

    // learner -> 正常节点
    learner_prs_.erase(id);  
    pr->is_learner = false;
    prs_[id] = pr;
  }

  if (id_ == id) {  // 自己是否为 learner 的状态更改
    is_learner_ = is_learner;
  }

  // When a node is first added, we should mark it as recently active.
  // Otherwise, CheckQuorum may cause us to step down if it is invoked
  // before the added node has a chance to communicate with us.
  get_progress(id)->recent_active = true;
}

void Raft::remove_node(uint64_t id) {
  del_progress(id);  // 从 progress 和 learner 中删除该 id

  // do not try to commit or abort transferring if there is no nodes in the cluster.
  if (prs_.empty() && learner_prs_.empty()) {
    return;
  }

  // The quorum size is now smaller, so see if any pending entries can
  // be committed.
  if (maybe_commit()) { // 集群数量变少，重新检查是否可以提交一些日志
    bcast_append();
  }
  // 如果当前节点是 leader 并且删除的节点是 leader 受让人，则取消领导转让
  if (state_ == RaftState::Leader && lead_transferee_ == id) {
    abort_leader_transfer();  // 令当前记节点的领导受让人重置 0
  }
}

Status Raft::step_follower(proto::MessagePtr msg) {
  switch (msg->type) {
    case proto::MsgProp:
      // 如果无主或者不允许转发则直接返回参数不合法
      if (lead_ == 0) {
        LOG_INFO("%lu no leader at term %lu; dropping proposal", id_, term_);
        return Status::invalid_argument("raft proposal dropped");
      } else if (disable_proposal_forwarding_) {
        LOG_INFO("%lu not forwarding to leader %lu at term %lu; dropping proposal", id_, lead_, term_);
        return Status::invalid_argument("raft proposal dropped");
      }
      msg->to = lead_;  // 直接转发给 leader
      send(msg);  // 将消息发送给leader
      break;
    // follower 收到来自 leader 的追加日志消息时，处理追加预写日志后，回应leader的追加请求
    case proto::MsgApp: {  
      election_elapsed_ = 0;  // 重置选举时间
      lead_ = msg->from;
      handle_append_entries(msg);
      break;
    }
    case proto::MsgHeartbeat: { // 收到了leader处理读请求前为了自证身份的心跳消息，follower 可以借此更新自己的 commit index和回应上下文
      election_elapsed_ = 0;
      lead_ = msg->from;
      handle_heartbeat(msg);
      break;
    }
    case proto::MsgSnap: {  // 接收到来自 leader 请求同步 snapshot 的消息
      election_elapsed_ = 0;
      lead_ = msg->from;
      handle_snapshot(msg);
      break;
    }
    case proto::MsgTransferLeader:  // 收到转让领导权的消息。转发给leader 让leader 处理
      if (lead_ == 0) {
        LOG_INFO("%lu no leader at term %lu; dropping leader transfer msg", id_, term_);
        return Status::ok();
      }
      msg->to = lead_;
      send(msg);
      break;
    case proto::MsgTimeoutNow: // leader收到应用层发来的MsgTransferLeader消息的时候，会给follower发送MsgTimeoutNow消息，进行领导权转让
      if (promotable()) {  // 如果能在 progress 中找到该节点我们就认为该节点是 promotable 的
        LOG_INFO("%lu [term %lu] received MsgTimeoutNow from %lu and starts an election to get leadership.",
                 id_,
                 term_,
                 msg->from);
        // Leadership transfers never use pre-vote even if r.preVote is true; we
        // know we are not recovering from a partition so there is no need for the
        // extra round trip.
        campaign(kCampaignTransfer);  // 直接发起正式选举，跳过预选举
      } else {
        LOG_INFO("%lu received MsgTimeoutNow from %lu but is not promotable", id_, msg->from);
      }
      break;
    case proto::MsgReadIndex:  // 收到读请求，转发给 leader 进行处理
      if (lead_ == 0) {
        LOG_INFO("%lu no leader at term %lu; dropping index reading msg", id_, term_);
        return Status::ok();
      }
      msg->to = lead_;
      send(msg);
      break;
    case proto::MsgReadIndexResp: // 在 ReadOnlyLeaseBased 状态下，follower会收到来自 leader 发回来的 ReadIndexResp表示让 follower 处理这个读请求
      if (msg->entries.size() != 1) {
        LOG_ERROR("%lu invalid format of MsgReadIndexResp from %lu, entries count: %lu",
                  id_,
                  msg->from,
                  msg->entries.size());
        return Status::ok();
      }
      ReadState rs;
      rs.index = msg->index;
      rs.request_ctx = std::move(msg->entries[0].data);
      read_states_.push_back(std::move(rs));  // 记录处理读请求的状态
      break;
  }
  return Status::ok();
}

// candidate 或者 follower 收到 MsgApp 消息时的处理逻辑
void Raft::handle_append_entries(proto::MessagePtr msg) {
  // 如果消息中夹带的前一笔日志index小于当前节点的commit index，则回应leader的消息，告诉leader当前自身的commit index
  if (msg->index < raft_log_->committed_) {
    proto::MessagePtr m(new proto::Message());
    m->to = msg->from;
    m->type = proto::MsgAppResp;
    m->index = raft_log_->committed_;
    send(std::move(m));
    return;
  }

  // 如果消息的index大于当前节点的commit index，首先先用 vector 保存消息中送来的 entry
  std::vector<proto::EntryPtr> entries;
  for (proto::Entry& entry: msg->entries) {
    entries.push_back(std::make_shared<proto::Entry>(std::move(entry)));
  }

  bool ok = false;
  uint64_t last_index = 0;  // 尝试追加后的最后 commit index
  // leader发来的预写日志会带上前一笔日志的 term 和 index，也就是 log_term 和 index
  raft_log_->maybe_append(msg->index, msg->log_term, msg->commit, std::move(entries), last_index, ok);

  if (ok) {  // 如果追加成功，则回应leader的消息，index 为 该节点的last_index
    proto::MessagePtr m(new proto::Message());
    m->to = msg->from;
    m->type = proto::MsgAppResp;
    m->index = last_index;
    send(std::move(m));
  } else {  // 如果追加不成功，就说明这个 entry 的 term 或者 index不匹配，回应leader的消息，设置 MsgAppResp 的 reject 为 true 并且在 reject_hint 中表明该节点的预写日志中的最后一笔索引 last_index是多少，希望leader能够补齐该节点和 leader 之间的这个差距
    uint64_t term = 0;
    raft_log_->term(msg->index, term);
    LOG_DEBUG("%lu [log_term: %lu, index: %lu] rejected msgApp [log_term: %lu, index: %lu] from %lu",
              id_, term, msg->index, msg->log_term, msg->index, msg->from)

    proto::MessagePtr m(new proto::Message());
    m->to = msg->from;
    m->type = proto::MsgAppResp;
    m->index = msg->index;  // 拒绝日志的index
    m->reject = true;
    m->reject_hint = raft_log_->last_index();  // 夹带上我这个节点最后一条日志的 index
    send(std::move(m));
  }

}

void Raft::handle_heartbeat(proto::MessagePtr msg) {
  raft_log_->commit_to(msg->commit);  // 更新commit index
  proto::MessagePtr m(new proto::Message());
  m->to = msg->from;
  m->type = proto::MsgHeartbeatResp;
  msg->context = std::move(msg->context);  // 回应上下文
  send(std::move(m));
}

void Raft::handle_snapshot(proto::MessagePtr msg) {
  uint64_t sindex = msg->snapshot.metadata.index;
  uint64_t sterm = msg->snapshot.metadata.term;

  // 判断是否根据 leader 发来的 snapshot 重新恢复，如果是则回应 leader
  if (restore(msg->snapshot)) {
    LOG_INFO("%lu [commit: %lu] restored snapshot [index: %lu, term: %lu]",
             id_, raft_log_->committed_, sindex, sterm);
    proto::MessagePtr m(new proto::Message());
    m->to = msg->from;
    m->type = proto::MsgAppResp;
    msg->index = raft_log_->last_index();    // 从 unstable 或者 storage 中获得最后一条日志的 index
    send(std::move(m));
  } else {  // 1、snapshot 提供的 index 是我已经 commit 的，直接忽略；2、并不是我 follower 丢失数据，而是说我的日志里面有你 snapshot 发来的日志，但没有来得及commit而已
    LOG_INFO("%lu [commit: %lu] ignored snapshot [index: %lu, term: %lu]",
             id_, raft_log_->committed_, sindex, sterm);
    proto::MessagePtr m(new proto::Message());
    m->to = msg->from;
    m->type = proto::MsgAppResp;
    msg->index = raft_log_->committed_;  // 回复我当前的 commit index
    send(std::move(m));
  }

}

// 根据快照的 term 和 index 恢复数据
bool Raft::restore(const proto::Snapshot& s) {
  if (s.metadata.index <= raft_log_->committed_) {
    return false;
  }

  // 如果follower 节点能在raft_log_日志模块中找到该snapshot 的 term 和 index，则直接在该节点上 commit 到该 index
  if (raft_log_->match_term(s.metadata.index, s.metadata.term)) {
    LOG_INFO(
        "%lu [commit: %lu, last_index: %lu, last_term: %lu] fast-forwarded commit to snapshot [index: %lu, term: %lu]",
        id_,
        raft_log_->committed_,
        raft_log_->last_index(),
        raft_log_->last_term(),
        s.metadata.index,
        s.metadata.term);
    raft_log_->commit_to(s.metadata.index);
    return false;
  }

  // The normal peer can't become learner.
  if (!is_learner_) {
    for (uint64_t id : s.metadata.conf_state.learners) {
      if (id == id_) {
        LOG_ERROR("%lu can't become learner when restores snapshot [index: %lu, term: %lu]",
                  id_,
                  s.metadata.index,
                  s.metadata.term);
        return false;
      }

    }
  }

  // 以下逻辑执行真正的恢复数据
  LOG_INFO("%lu [commit: %lu, last_index: %lu, last_term: %lu] starts to restore snapshot [index: %lu, term: %lu]",
           id_,
           raft_log_->committed_,
           raft_log_->last_index(),
           raft_log_->last_term(),
           s.metadata.index,
           s.metadata.term);

  proto::SnapshotPtr snap(new proto::Snapshot(s));
  raft_log_->restore(snap); // 1、在应用层的 unstable 中储存快照信息；2、更新commit index
  prs_.clear();
  learner_prs_.clear();
  restore_node(s.metadata.conf_state.nodes, false);  // 重置该节点正常节点的 progress
  restore_node(s.metadata.conf_state.learners, true);  // 重置该节点learner 的 progress
  return true;
}

// 根据设置的 tick 函数调用tick()
void Raft::tick() {
  if (tick_) {
    tick_();
  } else {
    LOG_WARN("tick function is not set");
  }
}

// 软状态（当前leader 的 id， 当前状态）
SoftStatePtr Raft:: soft_state() const {
  return std::make_shared<SoftState>(lead_, state_);
}

// 硬状态（当前的 term，当前投票给了谁，当前的 commit index）
proto::HardState Raft::hard_state() const {
  proto::HardState hs;
  hs.term = term_;
  hs.vote = vote_;
  hs.commit = raft_log_->committed_;
  return hs;
}

// 加载状态
void Raft::load_state(const proto::HardState& state) {
  if (state.commit < raft_log_->committed_ || state.commit > raft_log_->last_index()) {  // 保存的状态里commit index 少于我节点自身保存的 commit index 或者大于我节点保存的最后一条预写日志索引，则不合法
    LOG_FATAL("%lu state.commit %lu is out of range [%lu, %lu]",
              id_,
              state.commit,
              raft_log_->committed_,
              raft_log_->last_index());
  }
  raft_log_->committed_ = state.commit;
  term_ = state.term;
  vote_ = state.vote;
}

// 存放在 progress 中存在的节点 id
void Raft::nodes(std::vector<uint64_t>& node) const {
  for (auto it = prs_.begin(); it != prs_.end(); ++it) {
    node.push_back(it->first);
  }
  std::sort(node.begin(), node.end());
}

// 存放 learner 的节点 id
void Raft::learner_nodes(std::vector<uint64_t>& learner) const {
  for (auto it = learner_prs_.begin(); it != prs_.end(); ++it) {
    learner.push_back(it->first);
  }
  std::sort(learner.begin(), learner.end());
}

// 先找 progress， 后找 learner
ProgressPtr Raft::get_progress(uint64_t id) {
  auto it = prs_.find(id);
  if (it != prs_.end()) {
    return it->second;
  }

  it = learner_prs_.find(id);
  if (it != learner_prs_.end()) {
    return it->second;
  }
  return nullptr;
}

// 根据id，match，next和 is_learner? 设置 progress
void Raft::set_progress(uint64_t id, uint64_t match, uint64_t next, bool is_learner) {
  if (!is_learner) {  // 设置为非 learner
    learner_prs_.erase(id);
    ProgressPtr progress(new Progress(max_inflight_));
    progress->next = next;
    progress->match = match;
    prs_[id] = progress;
    return;
  }

  // 如果设置为 learner，不能是已经存在的 progress
  auto it = prs_.find(id);
  if (it != prs_.end()) {
    LOG_FATAL("%lu unexpected changing from voter to learner for %lu", id_, id);
  }

  ProgressPtr progress(new Progress(max_inflight_));
  progress->next = next;
  progress->match = match;
  progress->is_learner = true;

  learner_prs_[id] = progress;
}

// 删除 progress
void Raft::del_progress(uint64_t id) {
  prs_.erase(id);
  learner_prs_.erase(id);
}

// leader 向 follower 发送添加日志
void Raft::send_append(uint64_t to) {
  maybe_send_append(to, true);
}

// leader 节点向 follower 节点发送 append 消息
bool Raft::maybe_send_append(uint64_t to, bool send_if_empty) {
  ProgressPtr pr = get_progress(to);
  if (pr->is_paused()) {  // 如果该 follower 节点处于 1、探测状态的时候；2、接受信息的窗口已满的时候； pause = true
    return false;
  }

  proto::MessagePtr msg(new proto::Message());
  msg->to = to;
  uint64_t term = 0;
  Status status_term = raft_log_->term(pr->next - 1, term);
  std::vector<proto::EntryPtr> entries;
  Status status_entries = raft_log_->entries(pr->next, max_msg_size_, entries);  // 从 unstable 和 storage 中获得发送给每个节点的不同 entries vector，
  if (entries.empty() && !send_if_empty) {
    return false;
  }

  // 如果一个 leader 发现progress中应该发给其他节点的日志条目已经是在应用层中被压缩的时候，需要发MsgSnap给 follower 需要其创建leader 发送过去的快照
  if (!status_term.is_ok() || !status_entries.is_ok()) {
    if (!pr->recent_active) {
      LOG_DEBUG("ignore sending snapshot to %lu since it is not recently active", to)
      return false;
    }

    msg->type = proto::MsgSnap;

    proto::SnapshotPtr snap;
    Status status = raft_log_->snapshot(snap);  // 从应用层中的持久模块中获取快照
    if (!status.is_ok()) {
      LOG_FATAL("snapshot error %s", status.to_string().c_str());
    }
    if (snap->is_empty()) {
      LOG_FATAL("need non-empty snapshot");
    }
    uint64_t sindex = snap->metadata.index;
    uint64_t sterm = snap->metadata.term;
    LOG_DEBUG("%lu [first_index: %lu, commit: %lu] sent snapshot[index: %lu, term: %lu] to %lu [%s]",
              id_, raft_log_->first_index(), raft_log_->committed_, sindex, sterm, to, pr->string().c_str());
    pr->become_snapshot(sindex);  // leader 标记该节点的 progress 状态为 ProgressStateSnapShot，并且重置其接收信息的滑动窗口 inflights
    msg->snapshot = *snap;  // msg 中夹带 snapshot
    LOG_DEBUG("%lu paused sending replication messages to %lu [%s]", id_, to, pr->string().c_str());
  } else { // 封装 MsgApp 消息发送给其他节点 使其同步预写日志
    msg->type = proto::MsgApp;
    msg->index = pr->next - 1;  // 前一条预写日志的 index
    msg->log_term = term;
    for (proto::EntryPtr& entry: entries) {
      //copy
      msg->entries.emplace_back(*entry);
    }

    msg->commit = raft_log_->committed_;  // leader 节点的 commit index
    if (!msg->entries.empty()) {
      switch (pr->state) {
        // optimistically increase the next when in ProgressStateReplicate
        case ProgressStateReplicate: { // ProgressStateReplicate 表示该节点正处于正常同步的状态，因此我们乐观地让其 progress 的next 增加到发送的最后一条预写日志的 index + 1
          uint64_t last = msg->entries.back().index;  // 发送的最后一条预写日志的 index
          pr->optimistic_update(last);  // 直接 next = last + 1
          pr->inflights->add(last);  // 在该节点对应的滑动窗口中放入该 message 的最后一条entry的 index
          break;
        }
        case ProgressStateProbe: {  // ProgressStateProbe 表示该节点处于探测状态，当节点拒绝了 leader 的预写日志的时候会进入该状态，leader会标记其为停止 set_pause 暂时停止往该节点发送新的预写日志
          pr->set_pause();
          break;
        }
        default: {
          LOG_FATAL("%lu is sending append in unhandled state %s", id_, progress_state_to_string(pr->state));
        }
      }
    }
  }
  send(std::move(msg));
  return true;
}

// 在bcast_heartbeat_with_ctx中发送心跳
void Raft::send_heartbeat(uint64_t to, std::vector<uint8_t> ctx) {
  // Attach the commit as min(to.matched, r.committed).
  // When the leader sends out heartbeat message,
  // the receiver(follower) might not be matched with the leader
  // or it might not have all the committed entries.
  // The leader MUST NOT forward the follower's commit to
  // an unmatched index.
  uint64_t commit = std::min(get_progress(to)->match, raft_log_->committed_);
  proto::MessagePtr msg(new proto::Message());
  msg->to = to;
  msg->type = proto::MsgHeartbeat;
  msg->commit = commit;
  msg->context = std::move(ctx);
  send(std::move(msg));
}

// 传入一个函数，该函数对每一个节点的 progress 和学习者节点的 progress 进行操作
void Raft::for_each_progress(const std::function<void(uint64_t, ProgressPtr&)>& callback) {
  for (auto it = prs_.begin(); it != prs_.end(); ++it) {
    callback(it->first, it->second);
  }

  for (auto it = learner_prs_.begin(); it != learner_prs_.end(); ++it) {
    callback(it->first, it->second);
  }
}

// 遍历每个 progress 发送 append 消息
void Raft::bcast_append() {
  for_each_progress([this](uint64_t id, ProgressPtr& progress) {
    if (id == id_) {  // 略过自己
      return;
    }

    this->send_append(id);
  });
}

// 通过 ctx 根据自身记录的每个节点的 progress 发送心跳
void Raft::bcast_heartbeat() {
  std::vector<uint8_t> ctx;
  read_only_->last_pending_request_ctx(ctx); // 插入 ctx
  bcast_heartbeat_with_ctx(std::move(ctx));  // 发送心跳
}

void Raft::bcast_heartbeat_with_ctx(const std::vector<uint8_t>& ctx) {  // 根据每个节点的 progress  通过 ctx 发送心跳
  for_each_progress([this, ctx](uint64_t id, ProgressPtr& progress) {
    if (id == id_) {
      return;
    }

    this->send_heartbeat(id, std::move(ctx));
  });
}

// 根据 progress 中记录的所有节点的 commit 位置，更新大多数都已经到达的 commit index
bool Raft:: maybe_commit() {
  // Preserving matchBuf across calls is an optimization
  // used to avoid allocating a new slice on each call.
  match_buf_.clear();

  // 拿到所有 progress 已经同步的索引放在vector 中
  for (auto it = prs_.begin(); it != prs_.end(); ++it) {
    match_buf_.push_back(it->second->match);
  }

  std::sort(match_buf_.begin(), match_buf_.end());     // 排序
  auto mci = match_buf_[match_buf_.size() - quorum()]; // 取到多数派已经同意的索引，说明多数派已经同步预写日志到了该 index
  return raft_log_->maybe_commit(mci, term_);  // leader 尝试去提交 mci 前置位上的预写日志
}

// 新 leader 上任、follower等成员状态变更的时候重置投票、任期、过期时间等信息
void Raft::reset(uint64_t term) {
  // 如果当前term_不等于传入的 term 则表明，candidate 调用了 reset，此时需要更新 term_为传入的 term，并且重置candidate节点的投票对象
  if (term_ != term) {
    term_ = term;
    vote_ = 0;
  }
  lead_ = 0;

  election_elapsed_ = 0;  // 选举超时
  heartbeat_elapsed_ = 0;  // 心跳
  reset_randomized_election_timeout();  // 重置选举随机值

  abort_leader_transfer();

  votes_.clear();  // 清空每个节点的投票结果
  for_each_progress([this](uint64_t id, ProgressPtr& progress) {
    bool is_learner = progress->is_learner;
    progress = std::make_shared<Progress>(max_inflight_);
    progress->next = raft_log_->last_index() + 1;
    progress->is_learner = is_learner;

    if (id == id_) {
      progress->match = raft_log_->last_index();
    }

  });

  pending_conf_index_ = 0;
  uncommitted_size_ = 0;
  read_only_->pending_read_index.clear();
  read_only_->read_index_queue.clear();
}

// 调用 add_node_or_learner 来添加 node（添加 progress）
void Raft::add_node(uint64_t id) {
  add_node_or_learner(id, false);
}

bool Raft::append_entry(const std::vector<proto::Entry>& entries) {
  uint64_t li = raft_log_->last_index();
  std::vector<proto::EntryPtr> ents(entries.size(), nullptr);

  // 存放进新创建的 vector 中，index 从 raft_log_中的 last_index() + 1 开始
  for (size_t i = 0; i < entries.size(); ++i) {
    proto::EntryPtr ent(new proto::Entry());
    ent->term = term_;
    ent->index = li + 1 + i;
    ent->data = entries[i].data;
    ent->type = entries[i].type;
    ents[i] = ent;
  }
  // 判断是否超过最大 uncommitted entries 的限制
  if (!increase_uncommitted_size(ents)) { 
    LOG_DEBUG("%lu appending new entries to log would exceed uncommitted entry size limit; dropping proposal", id_);
    // Drop the proposal.
    return false;
  }

  // 通过调用 unstable 中的 truncate_and_append，比对 unstable 中第一条 entry 的 index 和打算 append 进来的 entry 的第一条 entry 大小关系，再最终调整决定 unstable 中的 entry
  li = raft_log_->append(ents);
  get_progress(id_)->maybe_update(li);  // 更新自己的 progress 
  // Regardless of maybeCommit's return, our caller will call bcastAppend.
  maybe_commit();
  return true;
}

// follower 和 candidate 绑定的计时驱动竞选函数
void Raft::tick_election() {
  election_elapsed_++;  // 计时自增

  // 如果超时并且自身有资格发起竞选则给自己发送一条驱动竞选信息
  if (promotable() && past_election_timeout()) {
    election_elapsed_ = 0;
    proto::MessagePtr msg(new proto::Message());
    msg->from = id_;
    msg->type = proto::MsgHup;
    step(std::move(msg));  // 自己给自己发送一条信息驱动竞选
  }
}

//  header 绑定的计时发送心跳维持 leader 地位函数
void Raft::tick_heartbeat() {
  heartbeat_elapsed_++;
  election_elapsed_++;

  if (election_elapsed_ >= election_timeout_) {  // 如果超过了选举超时时间，则检查是否还能和大多数派取得响应，维持自己的 leader 地位
    election_elapsed_ = 0;
    if (check_quorum_) {
      proto::MessagePtr msg(new proto::Message());
      msg->from = id_;
      msg->type = proto::MsgCheckQuorum;  // 检查是否还有多数派能响应
      step(std::move(msg));  // 驱动自己和大多数派取得联系
    }
    // 选举超时时间到了还不能转让 leader 的话，自己再次成为 leader
    if (state_ == RaftState::Leader && lead_transferee_ != 0) {  
      abort_leader_transfer();
    }
  }

  if (state_ != RaftState::Leader) {
    return;
  }

  if (heartbeat_elapsed_ >= heartbeat_timeout_) {  // 超过了心跳超时时间，检查与其他节点的响应情况
    heartbeat_elapsed_ = 0;
    proto::MessagePtr msg(new proto::Message());
    msg->from = id_;
    msg->type = proto::MsgBeat;  // 发起一轮心跳同步
    step(std::move(msg));
  }
}

// 超过选举超时时间
bool Raft::past_election_timeout() {
  return election_elapsed_ >= randomized_election_timeout_;
}

// 重置随机的选举超时时间
void Raft::reset_randomized_election_timeout() {
  randomized_election_timeout_ = election_timeout_ + random_device_.gen();
  assert(randomized_election_timeout_ <= 2 * election_timeout_);
}

// 检查当前是否有足够的活跃节点来满足法定人数。如果没有，领导者将调用 r.becomeFollower() 方法退位，成为 follower。
bool Raft::check_quorum_active() {
  size_t act = 0;
  for_each_progress([&act, this](uint64_t id, ProgressPtr& pr) {
    if (id == this->id_) { // 如果是自己，则增加活跃节点数后返回
      act++;
      return;
    }
    if (pr->recent_active && !pr->is_learner) {  // 判断其他节点是否为活跃节点
      act++;
    }
  });

  return act >= quorum();
}

// 发送超时消息，用于让 leader 转移领导权，leader 发送该类型的消息给 follower，簇拥 follower 发起正式选举
void Raft::send_timeout_now(uint64_t to) {
  proto::MessagePtr msg(new proto::Message());
  msg->to = to;
  msg->type = proto::MsgTimeoutNow;
  send(std::move(msg));
}

// 取消领导者转移
void Raft::abort_leader_transfer() {
  lead_transferee_ = 0;
}

// 在收到写请求的时候需要统计未提交日志的大小，不能超过 max_uncommitted_size
bool Raft::increase_uncommitted_size(const std::vector<proto::EntryPtr>& entries) {
  uint32_t s = 0;
  for (auto& entry : entries) {  // 统计所有日志的单纯数据的大小
    s += entry->payload_size();
  }
  if (uncommitted_size_ > 0 && uncommitted_size_ + s > max_uncommitted_size_) {  // 如果超过了最大未提交日志的限制
    // If the uncommitted tail of the Raft log is empty, allow any size
    // proposal. Otherwise, limit the size of the uncommitted tail of the
    // log and drop any proposal that would push the size over the limit.
    return false;
  }
  uncommitted_size_ += s;
  return true;
}

// 减少已经提交的entries占用的未提交大小，传入的参数 entries 为已经提交的 entries
void Raft::reduce_uncommitted_size(const std::vector<proto::EntryPtr>& entries) {
  if (uncommitted_size_ == 0) {
    // Fast-path for followers, who do not track or enforce the limit.
    return;
  }

  uint32_t size = 0;

  for (const proto::EntryPtr& e: entries) {
    size += e->payload_size();
  }
  if (size > uncommitted_size_) {
    // uncommittedSize may underestimate the size of the uncommitted Raft
    // log tail but will never overestimate it. Saturate at 0 instead of
    // allowing overflow.
    uncommitted_size_ = 0;
  } else {
    uncommitted_size_ -= size;
  }
}

}