/*
* Redox - A modern, asynchronous, and wicked fast C++11 client for Redis
*
*    https://github.com/hmartiro/redox
*
* Copyright 2015 - Hayk Martirosyan <hayk.mart at gmail dot com>
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

#include <vector>
#include <set>
#include <unordered_set>
#include "command.hpp"
#include "client.hpp"

using namespace std;

namespace redox {

Command::Command(Redox *rdx, long id, const vector<string> &cmd,
                         const function<void(Command &)> &callback, 
                         double repeat, double after, 
                         bool free_memory, log::Logger &logger)
    : rdx_(rdx), id_(id), cmd_(cmd), repeat_(repeat), after_(after), free_memory_(free_memory),
      callback_(callback), last_error_(), logger_(logger) {
}

void Command::wait() {
  unique_lock<mutex> lk(waiter_lock_);
  waiter_.wait(lk, [this]() { return waiting_done_.load(); });
  waiting_done_ = {false};
}

void Command::processReply(redisReply *r) {

  last_error_.clear();
  reply_obj_ = r;

  if (reply_obj_ == nullptr) {
    reply_status_ = ERROR_REPLY;
    last_error_ = "Received null redisReply* from hiredis.";
    logger_.error() << last_error_;
    Redox::disconnectedCallback(rdx_->ctx_, REDIS_ERR);

  } else {
    reply_status_ = OK_REPLY;
    //parseReplyObject();
  }

  invoke();

  pending_--;

  {
    unique_lock<mutex> lk(waiter_lock_);
    waiting_done_ = true;
  }
  waiter_.notify_all();

  // Always free the reply object for repeating commands
  if (repeat_ > 0) {
    freeReply();

  } else {

    // User calls .free()
    if (!free_memory_)
      return;

    // Free non-repeating commands automatically
    // once we receive expected replies
    if (pending_ == 0)
      free();
  }
}

// This is the only method in Command that has
// access to private members of Redox
void Command::free() {

  lock_guard<mutex> lg(rdx_->free_queue_guard_);
  rdx_->commands_to_free_.push(id_);
  ev_async_send(rdx_->evloop_, &rdx_->watcher_free_);
}

void Command::freeReply() {

  if (reply_obj_) {
    freeReplyObject(reply_obj_);
    reply_obj_ = nullptr;
  }
}

string Command::cmd() const { return rdx_->vecToStr(cmd_); }

bool Command::isExpectedReply(int type) {

  if (reply_obj_->type == type) {
    reply_status_ = OK_REPLY;
    return true;
  }

  if (checkErrorReply() || checkNilReply())
    return false;

  stringstream errorMessage;
  errorMessage << "Received reply of type " << reply_obj_->type << ", expected type " << type << ".";
  last_error_ = errorMessage.str();
  logger_.error() << cmd() << ": " << last_error_;
  reply_status_ = WRONG_TYPE;
  return false;
}

bool Command::isExpectedReply(int typeA, int typeB) {

  if ((reply_obj_->type == typeA) || (reply_obj_->type == typeB)) {
    reply_status_ = OK_REPLY;
    return true;
  }

  if (checkErrorReply() || checkNilReply())
    return false;

  stringstream errorMessage;
  errorMessage << "Received reply of type " << reply_obj_->type << ", expected type " << typeA
               << " or " << typeB << ".";
  last_error_ = errorMessage.str();
  logger_.error() << cmd() << ": " << last_error_;
  reply_status_ = WRONG_TYPE;
  return false;
}

bool Command::checkErrorReply() {

  if (reply_obj_->type == REDIS_REPLY_ERROR) {
    if (reply_obj_->str != 0) {
      last_error_ = reply_obj_->str;
    }

    logger_.error() << cmd() << ": " << last_error_;
    reply_status_ = ERROR_REPLY;
    return true;
  }
  return false;
}

bool Command::checkNilReply() {

  if (reply_obj_->type == REDIS_REPLY_NIL) {
    logger_.warning() << cmd() << ": Nil reply.";
    reply_status_ = NIL_REPLY;
    return true;
  }
  return false;
}

// ----------------------------------------------------------------------------
// Specializations of parseReplyObject for all expected return types
// ----------------------------------------------------------------------------

template <> redisReply* Command::reply() {
  if (!checkErrorReply())
    reply_status_ = OK_REPLY;
  return reply_obj_;
}

template <> string Command::reply() {
  string ret;
  if (isExpectedReply(REDIS_REPLY_STRING, REDIS_REPLY_STATUS)) {
    ret = {reply_obj_->str, static_cast<size_t>(reply_obj_->len)};
  }
  return ret;
}

template <> char* Command::reply() {
  if (!isExpectedReply(REDIS_REPLY_STRING, REDIS_REPLY_STATUS))
    return nullptr;
  return reply_obj_->str;
}

template <> int Command::reply() {
  if (!isExpectedReply(REDIS_REPLY_INTEGER))
    return -1;
  return (int)reply_obj_->integer;
}

template <> long long int Command::reply() {

  if (!isExpectedReply(REDIS_REPLY_INTEGER))
    return -1;
  return reply_obj_->integer;
}

template <> nullptr_t Command::reply() {
  if (!isExpectedReply(REDIS_REPLY_NIL))
    return nullptr;
}

template <> vector<string> Command::reply() {
  vector<string> ret;
  if (!isExpectedReply(REDIS_REPLY_ARRAY))
    return ret;

  for (size_t i = 0; i < reply_obj_->elements; i++) {
    redisReply *r = *(reply_obj_->element + i);
    ret.emplace_back(r->str, r->len);
  }
  return ret;
}

template <> unordered_set<string> Command::reply() {
  unordered_set<string> ret;
  if (!isExpectedReply(REDIS_REPLY_ARRAY))
    return ret;

  for (size_t i = 0; i < reply_obj_->elements; i++) {
    redisReply *r = *(reply_obj_->element + i);
    ret.emplace(r->str, r->len);
  }
  return ret;
}

template <> set<string> Command::reply() {
  set<string> ret;
  if (!isExpectedReply(REDIS_REPLY_ARRAY))
    return ret;

  for (size_t i = 0; i < reply_obj_->elements; i++) {
    redisReply *r = *(reply_obj_->element + i);
    ret.emplace(r->str, r->len);
  }
  return ret;
}

void Command::stopTimer() {
  lock_guard<mutex> lg(timer_guard_);
#ifdef USE_LIBHV
  htimer_del(timer_);
  timer_ = nullptr;
#else
  ev_timer_stop(rdx_->evloop_, &timer_);
#endif
}

template REDOX_EXPORT string Command::reply();
template REDOX_EXPORT redisReply *Command::reply();
template REDOX_EXPORT char *Command::reply();
template REDOX_EXPORT int Command::reply();
template REDOX_EXPORT long long int Command::reply();
template REDOX_EXPORT nullptr_t Command::reply();
template REDOX_EXPORT vector<string> Command::reply();
template REDOX_EXPORT set<string> Command::reply();
template REDOX_EXPORT unordered_set<string> Command::reply();
} // End namespace redox
