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

#include <signal.h>
#include <algorithm>
#include "client.hpp"

using namespace std;

namespace {

template<typename tev, typename tcb>
void redox_ev_async_init(tev ev, tcb cb)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
    ev_async_init(ev,cb);
#pragma GCC diagnostic pop
}

template<typename ttimer, typename tcb, typename tafter, typename trepeat>
void redox_ev_timer_init(ttimer timer, tcb cb, tafter after, trepeat repeat)
{
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
    ev_timer_init(timer,cb,after,repeat);
#pragma GCC diagnostic pop
}

} // anonymous

namespace redox {

Redox::Redox(ostream &log_stream, log::Level log_level)
    : logger_(log_stream, log_level), evloop_(nullptr) {}

bool Redox::connect(const string &host, const int port,
                    function<void(int)> connection_callback) {

  host_ = host;
  port_ = port;
  user_connection_callback_ = connection_callback;

  if (!initEv())
    return false;

  // Connect over TCP
  ctx_ = redisAsyncConnect(host.c_str(), port);

  if (!initHiredis())
    return false;

  event_loop_thread_ = thread([this] { runEventLoop(); });

  // Block until connected and running the event loop, or until
  // a connection error happens and the event loop exits
#ifdef USE_LIBHV
  {
    unique_lock<mutex> ul(connect_lock_);
    connect_waiter_.wait(ul, [this] { return connect_state_ != NOT_YET_CONNECTED; });
  }
#endif // USE_LIBHV
  {
    unique_lock<mutex> ul(running_lock_);
    running_waiter_.wait(ul, [this] {
      lock_guard<mutex> lg(connect_lock_);
      return running_ || connect_state_ == CONNECT_ERROR;
    });
  }

  // Return if succeeded
  return getConnectState() == CONNECTED;
}

bool Redox::connectUnix(const string &path, function<void(int)> connection_callback) {

  path_ = path;
  user_connection_callback_ = connection_callback;

  if (!initEv())
    return false;

  // Connect over unix sockets
  ctx_ = redisAsyncConnectUnix(path.c_str());

  if (!initHiredis())
    return false;

  event_loop_thread_ = thread([this] { runEventLoop(); });

  // Block until connected and running the event loop, or until
  // a connection error happens and the event loop exits
#ifdef USE_LIBHV
  {
    unique_lock<mutex> ul(connect_lock_);
    connect_waiter_.wait(ul, [this] { return connect_state_ != NOT_YET_CONNECTED; });
  }
#endif // USE_LIBHV
  {
    unique_lock<mutex> ul(running_lock_);
    running_waiter_.wait(ul, [this] {
      lock_guard<mutex> lg(connect_lock_);
      return running_ || connect_state_ == CONNECT_ERROR;
    });
  }

  // Return if succeeded
  return getConnectState() == CONNECTED;
}

void Redox::disconnect() {
  stop();
  wait();
}

void Redox::stop() {
  to_exit_ = true;
  logger_.debug() << "stop() called, breaking event loop";
  ev_async_send(evloop_, &watcher_stop_);
}

void Redox::wait() {
  unique_lock<mutex> ul(exit_lock_);
  exit_waiter_.wait(ul, [this] { return exited_; });
}

Redox::~Redox() {

  // Bring down the event loop
  if (getRunning()) {
    stop();
  }

  if (event_loop_thread_.joinable())
    event_loop_thread_.join();
  if (evloop_ != nullptr) {
#ifdef USE_LIBHV
    hloop_free(&evloop_);
#else
    ev_loop_destroy(evloop_);
#endif
  }
}

void Redox::connectedCallback(const redisAsyncContext *ctx, int status) {
  Redox *rdx = (Redox *)ctx->data;

  if (status != REDIS_OK) {
    rdx->logger_.fatal() << "Could not connect to Redis: " << ctx->errstr;
    rdx->logger_.fatal() << "Status: " << status;
    rdx->setConnectState(CONNECT_ERROR);

  } else {
    rdx->logger_.info() << "Connected to Redis.";
    // Disable hiredis automatically freeing reply objects
    ctx->c.reader->fn->freeObject = [](void *reply) {};
    rdx->setConnectState(CONNECTED);
  }

  if (rdx->user_connection_callback_) {
    rdx->user_connection_callback_(rdx->getConnectState());
  }
}

void Redox::disconnectedCallback(const redisAsyncContext *ctx, int status) {

  Redox *rdx = (Redox *)ctx->data;

  if (status != REDIS_OK) {
    rdx->logger_.error() << "Disconnected from Redis on error: " << ctx->errstr;
    rdx->setConnectState(DISCONNECT_ERROR);
  } else {
    rdx->logger_.info() << "Disconnected from Redis as planned.";
    rdx->setConnectState(DISCONNECTED);
  }

  rdx->stop();
  if (rdx->user_connection_callback_) {
    rdx->user_connection_callback_(rdx->getConnectState());
  }
}

bool Redox::initEv() {
#ifdef SIGPIPE
  signal(SIGPIPE, SIG_IGN);
#endif
#ifdef USE_LIBHV
  evloop_ = hloop_new(0);
  if (evloop_)
    hloop_set_userdata(evloop_, this);
#else
  evloop_ = ev_loop_new(EVFLAG_AUTO);
  if (evloop_)
    ev_set_userdata(evloop_, (void *)this); // Back-reference
#endif
  if (evloop_ == nullptr) {
    logger_.fatal() << "Could not create a libev event loop.";
    setConnectState(INIT_ERROR);
    return false;
  }
  return true;
}

bool Redox::initHiredis() {

  ctx_->data = (void *)this; // Back-reference

  if (ctx_->err) {
    logger_.fatal() << "Could not create a hiredis context: " << ctx_->errstr;
    setConnectState(INIT_ERROR);
    return false;
  }
  // Attach event loop to hiredis
#ifdef USE_LIBHV
  if (redisLibhvAttach(ctx_, evloop_) != REDIS_OK) 
#else
  if (redisLibevAttach(evloop_, ctx_) != REDIS_OK)
#endif
  {
    logger_.fatal() << "Could not attach libev event loop to hiredis.";
    setConnectState(INIT_ERROR);
    return false;
  }

  // Set the callbacks to be invoked on server connection/disconnection
  if (redisAsyncSetConnectCallback(ctx_, Redox::connectedCallback) != REDIS_OK) {
    logger_.fatal() << "Could not attach connect callback to hiredis.";
    setConnectState(INIT_ERROR);
    return false;
  }

  if (redisAsyncSetDisconnectCallback(ctx_, Redox::disconnectedCallback) != REDIS_OK) {
    logger_.fatal() << "Could not attach disconnect callback to hiredis.";
    setConnectState(INIT_ERROR);
    return false;
  }

  return true;
}

void Redox::noWait(bool state) {
  if (state)
    logger_.info() << "No-wait mode enabled.";
  else
    logger_.info() << "No-wait mode disabled.";
  nowait_ = state;
}

int Redox::getConnectState() {
  lock_guard<mutex> lk(connect_lock_);
  return connect_state_;
}

void Redox::setConnectState(int connect_state) {
  {
    lock_guard<mutex> lk(connect_lock_);
    connect_state_ = connect_state;
  }
  connect_waiter_.notify_all();
}

int Redox::getRunning() {
  lock_guard<mutex> lg(running_lock_);
  return running_;
}
void Redox::setRunning(bool running) {
  {
    lock_guard<mutex> lg(running_lock_);
    running_ = running;
  }
  running_waiter_.notify_one();
}

int Redox::getExited() {
  lock_guard<mutex> lg(exit_lock_);
  return exited_;
}
void Redox::setExited(bool exited) {
  {
    lock_guard<mutex> lg(exit_lock_);
    exited_ = exited;
  }
  exit_waiter_.notify_one();
}

void Redox::runEventLoop() {
#ifndef USE_LIBHV
  // Events to connect to Redox
  ev_run(evloop_, EVRUN_ONCE);
  ev_run(evloop_, EVRUN_NOWAIT);
  // Block until connected to Redis, or error
  {
    unique_lock<mutex> ul(connect_lock_);
    connect_waiter_.wait(ul, [this] { return connect_state_ != NOT_YET_CONNECTED; });

    // Handle connection error
    if (connect_state_ != CONNECTED) {
      logger_.warning() << "Did not connect, event loop exiting.";
      setExited(true);
      setRunning(false);
      return;
    }
  }
#endif
#ifdef USE_LIBHV
  memset(&watcher_stop_, 0, sizeof(hevent_t));
  watcher_stop_.cb = [](hevent_t *ev) { 
    hloop_t* loop = hevent_loop(ev);
    hloop_stop(loop);
  };
  
  memset(&watcher_command_, 0, sizeof(hevent_t));
  watcher_command_.cb = [](hevent_t *ev) { 
    Redox *rdx = (Redox *)hevent_userdata(ev);
    rdx->processQueuedCommands();
  };
  hevent_set_userdata(&watcher_command_, this);

  memset(&watcher_free_, 0, sizeof(hevent_t));
  watcher_free_.cb = [](hevent_t *ev) {
    Redox *rdx = (Redox *)hevent_userdata(ev);
    rdx->freeQueuedCommands();
  };
  hevent_set_userdata(&watcher_free_, this);
#else
  // Set up asynchronous watcher which we signal every
  // time we add a command
  redox_ev_async_init(&watcher_command_, [](struct ev_loop *loop, ev_async *async, int revents) {
    Redox *rdx = (Redox *)ev_userdata(loop);
    rdx->processQueuedCommands();
  });
  ev_async_start(evloop_, &watcher_command_);

  // Set up an async watcher to break the loop
  redox_ev_async_init(&watcher_stop_, [](ev_loop *loop, ev_async *async, int revents) {
    ev_break(loop, EVBREAK_ALL);
  });
  ev_async_start(evloop_, &watcher_stop_);

  // Set up an async watcher which we signal every time
  // we want a command freed
  redox_ev_async_init(&watcher_free_, [](struct ev_loop *loop, ev_async *async, int revents) {
    Redox *rdx = (Redox *)ev_userdata(loop);
    rdx->freeQueuedCommands();
  });
  ev_async_start(evloop_, &watcher_free_);
#endif

  setRunning(true);

  // Run the event loop, using NOWAIT if enabled for maximum
  // throughput by avoiding any sleeping
#ifdef USE_LIBHV
  hloop_run(evloop_);
#else
  while (!to_exit_) {
    if (nowait_) {
      ev_run(evloop_, EVRUN_NOWAIT);
    } else {
      ev_run(evloop_);
    }
  }
#endif
  logger_.info() << "Stop signal detected. Closing down event loop.";

  // Signal event loop to free all commands
  freeAllCommands();

  // Wait to receive server replies for clean hiredis disconnect
#ifdef USE_LIBHV
  hloop_process_events(evloop_, 10);
#else
  this_thread::sleep_for(chrono::milliseconds(10));
  ev_run(evloop_, EVRUN_NOWAIT);
#endif

  if (getConnectState() == CONNECTED) {
    redisAsyncDisconnect(ctx_);
  }
#ifdef USE_LIBHV
  hloop_process_events(evloop_);
#else
  // Run once more to disconnect
  ev_run(evloop_, EVRUN_NOWAIT);
#endif
  long created = commands_created_;
  long deleted = commands_deleted_;
  if (created != deleted) {
    logger_.error() << "All commands were not freed! " << deleted << "/"
                    << created;
  }

  // Let go for block_until_stopped method
  setExited(true);
  setRunning(false);

  logger_.info() << "Event thread exited.";
}

Command *Redox::findCommand(long id) {
  lock_guard<mutex> lg(command_map_guard_);
  auto &command_map = commands_reply_;
  auto it = command_map.find(id);
  if (it == command_map.end())
    return nullptr;
  return it->second;
}

void Redox::commandCallback(redisAsyncContext *ctx, void *r, void *privdata) {

  Redox *rdx = (Redox *)ctx->data;
  long id = (long)privdata;
  redisReply *reply_obj = (redisReply *)r;

  Command *c = rdx->findCommand(id);
  if (c == nullptr) {
    freeReplyObject(reply_obj);
    return;
  }

  c->processReply(reply_obj);
}

bool Redox::submitToServer(Command *c) {

  Redox *rdx = c->rdx_;
  c->pending_++;

  // Construct a char** from the vector
  vector<const char *> argv;
  transform(c->cmd_.begin(), c->cmd_.end(), back_inserter(argv),
            [](const string &s) { return s.c_str(); });

  // Construct a size_t* of string lengths from the vector
  vector<size_t> argvlen;
  transform(c->cmd_.begin(), c->cmd_.end(), back_inserter(argvlen),
            [](const string &s) { return s.size(); });

  if (redisAsyncCommandArgv(rdx->ctx_, commandCallback, (void *)c->id_, 
      argv.size(), &argv[0], &argvlen[0]) != REDIS_OK) {
    rdx->logger_.error() << "Could not send \"" << c->cmd() << "\": " << rdx->ctx_->errstr;
    c->reply_status_ = Command::SEND_ERROR;
    c->invoke();
    return false;
  }

  return true;
}

void Redox::submitCommandCallback(long id) {
  Command *c = findCommand(id);
  if (c == nullptr) {
    logger_.error() << "Couldn't find Command " << id
                         << " in command_map (submitCommandCallback).";
    return;
  }

  submitToServer(c);
}

bool Redox::processQueuedCommand(long id) {

  Command *c = findCommand(id);
  if (c == nullptr)
    return false;

  if ((c->repeat_ == 0) && (c->after_ == 0)) {
    submitToServer(c);
  } else {
    std::lock_guard<std::mutex> lg(c->timer_guard_);
#ifdef USE_LIBHV
    int after = c->after_ * 1000;
    if (!after) after = 1;
    uint32_t repeat = 0;
    if (c->repeat_ > 0)
      repeat = INFINITE;
    c->timer_ = htimer_add(
        evloop_,
        [](htimer_t *timer) {
            Redox *rdx = (Redox *)hevent_userdata(timer);
            long id = (long)hevent_id(timer);
            Command *c = rdx->findCommand(id);
            if (c) {
              submitToServer(c);
              if (c->repeat_ > 0)
                htimer_reset(timer, c->repeat_ * 1000);
            }
        },
        after, repeat);
    hevent_set_id(c->timer_, c->id_);
    hevent_set_userdata(c->timer_, this);
#else
    redox_ev_timer_init(
        &c->timer_,
        [](struct ev_loop *loop, ev_timer *timer, int revents) {
          Redox *rdx = (Redox *)ev_userdata(loop);
          long id = (long)timer->data;
          rdx->submitCommandCallback(id);
        },
        c->after_, c->repeat_);
    ev_timer_start(evloop_, &c->timer_);
    c->timer_.data = (void *)c->id_;
#endif // USE_LIBHV
  }

  return true;
}

void Redox::processQueuedCommands() {
  lock_guard<mutex> lg(queue_guard_);

  while (!command_queue_.empty()) {

    long id = command_queue_.front();
    command_queue_.pop();
    processQueuedCommand(id);
  }
}

void Redox::freeQueuedCommands() {
  lock_guard<mutex> lg(free_queue_guard_);

  while (!commands_to_free_.empty()) {
    long id = commands_to_free_.front();
    commands_to_free_.pop();
    freeQueuedCommand(id);
  }
}

bool Redox::freeQueuedCommand(long id) {
  Command *c = findCommand(id);
  if (c == nullptr)
    return false;

  c->freeReply();

  // Stop the libev timer if this is a repeating command
  if ((c->repeat_ != 0) || (c->after_ != 0)) {
    c->stopTimer();
  }

  deregisterCommand(c->id_);

  delete c;

  return true;
}

long Redox::freeAllCommands() {

  lock_guard<mutex> lg(free_queue_guard_);
  lock_guard<mutex> lg2(queue_guard_);
  lock_guard<mutex> lg3(command_map_guard_);

  auto &command_map = commands_reply_;
  long len = command_map.size();

  for (auto &pair : command_map) {
    Command *c = pair.second;

    c->freeReply();

    // Stop the libev timer if this is a repeating command
    if ((c->repeat_ != 0) || (c->after_ != 0)) {
      c->stopTimer();
    }

    delete c;
  }

  command_map.clear();
  commands_deleted_ += len;

  return len;
}

// ----------------------------
// Helpers
// ----------------------------

string Redox::vecToStr(const vector<string> &vec, const char delimiter) {
  string str;
  for (size_t i = 0; i < vec.size() - 1; i++)
    str += vec[i] + delimiter;
  str += vec[vec.size() - 1];
  return str;
}

vector<string> Redox::strToVec(const string &s, const char delimiter) {
  vector<string> vec;
  size_t last = 0;
  size_t next = 0;
  while ((next = s.find(delimiter, last)) != string::npos) {
    vec.push_back(s.substr(last, next - last));
    last = next + 1;
  }
  vec.push_back(s.substr(last));
  return vec;
}

string Redox::get(const string &key) {

  auto &c = commandSync({"GET", key});
  if (!c.ok()) {
    throw runtime_error("[FATAL] Error getting key " + key + ": Status code " +
                        to_string(c.status()));
  }
  string reply = c.reply<string>();
  c.free();
  return reply;
}

bool Redox::set(const string &key, const string &value) { return commandSync({"SET", key, value}).ok(); }

bool Redox::del(const string &key) { return commandSync({"DEL", key}).ok(); }

void Redox::publish(const string &topic, const string &msg) {
  command({"PUBLISH", topic, msg}, nullptr);
}

// ------------------------------------------------
// Implementation of templated methods
// ------------------------------------------------

Command &Redox::createCommand(const std::vector<std::string> &cmd,
                              const std::function<void(Command &)> &callback, double repeat,
                              double after, bool free_memory) {
  {
    std::unique_lock<std::mutex> ul(running_lock_);
    if (!running_) {
      throw std::runtime_error("[ERROR] Need to connect Redox before running commands!");
    }
  }

  auto *c = new Command(this, commands_created_.fetch_add(1), cmd, callback, repeat, after,
                        free_memory, logger_);

  std::lock_guard<std::mutex> lg(queue_guard_);
  std::lock_guard<std::mutex> lg2(command_map_guard_);

  commands_reply_[c->id_] = c;
  command_queue_.push(c->id_);

  // Signal the event loop to process this command
  ev_async_send(evloop_, &watcher_command_);

  return *c;
}

void Redox::command(const std::vector<std::string> &cmd,
                    const std::function<void(Command &)> &callback) {
  createCommand(cmd, callback);
}

Command &Redox::commandLoop(const std::vector<std::string> &cmd,
                            const std::function<void(Command &)> &callback, double repeat,
                            double after) {
  return createCommand(cmd, callback, repeat, after, false);
}

void Redox::commandDelayed(const std::vector<std::string> &cmd,
                           const std::function<void(Command &)> &callback, double after) {
  createCommand(cmd, callback, 0, after, true);
}

Command &Redox::commandSync(const std::vector<std::string> &cmd) {
  auto &c = createCommand(cmd, nullptr, 0, 0, false);
  c.wait();
  return c;
}

} // End namespace redis
