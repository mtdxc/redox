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

#pragma once

#include <iostream>
#include <functional>

#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

#include <string>
#include <queue>
#include <set>
#include <unordered_map>
#include <unordered_set>

#include <hiredis/hiredis.h>
#include <hiredis/async.h>
#if USE_LIBHV
#include <hiredis/adapters/libhv.h>
typedef struct hevent_s ev_async;
#define ev_async_send hloop_post_event
#else
#include <hiredis/adapters/libev.h>
#endif

#include "utils/logger.hpp"
#include "command.hpp"

namespace redox {

static const std::string REDIS_DEFAULT_HOST = "localhost";
static const int REDIS_DEFAULT_PORT = 6379;
static const std::string REDIS_DEFAULT_PATH = "/var/run/redis/redis.sock";

/**
* Redox is a Redis client for C++. It provides a synchronous and asynchronous
* API for using Redis in high-performance situations.
*/
class Redox {

public:
  // Connection states
  static const int NOT_YET_CONNECTED = 0; // Starting state
  static const int CONNECTED = 1;         // Successfully connected
  static const int DISCONNECTED = 2;      // Successfully disconnected
  static const int CONNECT_ERROR = 3;     // Error connecting
  static const int DISCONNECT_ERROR = 4;  // Disconnected on error
  static const int INIT_ERROR = 5;        // Failed to init data structures

  // ------------------------------------------------
  // Core public API
  // ------------------------------------------------

  /**
  * Constructor. Optionally specify a log stream and a log level.
  */
  Redox(std::ostream &log_stream = std::cout, log::Level log_level = log::Warning);

  /**
  * Disconnects from the Redis server, shuts down the event loop, and cleans up.
  * Internally calls disconnect() and wait().
  */
  ~Redox();

  /**
  * Enables or disables 'no-wait' mode. If enabled, no-wait mode means that the
  * event loop does not pause in between processing events. It can greatly increase
  * the throughput (commands per second),but means that the event thread will run at
  * 100% CPU. Enable when performance is critical and you can spare a core. Default
  * is off.
  *
  * Implementation note: When enabled, the event thread calls libev's ev_run in a
  * loop with the EVRUN_NOWAIT flag.
  */
  void noWait(bool state);

  /**
  * Connects to Redis over TCP and starts an event loop in a separate thread. Returns
  * true once everything is ready, or false on failure.
  */
  bool connect(const std::string &host = REDIS_DEFAULT_HOST, const int port = REDIS_DEFAULT_PORT,
               std::function<void(int)> connection_callback = nullptr);

  /**
  * Connects to Redis over a unix socket and starts an event loop in a separate
  * thread. Returns true once everything is ready, or false on failure.
  */
  bool connectUnix(const std::string &path = REDIS_DEFAULT_PATH,
                   std::function<void(int)> connection_callback = nullptr);

  /**
  * Disconnect from Redis, shut down the event loop, then return. A simple
  * combination of .stop() and .wait().
  */
  void disconnect();

  /**
  * Signal the event loop thread to disconnect from Redis and shut down.
  */
  void stop();

  /**
  * Blocks until the event loop exits and disconnection is complete, then returns.
  * Usually no need to call manually as it is handled in the destructor.
  */
  void wait();

  /**
  * Asynchronously runs a command and invokes the callback when a reply is
  * received or there is an error. The callback is guaranteed to be invoked
  * exactly once. The Command object is provided to the callback, and the
  * memory for it is automatically freed when the callback returns.
  */

  void command(const std::vector<std::string> &cmd,
               const std::function<void(Command &)> &callback = nullptr);

  /**
  * Synchronously runs a command, returning the Command object only once
  * a reply is received or there is an error. The user is responsible for
  * calling .free() on the returned Command object.
  */

  Command &commandSync(const std::vector<std::string> &cmd);

  /**
  * Synchronously runs a command, returning only once a reply is received
  * or there's an error. Returns true on successful reply, false on error.
  */
  // bool commandSync(const std::vector<std::string> &cmd);

  /**
  * Creates an asynchronous command that is run every [repeat] seconds,
  * with the first one run in [after] seconds. If [repeat] is 0, the
  * command is run only once. The user is responsible for calling .free()
  * on the returned Command object.
  */

  
  Command &commandLoop(const std::vector<std::string> &cmd,
                               const std::function<void(Command &)> &callback,
                               double repeat, double after = 0.0);

  /**
  * Creates an asynchronous command that is run once after a given
  * delay. The callback is invoked exactly once on a successful reply
  * or error, and the Command object memory is automatically freed
  * after the callback returns.
  */

  void commandDelayed(const std::vector<std::string> &cmd,
                      const std::function<void(Command &)> &callback, double after);

  // ------------------------------------------------
  // Utility methods
  // ------------------------------------------------

  /**
  * Given a vector of strings, returns a string of the concatenated elements, separated
  * by the delimiter. Useful for printing out a command string from a vector.
  */
  static std::string vecToStr(const std::vector<std::string> &vec, const char delimiter = ' ');

  /**
  * Given a command string, returns a vector of strings by splitting the input by
  * the delimiter. Useful for turning a string input into a command.
  */
  static std::vector<std::string> strToVec(const std::string &s, const char delimiter = ' ');

  // ------------------------------------------------
  // Command wrapper methods for convenience only
  // ------------------------------------------------

  /**
  * Redis GET command wrapper - return the value for the given key, or throw
  * an exception if there is an error. Blocking call.
  */
  std::string get(const std::string &key);

  /**
  * Redis SET command wrapper - set the value for the given key. Return
  * true if succeeded, false if error. Blocking call.
  */
  bool set(const std::string &key, const std::string &value);

  /**
  * Redis DEL command wrapper - delete the given key. Return true if succeeded,
  * false if error. Blocking call.
  */
  bool del(const std::string &key);

  /**
  * Redis PUBLISH command wrapper - publish the given message to all subscribers.
  * Non-blocking call.
  */
  void publish(const std::string &topic, const std::string &msg);

  // ------------------------------------------------
  // Public members
  // ------------------------------------------------

  // Hiredis context, left public to allow low-level access
  redisAsyncContext *ctx_;

  // TODO make these private
  // Redox server over TCP
  std::string host_;
  int port_;

  // Redox server over unix
  std::string path_;

  // Logger
  log::Logger logger_;

private:
  // ------------------------------------------------
  // Private methods
  // ------------------------------------------------

  // One stop shop for creating commands. The base of all public
  // methods that run commands.

  // One stop shop for creating commands. The base of all public
  // methods that run commands.
  
  Command &createCommand(const std::vector<std::string> &cmd,
                                 const std::function<void(Command &)> &callback = nullptr,
                                 double repeat = 0.0, double after = 0.0, bool free_memory = true);

  // Setup code for the constructors
  // Return true on success, false on failure
  bool initEv();
  bool initHiredis();

  // Callbacks invoked on server connection/disconnection
  static void connectedCallback(const redisAsyncContext *c, int status);
  static void disconnectedCallback(const redisAsyncContext *c, int status);

  // Main event loop, run in a separate thread
  void runEventLoop();

  // Return the given Command from the relevant command map, or nullptr if not there
  Command *findCommand(long id);

  // Send all commands in the command queue to the server
  void processQueuedCommands();

  // Process the command with the given ID. Return true if the command had the
  // templated type, and false if it was not in the command map of that type.
  bool processQueuedCommand(long id);

  // Callback given to libev for a Command's timer watcher, to be processed in
  // a deferred or looping state
  void submitCommandCallback(long id);
  // Submit an asynchronous command to the Redox server. Return
  // true if succeeded, false otherwise.
  static bool submitToServer(Command *c);

  // Callback given to hiredis to invoke when a reply is received
  static void commandCallback(redisAsyncContext *ctx, void *r, void *privdata);

  // Free all commands in the commands_to_free_ queue
  void freeQueuedCommands();

  // Free the command with the given ID. Return true if the command had the templated
  // type, and false if it was not in the command map of that type.
  bool freeQueuedCommand(long id);

  // Invoked by Command objects when they are completed. Removes them
  // from the command map.
  void deregisterCommand(const long id) {
    std::lock_guard<std::mutex> lg1(command_map_guard_);
    commands_reply_.erase(id);
    commands_deleted_ += 1;
  }

  // Free all commands remaining in the command maps
  long freeAllCommands();


  // Helper functions to get/set variables with synchronization.
  int getConnectState();
  void setConnectState(int connect_state);
  int getRunning();
  void setRunning(bool running);
  int getExited();
  void setExited(bool exited);

  // ------------------------------------------------
  // Private members
  // ------------------------------------------------

  // Manage connection state
  int connect_state_ = NOT_YET_CONNECTED;
  std::mutex connect_lock_;
  std::condition_variable connect_waiter_;

  // User connect/disconnect callbacks
  std::function<void(int)> user_connection_callback_;

  // Dynamically allocated libev event loop
#ifdef USE_LIBHV
  hloop_t *evloop_;
#else
  struct ev_loop *evloop_;
#endif
  // No-wait mode for high-performance
  std::atomic_bool nowait_ = {false};

  // Asynchronous watchers
  ev_async watcher_command_; // For processing commands
  ev_async watcher_stop_;    // For breaking the loop
  ev_async watcher_free_;    // For freeing commands

  // Track of Command objects allocated. Also provides unique Command IDs.
  std::atomic_long commands_created_ = {0};
  std::atomic_long commands_deleted_ = {0};

  // Separate thread to have a non-blocking event loop
  std::thread event_loop_thread_;

  // Variable and CV to know when the event loop starts running
  bool running_ = false;
  std::mutex running_lock_;
  std::condition_variable running_waiter_;

  // Variable and CV to know when the event loop stops running
  std::atomic_bool to_exit_ = {false}; // Signal to exit
  bool exited_ = false;  // Event thread exited
  std::mutex exit_lock_;
  std::condition_variable exit_waiter_;

  // Maps of each Command, fetchable by the unique ID number
  // In C++14, member variable templates will replace all of these types
  // with a single templated declaration
  // ---------
  // template<class ReplyT>
  // std::unordered_map<long, Command<ReplyT>*> commands_;
  // ---------
  std::unordered_map<long, Command *> commands_reply_;
  std::mutex command_map_guard_; // Guards access to all of the above

  // Command IDs pending to be sent to the server
  std::queue<long> command_queue_;
  std::mutex queue_guard_;

  // Commands IDs pending to be freed by the event loop
  std::queue<long> commands_to_free_;
  std::mutex free_queue_guard_;

  // Commands use this method to deregister themselves from Redox,
  // give it access to private members
  friend void Command::free();

  // Access to call disconnectedCallback
  friend void Command::processReply(redisReply *r);
};

} // End namespace redis
