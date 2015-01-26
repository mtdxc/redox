#include <stdlib.h>
#include <iostream>
#include "../src/redox.hpp"

using namespace std;

int main(int argc, char *argv[]) {

  redox::Redox rdx; // Initialize Redox (default host/port)
  if (!rdx.start()) return 1; // Start the event loop

  redox::Redox rdx_pub;
  if(!rdx_pub.start()) return 1;

  auto got_message = [](const string& topic, const string& msg) {
    cout << topic << ": " << msg << endl;
  };

  auto subscribed = [](const string& topic) {
    cout << "> Subscribed to " << topic << endl;
  };

  auto unsubscribed = [](const string& topic) {
    cout << "> Unsubscribed from " << topic << endl;
  };

  rdx.psubscribe("news", got_message, subscribed, unsubscribed);
  rdx.subscribe("sports", got_message, subscribed, unsubscribed);

  this_thread::sleep_for(chrono::milliseconds(20));
  for(auto s : rdx.subscribed_topics()) cout << "topic: " << s << endl;

  rdx_pub.publish("news", "hello!");
  rdx_pub.publish("news", "whatup");
  rdx_pub.publish("sports", "yo");

  this_thread::sleep_for(chrono::seconds(1));
  rdx.unsubscribe("sports");
  rdx_pub.publish("sports", "yo");
  rdx_pub.publish("news", "whatup");

  this_thread::sleep_for(chrono::milliseconds(1));
  rdx.punsubscribe("news");

  rdx_pub.publish("sports", "yo");
  rdx_pub.publish("news", "whatup", [](const string& topic, const string& msg) {
    cout << "published to " << topic << ": " << msg << endl;
  });
  rdx_pub.publish("news", "whatup");
  rdx.block();
  rdx_pub.block();

  return 0;
}