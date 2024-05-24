/**
*
*/

#include <iostream>
#include <chrono>
#include <thread>
#include "redox.hpp"

using namespace std;
using redox::Redox;
using redox::Command;

redox::Redox rdx;

int main(int argc, char* argv[]) {

  if(!rdx.connect("localhost", 6379)) return 1;

  thread setter([]() {
    for(int i = 0; i < 5000; i++) {
      rdx.command({"INCR", "counter"});
      this_thread::sleep_for(chrono::milliseconds(1));
    }
    cout << "Setter thread exiting." << endl;
  });

  thread getter([]() {
    for(int i = 0; i < 5; i++) {
      rdx.command(
          {"GET", "counter"},
          [](Command& c) {
            if(c.ok()) cout << c.cmd() << ": " << c.reply<string>() << endl;
          }
      );
      this_thread::sleep_for(chrono::milliseconds(1000));
    }
    cout << "Getter thread exiting." << endl;
  });

  setter.join();
  getter.join();

  rdx.disconnect();
  return 0;
};
