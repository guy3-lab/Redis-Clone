#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <mutex>
#include <chrono>
#include <deque>

/* Q n A
    // Why does just sending in buffer work if it requires void *?
      // because array names are basically pointers themselve. So it could've been any name, 
      // and when passed into the function, the data will be stored at the first position in the array
      // and will overwrite anything in that buckets position.
    // Shouldn't I have to pass in the buffer as a pointer?
      // no, because cpp will do that automatically for you with array types
    // Does read work here or do i have to use recv instead?
      // read works here but is part of POSIX, whereas recv also works and is part of a more specific socket family of use cases
      // recv also has a 4th parameter that takes care of flags. idk anything about flags yet but well learn that later maybe
    // Important note: 
      // in the read function, I have passed response.size() but this is wrong.
      // what should instead be passed is sizeof(buffer), or the number of bytes allocated to the buffer array previously created
      // response.size is simply 7, in our case, because its the char length of PONG. 
      // and if we use response.size, then larger messages sent by the client would be split and henced returned incorrectly
      // so response.size needs to be updated to sizeof(buffer) (not 4096 because we shld maintain convention)
    // Why does the condition below being true imply that the client has disconnected?
      // first of all, the condition is actually wrong
      // the client has only disconnected if the return value of the read function is 0. 
      // if the return value is less than 0 then its just a general error
      // and ofcourse if above 0 then it works so this needs to be updated to <= 0  
    // why does response.c_str() work here if send requires type const void * instead of const char *?
      // because cpp will automatically conversion chain data type pointers to a void pointer
      // note that unlike char, void cannot be a data type on its own. it can only be used in method signature as the return type
      // and, in the way we use it in this program, as a pointer such as void *.
      // having a pointer variable of type void means that it is pointing to something but doesn't know what.
    // using a while loop here without any threading (initial solution) actually just gets clients sequentially instead of concurrently
    // causes an infinite loop too i think
    // we can build a very simple multi threading program using the inbuilt thread library
    // what is the diff between struct and class?
      // struct vs class difference is ONLY 1.
      // struct members are public by default and class members are private by default
    // why can't we use join instead of detach?
      // if we use join, the client handling order becomes sequential instead of concurrent
      // this is because the calling thread will join the call flow, and main will wait till calling thread receives pong from server
      // only after calling thread is finishe can client 2 be accepted  

      // build command parser that turns the resp command into an array of strings
      // then go to handle_client method and return appropriate string for each command
    // What is mutex? 
    // Why can't I just use a regular hashmap? 
    // What is this global storage thing? 
    // What is thread safety and why do we need it? 
    // What is lock_guard? What is lock? 
    // What is auto? 
    // What is the arrow key here, i've never seen it before: std::string value = it->second;?
*/

struct storageValue{
  std::string value;
  long long expiry_ms;
};

long long get_current_time_ms(){
  auto now = std::chrono::system_clock::now(); // time right now - not in ms
  auto time_since_beginning = now.time_since_epoch(); // time since 1970 (systems beginning of time) - not in ms
  auto time_since_beginning_ms = std::chrono::duration_cast<std::chrono::milliseconds>(time_since_beginning); // above time but in ms
  return time_since_beginning_ms.count();
}

std::unordered_map<std::string, storageValue> storageMap;
std::mutex mutex;
std::unordered_map<std::string, std::deque<std::string>> listStorage;

  std::vector<std::string> parse_array_command(const std::string& respString){ 
    int index = 0;
    std::vector<std::string> result;

    if(respString[index] != '*') return result; //defensive
    index++; // index + 1 for star
    // we need this cuz length could be more than single digit numbers
    // which would increase the length of the string 
    int array_length = 0;
    while(index < respString.length() && respString[index] != '\r'){
      // ASCII characters go from 0 to 9, so we multiply by 10 to add 10s, 100s, etc
      // say we have an array of length 27: *27
      // (0 * 10) + ('2' - '0') = 2
      // (2 * 10) + ('7' - '0') = 27
      // then exit loop
      array_length = array_length * 10 + (respString[index] - '0');
      index++;
    }
    index+=2; // skip \r\n
    // this loop iterates the every bulk string in entirety and adds every string to a vector
    // from $[string length] to final \r\n before next $[string length]
    for(int i = 0; i < array_length; i++){
      if(index >= respString.length() || respString[index] != '$') break;
      index++; // move past bulk string indicator: $

      int str_length = 0;
      while(respString[index] != '\r'){
        str_length = str_length * 10 + (respString[index] - '0');
        index++;
      }
      index+=2; // skip \r\n

      std::string message = "";
      while(respString[index] != '\r'){
        message+=respString[index];
        index++;
      }
      index+=2;
      result.push_back(message);
    }

    return result;
  }


void handle_client(int client_fd){
  //std::string testResponse = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
  //std::string testResponse2 = "*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$3\r\n100\r\n";
  char buffer[4096];

  while(true){
    memset(buffer, 0, sizeof(buffer)); // clear buffer
    int bytes_read = read(client_fd, buffer, sizeof(buffer));
    
    if(bytes_read <= 0){
      std::cerr << "Client disconnected or EOF.";
      break;
    }
    
    std::string respString(buffer, bytes_read);
    // this is our vector with each element being a string
    std::vector<std::string> message = parse_array_command(respString);
    // sets initial command to all upper
    std::transform(message[0].begin(), message[0].end(), message[0].begin(), ::toupper);
    std::string respondMessage;

    // use hashmap for the set and get commands
    // original solution of just checking for time expiry when timout is 0 doesnt work.
    // this is because when the user passes in a value like 100
    // the 100 is only a string. and even if we convert it, that value will never change
    // because its not associated with the system running our program...
    // this sounds quite obvious once it makes sense
    // so instead we have to simulate the exact time when the user sets an input
    // and the exact time after the users input is over.

    if (message[0] == "PING") {
      respondMessage = "+PONG\r\n";
    } else if (message[0] == "ECHO" && message.size() > 1) {
      respondMessage = "$" + std::to_string(message[1].length()) + "\r\n" + message[1] + "\r\n"; 
    } else if (message[0] == "SET" && message.size() > 2) {
      if (message.size() >= 5) {
        std::transform(message[3].begin(), message[3].end(), message[3].begin(), ::tolower);
        if (message[3] == "px") {
          int input_expiry_ms = std::stoi(message[4]); // 100
          long long final_expiry_ms = get_current_time_ms() + input_expiry_ms; //1100
          mutex.lock();
          storageMap[message[1]] = {message[2], final_expiry_ms}; // {"bar", 1100}
          mutex.unlock();
        }
      } else {
        mutex.lock();
        storageMap[message[1]] = {message[2], 0};
        mutex.unlock();
      }

      respondMessage = "+OK\r\n";
    } else if (message[0] == "GET" && message.size() > 1) {
      mutex.lock();
      if(storageMap.count(message[1]) > 0) { // check if count of a key is greater than 0 (all keys are unique so either 0 or 1)
        if (storageMap[message[1]].expiry_ms > 0){
          if (get_current_time_ms() < storageMap[message[1]].expiry_ms){
            respondMessage = "$" + std::to_string(storageMap[message[1]].value.size()) + "\r\n" + storageMap[message[1]].value + "\r\n";
          } else {
            storageMap.erase(message[1]);
            respondMessage = "$-1\r\n";
          }
        } else {
          respondMessage = "$" + std::to_string(storageMap[message[1]].value.size()) + "\r\n" + storageMap[message[1]].value + "\r\n";
        }  
      } else {
        respondMessage = "$-1\r\n";
      }
      mutex.unlock();

    } else if (message[0] == "RPUSH" && message.size() > 2) {
      mutex.lock();
      for(int i = 2; i < message.size(); i++){
        listStorage[message[1]].push_back(message[i]);
      }
      mutex.unlock();
      respondMessage = ":" + std::to_string(listStorage[message[1]].size()) + "\r\n";
    } else if (message[0] == "LPUSH" && message.size() > 2) {
      mutex.lock();
      for(int i = 2; i < message.size(); i++){
        listStorage[message[1]].push_front(message[i]);
      }
      mutex.unlock();
      respondMessage = ":" + std::to_string(listStorage[message[1]].size()) + "\r\n";
    }
    else if(message[0] == "LRANGE" && message.size() > 3) {
      int listSize = listStorage[message[1]].size();
      int start = std::stoi(message[2]);
      int stop = std::stoi(message[3]);

      mutex.lock();
      if (start < 0) {
        if (start * -1 < listSize) {
          start += listSize;
        } else {
          start = 0;
        }
      }
      if (stop < 0) {
        if (stop * -1 < listSize) {
          stop += listSize;
        } else {
          stop = 0;
        }
      }

      if (listStorage.count(message[1]) == 0 || start > listSize || start > stop) {
        mutex.unlock();
        respondMessage = "*0\r\n";
      }
      else {
        if (stop > listSize) {
          stop = listSize - 1;
        }
        std::vector<std::string> printArray;
        for (int i = start; i <= stop; i++) {
          printArray.push_back(listStorage[message[1]][i]);
        }
        mutex.unlock();
        //print
        if (printArray.size() != 0 || printArray.size() > listSize) {
          respondMessage = "*" + std::to_string(printArray.size()) + "\r\n";
          for (int i = 0; i < printArray.size(); i++){
            respondMessage += "$" + std::to_string(printArray[i].size()) + "\r\n" + printArray[i] + "\r\n";
          }
        } else {
          respondMessage = "*0\r\n";
        }
      }
      
    }
    else if (message[0] == "LLEN" && message.size() > 1){
      if(listStorage.count(message[1]) == 0){
        respondMessage = ":0\r\n";
      } else {
        respondMessage = ":" + std::to_string(listStorage[message[1]].size()) + "\r\n"; 
      }
    }
    else if (message[0] == "LPOP" && message.size() >= 2) {
      if(listStorage.count(message[1]) == 0) {
        // list doesnt exist
        respondMessage = "$-1\r\n";
      } else if (message.size() == 2) {
        respondMessage = "$" + std::to_string(listStorage[message[1]][0].size()) + "\r\n" + listStorage[message[1]][0] + "\r\n";
      } else {
        mutex.lock();
        int index = std::stoi(message[2]);
        std::vector<std::string> printArray;
        while(index != 0){
          printArray.push_back(listStorage[message[1]][0]);
          listStorage[message[1]].pop_front();
          index--;
        }
        mutex.unlock();

        respondMessage = "*" + std::to_string(printArray.size()) + "\r\n";
        for(const auto& element : printArray){
          respondMessage += "$" + std::to_string(element.size()) + "\r\n" + element + "\r\n";
        }
      }
    }
    else {
      respondMessage = "-ERR unknown command\r\n";
    }
    

    send(client_fd, respondMessage.c_str(), respondMessage.size(), 0);
  }
  close(client_fd);
}

int main(int argc, char **argv) {
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;
  
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
   std::cerr << "Failed to create server socket\n";
   return 1;
  }
  
  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
    std::cerr << "setsockopt failed\n";
    return 1;
  }
  
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);
  
  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) != 0) {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }
  
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::cerr << "listen failed\n";
    return 1;
  }

  while(true){
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    std::cout << "Waiting for a client to connect...\n";

    int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
    if(client_fd < 0){
      std::cerr << "Failed to accept client connection.";
      return 1;
    }
    std::cout << "Client connected.\n";

    std::thread client_thread(handle_client, client_fd);
    client_thread.detach();
  }

  close(server_fd);

  return 0;
}
