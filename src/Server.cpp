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

/* Q n A*/
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


      std::vector<std::string> parse_array_command(const std::string& command){
        int index = 0;
        std::vector<std::string> result;
        index++;
        // we need this cuz length could be 2 or 10 -> single digit or double digit or more..
        int array_length;
        while(command[index] != '\r'){
          array_length = array_length * 10 + (command[index] - '0');
          index++;
        }
        index+=2; // skip \r\n
        // now we reach bulk string
        for(int i = 0; i < array_length; i++){
          index++; // move past bulk string indicator: $
          int str_length;
          while(command[index] != '\r'){
            str_length = str_length * 10 + (command[index] - '0');
            index++;
          }
          index+=2;
          std::string message = "";
          while(command[index] != '\r'){
            message+=command[index];
          }
          result.push_back(message);
        }

        return result;
      }


void handle_client(socklen_t client_fd){
  std::string testResponse = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n";
  char buffer[4096];


  while(true){
    
    int bytes_read = read(client_fd, buffer, sizeof(buffer));
    
    if(bytes_read <= 0){
      std::cerr << "Client disconnected or EOF.";
      break;
    }
    
    std::string data(buffer, bytes_read);
    std::vector<std::string> message = parse_array_command(data);
    std::string respondMessage;

    if(data.find("PONG") != std::string::npos){
      respondMessage = "+PONG\r\n";
    }else{
      respondMessage = "$" + std::to_string(message[1].length()) + "\r\n" + message[1] + "\r\n"; 
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
