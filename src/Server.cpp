#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

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
  
  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::cout << "Waiting for a client to connect...\n";

  int client_fd = accept(server_fd, (struct sockaddr *) &client_addr, (socklen_t *) &client_addr_len);
  if(client_fd < 0){
    std::cerr << "Failed to accept client connection.";
    return 1;
  }
  std::cout << "Client connected.\n";

  std::string response = "+PONG\r\n";
  char buffer[4096];

  while(true){
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
    int bytes_read = read(client_fd, buffer, sizeof(buffer));
    // Why does the condition below being true imply that the client has disconnected?
      // first of all, the condition is actually wrong
      // the client has only disconnected if the return value of the read function is 0. 
      // if the return value is less than 0 then its just a general error
      // and ofcourse if above 0 then it works so this needs to be updated to <= 0
    // Why is it that if we cannot read the message then it must mean that the client has disconnected?
    if(bytes_read <= 0){
      std::cerr << "Client disconnected.";
      break;
    }
    // why does response.c_str() work here if send requires type const void * instead of const char *?
    send(client_fd, response.c_str(), response.size(), 0);
  }
  
  close(client_fd);
  close(server_fd);

  return 0;
}
