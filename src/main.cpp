#include <arpa/inet.h>
#include <cstdlib>
#include <cstring>
#include <netdb.h>
#include <print>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>


int main(int argc, char **argv)
{
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0) {
    std::println(stderr, "Failed to create server socket");
    return 1;
  }

  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) <
      0) {
    std::println(stderr, "setsockopt failed");
    return 1;
  }

  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);

  if (bind(server_fd, (struct sockaddr *) &server_addr, sizeof(server_addr)) !=
      0) {
    std::println(stderr, "Failed to bind to port 6379");
    return 1;
  }

  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0) {
    std::println(stderr, "listen failed");
    return 1;
  }

  struct sockaddr_in client_addr;
  int client_addr_len = sizeof(client_addr);
  std::println("Waiting for a client to connect...");

  // You can use print statements as follows for debugging, they'll be visible
  // when running tests.
  std::println("Logs from your program will appear here!");

  // Uncomment the code below to pass the first stage
  //
  auto const client_fd{accept(server_fd,
                              (struct sockaddr *) &client_addr,
                              (socklen_t *) &client_addr_len)};

  if (client_fd < 0) {
    std::println(stderr, "Failed to accept client connection");
    return 1;
  }
  std::println("Client connected");
  std::vector<uint8_t> receiveBuffer(1024);
  while (true) {
    auto const bytesRead{
            recv(client_fd, receiveBuffer.data(), receiveBuffer.size(), 0)};
    if (bytesRead > 0) {
      std::println("Received data: {}",
                   std::string_view(
                           reinterpret_cast<char const *>(receiveBuffer.data()),
                           bytesRead));
      std::println("Sending data back to client");
      char constexpr reply[] = "+PONG\r\n";
      auto const sentBytes{send(client_fd, reply, sizeof(reply) - 1, 0)};
      std::println("Sent {} bytes", sentBytes);
    } else {
      std::println("Client disconnected");
      break;
    }
  }

  close(server_fd);

  return 0;
}
