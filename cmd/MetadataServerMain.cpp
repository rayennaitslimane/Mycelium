#include "MetadataHttpServer.hpp"
#include "MetadataServer.hpp"

#include <exception>
#include <iostream>
#include <string>

namespace {

int port_of(const std::string& endpoint) {
    const auto pos = endpoint.rfind(':');
    const int port = pos == std::string::npos ? 0 : std::stoi(endpoint.substr(pos + 1));
    if (port <= 0 || port > 65535) {
        throw std::invalid_argument("invalid endpoint: " + endpoint);
    }
    return port;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 5) {
        std::cerr << "Usage: metadata_server <id> <raft_host:port> <http_host:port> <data_dir>\n";
        return 1;
    }

    try {
        MetadataServerConfig config;
        config.id = std::stoi(argv[1]);
        config.raft_endpoint = argv[2];
        config.raft_port = port_of(config.raft_endpoint);
        config.http_endpoint = argv[3];
        config.data_dir = argv[4];
        const int http_port = port_of(config.http_endpoint);

        MetadataServer server(config);
        server.start();

        MetadataHttpServer http(server, "0.0.0.0", http_port);
        std::cout << "Metadata server " << config.id << " listening on " << config.http_endpoint
                  << " (raft " << config.raft_endpoint << ")" << std::endl;
        http.start();
    } catch (const std::exception& e) {
        std::cerr << "metadata_server: " << e.what() << "\n";
        return 1;
    }
    return 0;
}
