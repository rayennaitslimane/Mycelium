#include "MetadataClient.hpp"
#include "StorageClient.hpp"
#include "StorageClientServer.hpp"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: storage_client <host> <port> <metadata_endpoints>\n"
                  << "  metadata_endpoints: comma-separated metadata_server host:port list\n";
        return 1;
    }

    const std::string host = argv[1];
    const int port = std::atoi(argv[2]);
    const auto metadata_endpoints = MetadataClient::parse_endpoints(argv[3]);

    if (port <= 0 || port > 65535) {
        std::cerr << "Error: invalid port number\n";
        return 1;
    }

    auto metadata_store = std::make_shared<MetadataClient>(metadata_endpoints);

    // Comma-separated storage node addresses to register before init().
    if (const char* nodes = std::getenv("MYC_REGISTER_NODES")) {
        std::stringstream ss(nodes);
        std::string address;
        while (std::getline(ss, address, ',')) {
            if (!address.empty()) {
                metadata_store->register_node(address);
            }
        }
    }

    StorageClient client(metadata_store);
    client.init();

    StorageClientServer server(client, host, port);

    std::cout << "Storage client server listening on " << host << ":" << port << "\n";

    server.start();

    return 0;
}
