#pragma once

#include "MetadataServer.hpp"

#include <httplib.h>

#include <string>

// HTTP surface of one MetadataServer. Followers answer leader-only requests
// with 307 to the leader, or 503 while no leader is known.
class MetadataHttpServer {
public:
    MetadataHttpServer(MetadataServer& server, const std::string& host, int port);

    ~MetadataHttpServer();

    MetadataHttpServer(const MetadataHttpServer&) = delete;
    MetadataHttpServer& operator=(const MetadataHttpServer&) = delete;

    void start();

    void stop();

private:
    MetadataServer& server_;
    std::string host_;
    int port_;
    httplib::Server http_;

    void setup_routes();
};
