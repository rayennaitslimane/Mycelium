#include "MetadataClient.hpp"

#include "MetadataCommand.hpp"
#include "MetadataJson.hpp"

#include <botan/auto_rng.h>
#include <botan/hex.h>
#include <httplib.h>

#include <algorithm>
#include <chrono>
#include <sstream>
#include <stdexcept>
#include <thread>

using nlohmann::json;

namespace {

constexpr auto kCallDeadline = std::chrono::seconds(10);
constexpr auto kInitialBackoff = std::chrono::milliseconds(50);
constexpr auto kMaxBackoff = std::chrono::milliseconds(1000);

std::string new_request_id() {
    thread_local Botan::AutoSeeded_RNG rng;
    return Botan::hex_encode(rng.random_vec(16));
}

// "http://host:port/path" -> "host:port"
std::string endpoint_from_location(const std::string& location) {
    const auto start = location.find("://");
    const std::string rest = start == std::string::npos ? location : location.substr(start + 3);
    return rest.substr(0, rest.find('/'));
}

}  // namespace

MetadataClient::MetadataClient(std::vector<std::string> endpoints)
    : endpoints_(std::move(endpoints)) {
    if (endpoints_.empty()) {
        throw std::invalid_argument("MetadataClient: no endpoints");
    }
}

std::vector<std::string> MetadataClient::parse_endpoints(const std::string& csv) {
    std::vector<std::string> out;
    std::stringstream ss(csv);
    std::string endpoint;
    while (std::getline(ss, endpoint, ',')) {
        if (!endpoint.empty()) {
            out.push_back(endpoint);
        }
    }
    return out;
}

MetadataClient::Reply MetadataClient::call(
    const std::string& method,
    const std::string& path,
    const json& body
) {
    const auto deadline = std::chrono::steady_clock::now() + kCallDeadline;
    auto backoff = kInitialBackoff;
    const std::string payload = body.is_null() ? std::string{} : body.dump();

    while (true) {
        std::string endpoint;
        {
            std::lock_guard<std::mutex> lock(mu_);
            endpoint = leader_.empty() ? endpoints_[next_ % endpoints_.size()] : leader_;
        }

        httplib::Client http("http://" + endpoint);
        http.set_connection_timeout(std::chrono::seconds(1));
        http.set_read_timeout(std::chrono::seconds(5));

        httplib::Result res;
        if (method == "GET") {
            res = http.Get(path);
        } else if (method == "PUT") {
            res = http.Put(path, payload, "application/json");
        } else if (method == "POST") {
            res = http.Post(path, payload, "application/json");
        } else {
            res = http.Delete(path, payload, "application/json");
        }

        if (res && res->status == 307) {
            std::lock_guard<std::mutex> lock(mu_);
            leader_ = endpoint_from_location(res->get_header_value("Location"));
        } else if (res && res->status != 503) {
            {
                std::lock_guard<std::mutex> lock(mu_);
                leader_ = endpoint;
            }
            Reply reply{res->status, json::parse(res->body, nullptr, false)};
            if (reply.status == 400) {
                throw std::invalid_argument(reply.body.value("error", "bad request"));
            }
            if (reply.status != 200 && reply.status != 404) {
                throw std::runtime_error("MetadataClient: " + method + " " + path +
                                         " returned " + std::to_string(reply.status));
            }
            return reply;
        } else {
            std::lock_guard<std::mutex> lock(mu_);
            leader_.clear();
            ++next_;
        }

        if (std::chrono::steady_clock::now() + backoff > deadline) {
            throw std::runtime_error("MetadataClient: no leader reachable for " + method + " " + path);
        }
        if (!res || res->status == 503) {
            std::this_thread::sleep_for(backoff);
            backoff = std::min(backoff * 2, kMaxBackoff);
        }
    }
}

bool MetadataClient::mutate(const std::string& method, const std::string& path, json body) {
    // Generated once so every retry of this call carries the same id.
    body["request_id"] = new_request_id();
    return call(method, path, body).body.at("ok").get<bool>();
}

void MetadataClient::put(const ObjectMetadata& metadata) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::Put;
    cmd.metadata = metadata;
    validate_command(cmd);
    mutate("PUT", "/meta/objects/" + metadata.id, json{{"metadata", metadata}});
}

bool MetadataClient::conditional_put(const ObjectMetadata& metadata, int expected_version) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::ConditionalPut;
    cmd.metadata = metadata;
    validate_command(cmd);
    return mutate("POST", "/meta/objects/" + metadata.id + "/cas",
                  json{{"metadata", metadata}, {"expected_version", expected_version}});
}

std::optional<ObjectMetadata> MetadataClient::get(const std::string& id) {
    const Reply reply = call("GET", "/meta/objects/" + id);
    if (reply.status == 404) {
        return std::nullopt;
    }
    return reply.body.get<ObjectMetadata>();
}

bool MetadataClient::remove(const std::string& id) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::Remove;
    cmd.key = id;
    validate_command(cmd);
    return mutate("DELETE", "/meta/objects/" + id, json::object());
}

std::vector<ObjectMetadata> MetadataClient::list() {
    return call("GET", "/meta/objects").body.get<std::vector<ObjectMetadata>>();
}

void MetadataClient::register_node(const std::string& address) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::RegisterNode;
    cmd.key = address;
    validate_command(cmd);
    mutate("PUT", "/meta/nodes/" + address, json::object());
}

bool MetadataClient::unregister_node(const std::string& address) {
    MetadataCommand cmd;
    cmd.op = MetadataOp::UnregisterNode;
    cmd.key = address;
    validate_command(cmd);
    return mutate("DELETE", "/meta/nodes/" + address, json::object());
}

std::vector<std::string> MetadataClient::list_nodes() {
    return call("GET", "/meta/nodes").body.get<std::vector<std::string>>();
}
