#include "MetadataHttpServer.hpp"

#include "MetadataJson.hpp"

#include <cstdlib>
#include <functional>
#include <stdexcept>
#include <string>

using nlohmann::json;

namespace {

using Handler = std::function<json(const httplib::Request&, httplib::Response&)>;

void send_error(httplib::Response& res, int status, const std::string& message) {
    res.status = status;
    res.set_content(json{{"error", message}}.dump(), "application/json");
}

json parse_body(const httplib::Request& req) {
    return req.body.empty() ? json::object() : json::parse(req.body);
}

json mutation_result(MetadataServer& server, const MetadataCommand& cmd) {
    return json{{"ok", server.propose(cmd).ok}};
}

}  // namespace

MetadataHttpServer::MetadataHttpServer(MetadataServer& server, const std::string& host, int port)
    : server_(server), host_(host), port_(port) {
    setup_routes();
}

MetadataHttpServer::~MetadataHttpServer() {
    stop();
}

void MetadataHttpServer::start() {
    http_.listen(host_, port_);
}

void MetadataHttpServer::stop() {
    http_.stop();
}

void MetadataHttpServer::setup_routes() {
    // Wraps a handler with the shared NotLeader/validation error mapping.
    auto wrap = [this](Handler handler) {
        return [this, handler](const httplib::Request& req, httplib::Response& res) {
            try {
                const json body = handler(req, res);
                res.set_content(body.dump(), "application/json");
            } catch (const NotLeaderError& e) {
                const std::string leader = server_.leader_http_endpoint();
                if (e.leader_id < 0 || e.leader_id == server_.id() || leader.empty()) {
                    send_error(res, 503, e.what());
                } else {
                    res.status = 307;
                    res.set_header("Location", "http://" + leader + req.path);
                }
            } catch (const std::invalid_argument& e) {
                send_error(res, 400, e.what());
            } catch (const json::exception& e) {
                send_error(res, 400, e.what());
            } catch (const std::exception& e) {
                send_error(res, 503, e.what());
            }
        };
    };

    http_.Get("/meta/objects", wrap([this](const httplib::Request&, httplib::Response&) {
        return json(server_.read().list());
    }));

    http_.Get("/meta/objects/:id", wrap([this](const httplib::Request& req, httplib::Response& res) {
        const auto metadata = server_.read().get(req.path_params.at("id"));
        if (!metadata) {
            res.status = 404;
            return json{{"error", "not found"}};
        }
        return json(*metadata);
    }));

    http_.Put("/meta/objects/:id", wrap([this](const httplib::Request& req, httplib::Response&) {
        const json input = parse_body(req);
        MetadataCommand cmd;
        cmd.op = MetadataOp::Put;
        input.at("metadata").get_to(cmd.metadata);
        cmd.metadata.id = req.path_params.at("id");
        cmd.request_id = input.value("request_id", "");
        return mutation_result(server_, cmd);
    }));

    http_.Post("/meta/objects/:id/cas", wrap([this](const httplib::Request& req, httplib::Response&) {
        const json input = parse_body(req);
        MetadataCommand cmd;
        cmd.op = MetadataOp::ConditionalPut;
        input.at("metadata").get_to(cmd.metadata);
        cmd.metadata.id = req.path_params.at("id");
        input.at("expected_version").get_to(cmd.expected_version);
        cmd.request_id = input.value("request_id", "");
        return mutation_result(server_, cmd);
    }));

    http_.Delete("/meta/objects/:id", wrap([this](const httplib::Request& req, httplib::Response&) {
        MetadataCommand cmd;
        cmd.op = MetadataOp::Remove;
        cmd.key = req.path_params.at("id");
        cmd.request_id = parse_body(req).value("request_id", "");
        return mutation_result(server_, cmd);
    }));

    http_.Get("/meta/nodes", wrap([this](const httplib::Request&, httplib::Response&) {
        return json(server_.read().list_nodes());
    }));

    http_.Put("/meta/nodes/:address", wrap([this](const httplib::Request& req, httplib::Response&) {
        MetadataCommand cmd;
        cmd.op = MetadataOp::RegisterNode;
        cmd.key = req.path_params.at("address");
        cmd.request_id = parse_body(req).value("request_id", "");
        return mutation_result(server_, cmd);
    }));

    http_.Delete("/meta/nodes/:address", wrap([this](const httplib::Request& req, httplib::Response&) {
        MetadataCommand cmd;
        cmd.op = MetadataOp::UnregisterNode;
        cmd.key = req.path_params.at("address");
        cmd.request_id = parse_body(req).value("request_id", "");
        return mutation_result(server_, cmd);
    }));

    // Never redirects, so tooling can inspect every member.
    http_.Get("/status", wrap([this](const httplib::Request&, httplib::Response&) {
        return json{{"id", server_.id()},
                    {"leader_id", server_.leader_id()},
                    {"is_leader", server_.is_leader()},
                    {"last_applied", server_.last_applied()}};
    }));

    http_.Post("/admin/servers", wrap([this](const httplib::Request& req, httplib::Response&) {
        const json input = parse_body(req);
        server_.add_server(
            input.at("id").get<int>(),
            input.at("raft_endpoint").get<std::string>(),
            input.at("http_endpoint").get<std::string>());
        return json{{"ok", true}};
    }));

    http_.Post("/admin/fault", wrap([this](const httplib::Request& req, httplib::Response& res) {
        const char* enabled = std::getenv("MYC_ENABLE_FAULT_INJECTION");
        if (!enabled || std::string(enabled) != "1") {
            res.status = 404;
            return json{{"error", "fault injection disabled"}};
        }
        server_.arm_fault(parse_body(req).at("point").get<std::string>());
        return json{{"ok", true}};
    }));
}
