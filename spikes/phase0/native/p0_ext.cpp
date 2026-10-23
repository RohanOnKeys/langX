//  Copyright (c) 2026 Rohan Pattanayak
//
//  Phase 0 spike module `_p0` (plan sections 16 and 22: spikes A', B, F).
//  Loaded by every coordinator and worker process, so the generic run_node
//  action and its fenced wrapper are registered by the same shared object in
//  every locality.
//
//  Hosting pattern (hpx::start in connect mode, GIL released around blocking
//  HPX calls) follows RayX exp70 Slice 3B native/connector_ext.cpp (Bita).
//  Everything else is new. Spike code only: not the langx API.

#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/init_runtime/finalize.hpp>
#include <hpx/modules/actions.hpp>
#include <hpx/modules/supervision.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/version.hpp>

#include <hpx/supervision_dispatch.hpp>
#include <hpx/supervision_dispatch/server/registry.hpp>
#include <hpx/supervision_dispatch/testing.hpp>

#include "p0_membership.hpp"

#include <unistd.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace py = pybind11;

namespace p0 {

    std::atomic<bool> started{false};
    std::atomic<bool> supervised{false};
    hpx::supervision::registry handle;

    std::int64_t steady_ns(std::chrono::steady_clock::time_point t)
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            t.time_since_epoch())
            .count();
    }

    std::int64_t now_ns()
    {
        return steady_ns(std::chrono::steady_clock::now());
    }

    std::string json_escape(std::string const& s)
    {
        std::string o;
        for (char c : s)
        {
            if (c == '"' || c == '\\')
            {
                o.push_back('\\');
                o.push_back(c);
            }
            else if (c == '\n')
                o += "\\n";
            else if (static_cast<unsigned char>(c) < 0x20)
                o += ' ';
            else
                o.push_back(c);
        }
        return o;
    }

    std::string error_payload(std::string const& kind, std::string const& msg)
    {
        return "{\"ok\":false,\"error_kind\":\"" + json_escape(kind) +
            "\",\"error\":\"" + json_escape(msg) + "\"}";
    }

    char const* event_name(hpx::supervision::event ev)
    {
        using hpx::supervision::event;
        switch (ev)
        {
        case event::unknown:
            return "unknown";
        case event::started:
            return "started";
        case event::running:
            return "running";
        case event::suspending:
            return "suspending";
        case event::completed:
            return "completed";
        case event::failed:
            return "failed";
        case event::losing_locality:
            return "losing_locality";
        }
        return "invalid";
    }

    hpx::id_type locality_gid(std::uint32_t locality)
    {
        return hpx::naming::get_id_from_locality_id(locality);
    }

    void require_started()
    {
        if (!started.load())
            throw std::runtime_error("HPX not started");
    }

    void require_supervised()
    {
        require_started();
        if (!supervised.load())
            throw std::runtime_error("supervision_init() not called");
    }

    // Python node execution happens on one dedicated OS thread per process
    // (plan section 8.3), never on an HPX thread: HPX thread stacks are small,
    // and a blocked HPX worker could starve the supervision heartbeat. The
    // HPX thread running run_node suspends on an hpx::future instead.
    struct py_job
    {
        std::string node;
        std::string payload;
        hpx::promise<std::string> result;
    };

    std::mutex queue_mtx;
    std::condition_variable queue_cv;
    std::deque<std::shared_ptr<py_job>> queue;
    py::object* handler = nullptr;    // leaked on purpose: no teardown order
    std::atomic<bool> py_thread_started{false};
    bool run_inline = std::getenv("P0_PY_INLINE") != nullptr;

    std::string call_handler(std::string const& node, std::string const& payload)
    {
        py::gil_scoped_acquire gil;
        if (handler == nullptr)
            return error_payload("no_handler", "set_node_handler() not called");
        try
        {
            return (*handler)(node, payload).cast<std::string>();
        }
        catch (py::error_already_set const& e)
        {
            return error_payload("python_exception", e.what());
        }
        catch (std::exception const& e)
        {
            return error_payload("cxx_exception", e.what());
        }
    }

    void py_thread_main()
    {
        for (;;)
        {
            std::shared_ptr<py_job> job;
            {
                std::unique_lock<std::mutex> l(queue_mtx);
                queue_cv.wait(l, [] { return !queue.empty(); });
                job = queue.front();
                queue.pop_front();
            }
            job->result.set_value(call_handler(job->node, job->payload));
        }
    }
}    // namespace p0

// The single generic action (plan section 7). Node errors come back as
// values, so any exception seen by the dispatcher is infrastructure.
std::string p0_run_node(std::string node, std::string payload)
{
    if (p0::run_inline)
        return p0::call_handler(node, payload);

    auto job = std::make_shared<p0::py_job>();
    job->node = std::move(node);
    job->payload = std::move(payload);
    hpx::future<std::string> f = job->result.get_future();
    {
        std::lock_guard<std::mutex> l(p0::queue_mtx);
        p0::queue.push_back(job);
    }
    p0::queue_cv.notify_one();
    return f.get();
}
HPX_PLAIN_ACTION(p0_run_node, p0_run_node_action)

// Same registration pattern as the upstream late_component examples, so the
// fenced wrapper has a stable name in every locality loading this module.
using p0_fenced_run_node_action =
    hpx::supervision::make_fenced_action_t<p0_run_node_action>;
HPX_REGISTER_ACTION_DECLARATION(p0_fenced_run_node_action)
HPX_REGISTER_ACTION(p0_fenced_run_node_action)

namespace p0 {

    std::vector<char> to_cstr(std::string const& s)
    {
        std::vector<char> v(s.begin(), s.end());
        v.push_back('\0');
        return v;
    }

    py::dict version_info()
    {
        py::dict d;
        d["hpx_version_full"] = hpx::full_version_as_string();
        d["hpx_git_commit"] = std::string(HPX_HAVE_GIT_COMMIT);
#if defined(HPX_HAVE_FORCE_DISCONNECT)
        d["have_force_disconnect"] = true;
#else
        d["have_force_disconnect"] = false;
#endif
        d["py_inline"] = run_inline;
        return d;
    }

    void start_connect(int hpx_threads, std::vector<std::string> const& extra)
    {
        if (started.load())
            throw std::runtime_error("HPX already started in this process");
        std::vector<std::vector<char>> store;
        store.push_back(to_cstr("p0_ext"));
        store.push_back(to_cstr("--hpx:threads=" + std::to_string(hpx_threads)));
        for (auto const& a : extra)
            store.push_back(to_cstr(a));
        std::vector<char*> argv;
        for (auto& v : store)
            argv.push_back(v.data());
        argv.push_back(nullptr);
        int argc = static_cast<int>(store.size());

        hpx::init_params params;
        params.mode = hpx::runtime_mode::connect;
        py::gil_scoped_release release;
        if (!hpx::start(nullptr, argc, argv.data(), params))
            throw std::runtime_error("hpx::start (connect) failed");
        started.store(true);
    }

    int stop_disconnect()
    {
        if (!started.load())
            return 0;
        // P0_DISCONNECT_MODE=direct runs hpx::disconnect on an HPX thread and
        // waits for it before hpx::stop; the default posts it (exp70 style).
        char const* mode = std::getenv("P0_DISCONNECT_MODE");
        bool const direct = mode != nullptr && std::string(mode) == "direct";
        int rc = 0;
        {
            py::gil_scoped_release release;
            if (direct)
                hpx::run_as_hpx_thread([]() { hpx::disconnect(); });
            else
                hpx::post([]() { hpx::disconnect(); });
            rc = hpx::stop();
        }
        started.store(false);
        return rc;
    }

    // Asks the root (locality 0) to force_disconnect a locality.
    py::dict root_force_disconnect(std::uint32_t locality, double timeout_s)
    {
        require_started();
        std::string status, result, err;
        std::int64_t const t0 = now_ns();
        {
            py::gil_scoped_release release;
            hpx::run_as_hpx_thread([&] {
                try
                {
                    auto f = hpx::async(p0_root_force_disconnect_action(),
                        hpx::naming::get_id_from_locality_id(0), locality);
                    if (f.wait_for(std::chrono::milliseconds(
                            static_cast<std::int64_t>(timeout_s * 1000.0))) !=
                        hpx::future_status::ready)
                    {
                        status = "timeout";
                        return;
                    }
                    result = f.get();
                    status = "ok";
                }
                catch (std::exception const& e)
                {
                    status = "dispatch_failed";
                    err = e.what();
                }
            });
        }
        py::dict d;
        d["status"] = status;
        d["result"] = result;
        d["error"] = err;
        d["elapsed_ns"] = now_ns() - t0;
        return d;
    }

    // Graceful supervision departure: publishes completed and stops the
    // heartbeat and failure-detection loops, which otherwise keep HPX
    // threads alive through hpx::disconnect.
    py::dict supervision_finalize()
    {
        require_started();
        std::string err;
        std::int64_t const t0 = now_ns();
        if (supervised.load())
        {
            py::gil_scoped_release release;
            hpx::run_as_hpx_thread([&] {
                try
                {
                    hpx::supervision::finalize();
                }
                catch (std::exception const& e)
                {
                    err = e.what();
                }
            });
        }
        supervised.store(false);
        py::dict d;
        d["error"] = err;
        d["elapsed_ns"] = now_ns() - t0;
        return d;
    }

    std::uint32_t locality_id()
    {
        require_started();
        py::gil_scoped_release release;
        return hpx::run_as_hpx_thread([] { return hpx::get_locality_id(); });
    }

    std::vector<std::uint32_t> membership()
    {
        require_started();
        py::gil_scoped_release release;
        return hpx::run_as_hpx_thread([] {
            std::vector<std::uint32_t> out;
            for (auto const& id : hpx::find_all_localities())
                out.push_back(hpx::naming::get_locality_id_from_id(id));
            return out;
        });
    }

    // fd_poll_ms > 0 applies the testing hook, which must precede init().
    py::dict supervision_init(int discovery_timeout_ms, int fd_poll_ms)
    {
        require_started();
        struct R
        {
            bool ok = false;
            std::uint64_t epoch = 0;
            std::uint32_t locality = 0;
            std::string error;
        } r;
        {
            py::gil_scoped_release release;
            r = hpx::run_as_hpx_thread([&]() -> R {
                R q;
                try
                {
                    if (fd_poll_ms > 0)
                    {
                        hpx::supervision::testing::
                            set_failure_detection_poll_timeout_for_testing(
                                std::chrono::milliseconds(fd_poll_ms));
                    }
                    handle = hpx::supervision::init(hpx::launch::sync,
                        std::chrono::milliseconds(discovery_timeout_ms));
                    q.epoch =
                        hpx::supervision::query_state(hpx::launch::sync, handle)
                            .epoch;
                    hpx::supervision::publish_event(hpx::launch::sync, handle,
                        hpx::supervision::event::running, q.epoch);
                    q.locality = hpx::get_locality_id();
                    q.ok = true;
                }
                catch (std::exception const& e)
                {
                    q.error = e.what();
                }
                return q;
            });
        }
        supervised.store(r.ok);
        py::dict d;
        d["ok"] = r.ok;
        d["epoch"] = r.epoch;
        d["locality"] = r.locality;
        d["error"] = r.error;
        d["fd_poll_ms"] = fd_poll_ms;
        return d;
    }

    py::list discover_and_join(int timeout_ms)
    {
        require_supervised();
        std::vector<std::pair<std::uint32_t, std::uint64_t>> peers;
        {
            py::gil_scoped_release release;
            peers = hpx::run_as_hpx_thread([&] {
                std::vector<std::pair<std::uint32_t, std::uint64_t>> out;
                for (auto const& p : hpx::supervision::discover_and_join(
                         handle, std::chrono::milliseconds(timeout_ms)))
                {
                    out.emplace_back(
                        hpx::naming::get_locality_id_from_id(p.locality),
                        p.join_epoch);
                }
                return out;
            });
        }
        py::list l;
        for (auto const& [loc, ep] : peers)
        {
            py::dict d;
            d["locality"] = loc;
            d["join_epoch"] = ep;
            l.append(d);
        }
        return l;
    }

    py::list snapshot_peers()
    {
        require_supervised();
        std::vector<std::pair<std::uint32_t, std::uint64_t>> peers;
        {
            py::gil_scoped_release release;
            peers = hpx::run_as_hpx_thread([&] {
                std::vector<std::pair<std::uint32_t, std::uint64_t>> out;
                for (auto const& p : handle.snapshot_peers().get())
                {
                    out.emplace_back(
                        hpx::naming::get_locality_id_from_id(p.peer_locality),
                        p.join_epoch);
                }
                return out;
            });
        }
        py::list l;
        for (auto const& [loc, ep] : peers)
        {
            py::dict d;
            d["locality"] = loc;
            d["join_epoch"] = ep;
            l.append(d);
        }
        return l;
    }

    std::string check_admission(std::uint32_t locality, std::uint64_t epoch)
    {
        require_supervised();
        py::gil_scoped_release release;
        return hpx::run_as_hpx_thread([&]() -> std::string {
            auto const o = hpx::supervision::check_admission(
                locality_gid(locality), epoch);
            if (o == hpx::supervision::dispatch_outcome::admitted)
                return "admitted";
            if (o == hpx::supervision::dispatch_outcome::rejected_fenced)
                return "rejected_fenced";
            return "other_" + std::to_string(static_cast<int>(o));
        });
    }

    py::dict state_dict(hpx::supervision::lifecycle_state const& s,
        std::string const& error)
    {
        py::dict d;
        d["last_event"] = std::string(event_name(s.last_event));
        d["epoch"] = s.epoch;
        d["seq"] = s.event_sequence_number;
        d["timestamp_ns"] = steady_ns(s.timestamp);
        d["error"] = error;
        return d;
    }

    // This locality's local view of a target (for a joined peer: its shadow).
    py::dict peer_state(std::uint32_t locality)
    {
        require_supervised();
        hpx::supervision::lifecycle_state s;
        std::string err;
        {
            py::gil_scoped_release release;
            hpx::run_as_hpx_thread([&] {
                hpx::error_code ec(hpx::throwmode::lightweight);
                s = hpx::supervision::query_state(locality_gid(locality), ec);
                if (ec)
                    err = ec.get_message();
            });
        }
        return state_dict(s, err);
    }

    py::dict own_state()
    {
        require_supervised();
        hpx::supervision::lifecycle_state s;
        std::string err;
        {
            py::gil_scoped_release release;
            hpx::run_as_hpx_thread([&] {
                hpx::error_code ec(hpx::throwmode::lightweight);
                s = hpx::supervision::query_state(hpx::launch::sync, handle, ec);
                if (ec)
                    err = ec.get_message();
            });
        }
        return state_dict(s, err);
    }

    void set_node_handler(py::object fn)
    {
        if (handler == nullptr)
            handler = new py::object(std::move(fn));
        else
            *handler = std::move(fn);
        if (!run_inline && !py_thread_started.exchange(true))
            std::thread(py_thread_main).detach();
    }

    // skip_client_check=true sends the fenced wrapper exactly as dispatch_work
    // does, minus the caller-side early-out, so the target-side re-check can
    // be observed on its own (spike B).
    py::dict dispatch_run_node(std::uint32_t locality, std::uint64_t epoch,
        std::string const& node, std::string const& payload, double timeout_s,
        bool skip_client_check)
    {
        require_supervised();
        struct R
        {
            std::string status, result, error;
            int error_code = 0;
            std::int64_t t0 = 0, t1 = 0;
        } r;
        {
            py::gil_scoped_release release;
            r = hpx::run_as_hpx_thread([&]() -> R {
                R q;
                q.t0 = now_ns();
                hpx::id_type const target = locality_gid(locality);
                try
                {
                    hpx::future<std::string> f = skip_client_check ?
                        hpx::async(p0_fenced_run_node_action(),
                            hpx::colocated(target), p0_run_node_action(), target,
                            epoch, node, payload) :
                        hpx::supervision::dispatch_work<p0_run_node_action>(
                            target, epoch, node, payload);
                    auto const st = f.wait_for(std::chrono::milliseconds(
                        static_cast<std::int64_t>(timeout_s * 1000.0)));
                    if (st != hpx::future_status::ready)
                    {
                        q.status = "timeout";
                    }
                    else
                    {
                        q.result = f.get();
                        q.status = "ok";
                    }
                }
                catch (hpx::exception const& e)
                {
                    q.error_code = static_cast<int>(hpx::get_error(e));
                    q.status = hpx::get_error(e) == hpx::error::target_fenced ?
                        "fenced" :
                        "dispatch_failed";
                    q.error = e.what();
                }
                catch (std::exception const& e)
                {
                    q.status = "dispatch_failed";
                    q.error = e.what();
                }
                q.t1 = now_ns();
                return q;
            });
        }
        py::dict d;
        d["status"] = r.status;
        d["result"] = r.result;
        d["error"] = r.error;
        d["error_code"] = r.error_code;
        d["t0_ns"] = r.t0;
        d["t1_ns"] = r.t1;
        return d;
    }

    py::dict force_disconnect(std::uint32_t locality)
    {
        require_started();
        py::dict d;
#if defined(HPX_HAVE_FORCE_DISCONNECT)
        int rc = 0;
        std::string err;
        std::int64_t t0 = now_ns(), t1 = 0;
        {
            py::gil_scoped_release release;
            hpx::run_as_hpx_thread([&] {
                hpx::error_code ec(hpx::throwmode::lightweight);
                try
                {
                    rc = hpx::force_disconnect(locality_gid(locality), ec);
                    if (ec)
                        err = ec.get_message();
                }
                catch (std::exception const& e)
                {
                    err = e.what();
                }
            });
            t1 = now_ns();
        }
        d["available"] = true;
        d["rc"] = rc;
        d["error"] = err;
        d["elapsed_ns"] = t1 - t0;
#else
        d["available"] = false;
#endif
        return d;
    }
}    // namespace p0

PYBIND11_MODULE(_p0, m)
{
    m.doc() = "langX phase 0 spike module (spikes A', B, F); not the langx API";
    m.def("version_info", &p0::version_info);
    m.def("start_connect", &p0::start_connect, py::arg("hpx_threads"),
        py::arg("extra_args"));
    m.def("stop_disconnect", &p0::stop_disconnect);
    m.def("supervision_finalize", &p0::supervision_finalize);
    m.def("pid", [] { return static_cast<std::int64_t>(::getpid()); });
    m.def("steady_now_ns", &p0::now_ns);
    m.def("locality_id", &p0::locality_id);
    m.def("membership", &p0::membership);
    m.def("supervision_init", &p0::supervision_init,
        py::arg("discovery_timeout_ms") = 5000, py::arg("fd_poll_ms") = 0);
    m.def("discover_and_join", &p0::discover_and_join,
        py::arg("timeout_ms") = 5000);
    m.def("snapshot_peers", &p0::snapshot_peers);
    m.def("check_admission", &p0::check_admission, py::arg("locality"),
        py::arg("epoch"));
    m.def("peer_state", &p0::peer_state, py::arg("locality"));
    m.def("own_state", &p0::own_state);
    m.def("set_node_handler", &p0::set_node_handler, py::arg("fn"));
    m.def("dispatch_run_node", &p0::dispatch_run_node, py::arg("locality"),
        py::arg("epoch"), py::arg("node"), py::arg("payload"),
        py::arg("timeout_s") = 30.0, py::arg("skip_client_check") = false);
    m.def("force_disconnect", &p0::force_disconnect, py::arg("locality"));
    m.def("root_force_disconnect", &p0::root_force_disconnect,
        py::arg("locality"), py::arg("timeout_s") = 30.0);
}
