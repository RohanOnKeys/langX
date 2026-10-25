//  Copyright (c) 2026 Rohan Pattanayak
//
//  SPDX-License-Identifier: BSL-1.0

/// \file _langx.cpp
/// \brief pybind11 bridge between the langx package and an in-process HPX
///        locality (plan section 7).
///
/// One module is loaded by the coordinator and by every worker, so the
/// generic run_node action and its fenced wrapper are registered by the same
/// shared object in every locality.
///
/// Threading:
///   * every HPX call runs on an HPX thread via run_as_hpx_thread, with the
///     GIL released;
///   * Python node functions run on one dedicated OS thread per process,
///     never on an HPX thread (plan section 8.3);
///   * dispatch results are pushed to a completion queue and signalled on a
///     pipe, which the asyncio loop watches; no HPX thread ever takes the
///     GIL.
///
/// The in-process hosting pattern (hpx::start in connect mode, GIL released
/// around blocking calls) follows RayX exp70 Slice 3B
/// native/connector_ext.cpp (Bita).

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/modules/actions.hpp>
#include <hpx/modules/supervision.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/version.hpp>

#include <hpx/supervision_dispatch.hpp>
#include <hpx/supervision_dispatch/server/registry.hpp>
#include <hpx/supervision_dispatch/testing.hpp>

#include <langx/membership.hpp>

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace py = pybind11;

namespace langx {

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
            throw std::runtime_error("supervision not initialized");
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
            else if (static_cast<unsigned char>(c) < 0x20)
                o.push_back(' ');
            else
                o.push_back(c);
        }
        return o;
    }

    // ---- completion queue ---------------------------------------------------

    struct completion
    {
        std::uint64_t id = 0;
        std::string status;    // ok, fenced, failed
        std::string result;
        std::string error;
        int error_code = 0;
        std::int64_t t0_ns = 0;
        std::int64_t t1_ns = 0;
    };

    std::mutex cq_mtx;
    std::vector<completion> cq;
    int cq_pipe[2] = {-1, -1};

    void ensure_pipe()
    {
        std::lock_guard<std::mutex> l(cq_mtx);
        if (cq_pipe[0] >= 0)
            return;
        if (::pipe2(cq_pipe, O_NONBLOCK | O_CLOEXEC) != 0)
            throw std::runtime_error("pipe2 failed");
    }

    void complete(completion c)
    {
        {
            std::lock_guard<std::mutex> l(cq_mtx);
            cq.push_back(std::move(c));
        }
        char const b = 1;
        [[maybe_unused]] auto const n = ::write(cq_pipe[1], &b, 1);
    }

    /// Attaches a continuation that moves the outcome of \p f to the
    /// completion queue. Runs inline wherever \p f becomes ready, so it never
    /// creates an HPX thread, including during runtime teardown.
    void deliver(std::uint64_t id, std::int64_t t0, hpx::future<std::string> f)
    {
        f.then(hpx::launch::sync, [id, t0](hpx::future<std::string>&& r) {
            completion c;
            c.id = id;
            c.t0_ns = t0;
            try
            {
                c.result = r.get();
                c.status = "ok";
            }
            catch (hpx::exception const& e)
            {
                c.error_code = static_cast<int>(hpx::get_error(e));
                c.status = hpx::get_error(e) == hpx::error::target_fenced ?
                    "fenced" :
                    "failed";
                c.error = e.what();
            }
            catch (std::exception const& e)
            {
                c.status = "failed";
                c.error = e.what();
            }
            c.t1_ns = now_ns();
            complete(std::move(c));
        });
    }

    // ---- node execution on the worker --------------------------------------

    struct py_job
    {
        std::string node;
        std::string payload;
        hpx::promise<std::string> result;
    };

    std::mutex job_mtx;
    std::condition_variable job_cv;
    std::deque<std::shared_ptr<py_job>> jobs;
    py::object* handler = nullptr;    // leaked on purpose: no teardown order
    std::atomic<bool> py_thread_started{false};

    std::string error_payload(std::string const& kind, std::string const& msg)
    {
        return "{\"ok\":false,\"error_kind\":\"" + json_escape(kind) +
            "\",\"error\":\"" + json_escape(msg) + "\"}";
    }

    std::string call_handler(std::string const& node, std::string const& payload)
    {
        py::gil_scoped_acquire gil;
        if (handler == nullptr)
            return error_payload("no_handler", "set_node_handler not called");
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
                std::unique_lock<std::mutex> l(job_mtx);
                job_cv.wait(l, [] { return !jobs.empty(); });
                job = jobs.front();
                jobs.pop_front();
            }
            job->result.set_value(call_handler(job->node, job->payload));
        }
    }
}    // namespace langx

/// \brief The single generic action (plan section 7).
///
/// Runs on the worker. Hands the call to the Python thread and suspends the
/// HPX thread until it is done. Node errors come back as values, so any
/// exception seen by the dispatcher is infrastructure.
std::string langx_run_node(std::string node, std::string payload)
{
    auto job = std::make_shared<langx::py_job>();
    job->node = std::move(node);
    job->payload = std::move(payload);
    hpx::future<std::string> f = job->result.get_future();
    {
        std::lock_guard<std::mutex> l(langx::job_mtx);
        langx::jobs.push_back(job);
    }
    langx::job_cv.notify_one();
    return f.get();
}
HPX_PLAIN_ACTION(langx_run_node, langx_run_node_action)

// Stable registration of the fenced wrapper, as in the upstream
// late_component examples.
using langx_fenced_run_node_action =
    hpx::supervision::make_fenced_action_t<langx_run_node_action>;
HPX_REGISTER_ACTION_DECLARATION(langx_fenced_run_node_action)
HPX_REGISTER_ACTION(langx_fenced_run_node_action)

/// \brief Echo action that never enters Python on the worker.
///
/// Measurement only: comparing it with run_node separates the worker's
/// Python cost from HPX and the coordinator's Python bridge.
std::string langx_echo(std::string payload)
{
    return payload;
}
HPX_PLAIN_ACTION(langx_echo, langx_echo_action)

using langx_fenced_echo_action =
    hpx::supervision::make_fenced_action_t<langx_echo_action>;
HPX_REGISTER_ACTION_DECLARATION(langx_fenced_echo_action)
HPX_REGISTER_ACTION(langx_fenced_echo_action)

namespace langx {

    py::dict version_info()
    {
        py::dict d;
        d["hpx_version"] = hpx::full_version_as_string();
        d["hpx_git_commit"] = std::string(HPX_HAVE_GIT_COMMIT);
#if defined(HPX_HAVE_FORCE_DISCONNECT)
        d["have_force_disconnect"] = true;
#else
        d["have_force_disconnect"] = false;
#endif
        return d;
    }

    void start(int threads, std::vector<std::string> const& extra)
    {
        if (started.load())
            throw std::runtime_error("HPX already started in this process");
        std::vector<std::string> store{"langx", "--hpx:threads=" +
                std::to_string(threads)};
        store.insert(store.end(), extra.begin(), extra.end());
        std::vector<char*> argv;
        for (auto& s : store)
            argv.push_back(s.data());
        argv.push_back(nullptr);
        int argc = static_cast<int>(store.size());

        hpx::init_params params;
        params.mode = hpx::runtime_mode::connect;
        py::gil_scoped_release release;
        if (!hpx::start(nullptr, argc, argv.data(), params))
            throw std::runtime_error("hpx::start (connect) failed");
        started.store(true);
    }

    int stop()
    {
        if (!started.load())
            return 0;
        int rc = 0;
        {
            py::gil_scoped_release release;
            // Posted, not awaited: an awaited hpx::disconnect never returned
            // in phase 0.
            hpx::post([] { hpx::disconnect(); });
            rc = hpx::stop();
        }
        started.store(false);
        return rc;
    }

    py::tuple init(int discovery_timeout_ms, int poll_timeout_ms)
    {
        require_started();
        std::uint64_t epoch = 0;
        std::uint32_t locality = 0;
        {
            py::gil_scoped_release release;
            hpx::run_as_hpx_thread([&] {
                if (poll_timeout_ms > 0)
                {
                    hpx::supervision::testing::
                        set_failure_detection_poll_timeout_for_testing(
                            std::chrono::milliseconds(poll_timeout_ms));
                }
                handle = hpx::supervision::init(hpx::launch::sync,
                    std::chrono::milliseconds(discovery_timeout_ms));
                epoch =
                    hpx::supervision::query_state(hpx::launch::sync, handle)
                        .epoch;
                hpx::supervision::publish_event(hpx::launch::sync, handle,
                    hpx::supervision::event::running, epoch);
                locality = hpx::get_locality_id();
            });
        }
        supervised.store(true);
        return py::make_tuple(locality, epoch);
    }

    std::int64_t finalize()
    {
        require_started();
        std::int64_t const t0 = now_ns();
        if (supervised.exchange(false))
        {
            py::gil_scoped_release release;
            hpx::run_as_hpx_thread([] { hpx::supervision::finalize(); });
        }
        return now_ns() - t0;
    }

    bool is_initialized()
    {
        return supervised.load();
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

    using peer_list = std::vector<std::pair<std::uint32_t, std::uint64_t>>;

    peer_list discover_and_join(int timeout_ms)
    {
        require_supervised();
        py::gil_scoped_release release;
        return hpx::run_as_hpx_thread([&] {
            peer_list out;
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

    peer_list snapshot_peers()
    {
        require_supervised();
        py::gil_scoped_release release;
        return hpx::run_as_hpx_thread([] {
            peer_list out;
            for (auto const& p : handle.snapshot_peers().get())
            {
                out.emplace_back(
                    hpx::naming::get_locality_id_from_id(p.peer_locality),
                    p.join_epoch);
            }
            return out;
        });
    }

    bool admitted(std::uint32_t locality, std::uint64_t epoch)
    {
        require_supervised();
        py::gil_scoped_release release;
        return hpx::run_as_hpx_thread([&] {
            return hpx::supervision::check_admission(
                       locality_gid(locality), epoch) ==
                hpx::supervision::dispatch_outcome::admitted;
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

    /// This locality's view of \p locality (for a joined peer, its shadow).
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
        if (!py_thread_started.exchange(true))
            std::thread(py_thread_main).detach();
    }

    int completion_fd()
    {
        ensure_pipe();
        return cq_pipe[0];
    }

    py::list drain_completions()
    {
        ensure_pipe();
        char buf[256];
        while (::read(cq_pipe[0], buf, sizeof(buf)) > 0)
        {
        }
        std::vector<completion> batch;
        {
            std::lock_guard<std::mutex> l(cq_mtx);
            batch.swap(cq);
        }
        py::list out;
        for (auto const& c : batch)
        {
            py::dict d;
            d["id"] = c.id;
            d["status"] = c.status;
            d["result"] = py::bytes(c.result);
            d["error"] = c.error;
            d["error_code"] = c.error_code;
            d["t0_ns"] = c.t0_ns;
            d["t1_ns"] = c.t1_ns;
            out.append(d);
        }
        return out;
    }

    /// Sends run_node(node, payload) to \p locality under supervision
    /// fencing. The outcome arrives later through drain_completions().
    void dispatch_node(std::uint64_t id, std::uint32_t locality,
        std::uint64_t epoch, std::string const& node,
        std::string const& payload)
    {
        require_supervised();
        ensure_pipe();
        py::gil_scoped_release release;
        hpx::run_as_hpx_thread([&] {
            std::int64_t const t0 = now_ns();
            hpx::future<std::string> f;
            try
            {
                f = hpx::supervision::dispatch_work<langx_run_node_action>(
                    locality_gid(locality), epoch, node, payload);
            }
            catch (...)
            {
                f = hpx::make_exceptional_future<std::string>(
                    std::current_exception());
            }
            deliver(id, t0, std::move(f));
        });
    }

    /// Sends langx_echo(payload) under supervision fencing; the outcome
    /// arrives through drain_completions(). Measurement only.
    void dispatch_echo(std::uint64_t id, std::uint32_t locality,
        std::uint64_t epoch, std::string const& payload)
    {
        require_supervised();
        ensure_pipe();
        py::gil_scoped_release release;
        hpx::run_as_hpx_thread([&] {
            std::int64_t const t0 = now_ns();
            hpx::future<std::string> f;
            try
            {
                f = hpx::supervision::dispatch_work<langx_echo_action>(
                    locality_gid(locality), epoch, payload);
            }
            catch (...)
            {
                f = hpx::make_exceptional_future<std::string>(
                    std::current_exception());
            }
            deliver(id, t0, std::move(f));
        });
    }

    /// Asks the root to force_disconnect \p locality. The outcome (JSON from
    /// langx_root_force_disconnect) arrives through drain_completions().
    void request_force_disconnect(std::uint64_t id, std::uint32_t locality)
    {
        require_started();
        ensure_pipe();
        py::gil_scoped_release release;
        hpx::run_as_hpx_thread([&] {
            std::int64_t const t0 = now_ns();
            hpx::future<std::string> f;
            try
            {
                f = hpx::async(langx_root_force_disconnect_action(),
                    locality_gid(0), locality);
            }
            catch (...)
            {
                f = hpx::make_exceptional_future<std::string>(
                    std::current_exception());
            }
            deliver(id, t0, std::move(f));
        });
    }
}    // namespace langx

PYBIND11_MODULE(_langx, m)
{
    m.doc() = "langx native bridge to an in-process HPX locality";
    m.def("version_info", &langx::version_info);
    m.def("start", &langx::start, py::arg("threads"), py::arg("args"));
    m.def("stop", &langx::stop);
    m.def("init", &langx::init, py::arg("discovery_timeout_ms"),
        py::arg("poll_timeout_ms"));
    m.def("finalize", &langx::finalize);
    m.def("is_initialized", &langx::is_initialized);
    m.def("pid", [] { return static_cast<std::int64_t>(::getpid()); });
    m.def("steady_now_ns", [] { return langx::now_ns(); });
    m.def("locality_id", &langx::locality_id);
    m.def("membership", &langx::membership);
    m.def("discover_and_join", &langx::discover_and_join, py::arg("timeout_ms"));
    m.def("snapshot_peers", &langx::snapshot_peers);
    m.def("admitted", &langx::admitted, py::arg("locality"), py::arg("epoch"));
    m.def("peer_state", &langx::peer_state, py::arg("locality"));
    m.def("own_state", &langx::own_state);
    m.def("set_node_handler", &langx::set_node_handler, py::arg("fn"));
    m.def("completion_fd", &langx::completion_fd);
    m.def("drain_completions", &langx::drain_completions);
    m.def("dispatch_node", &langx::dispatch_node, py::arg("id"),
        py::arg("locality"), py::arg("epoch"), py::arg("node"),
        py::arg("payload"));
    m.def("dispatch_echo", &langx::dispatch_echo, py::arg("id"),
        py::arg("locality"), py::arg("epoch"), py::arg("payload"));
    m.def("request_force_disconnect", &langx::request_force_disconnect,
        py::arg("id"), py::arg("locality"));
}
