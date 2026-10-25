//  Copyright (c) 2026 Rohan Pattanayak
//
//  SPDX-License-Identifier: BSL-1.0

/// \file supervision_gaps_repro.cpp
/// \brief Reproducers for HPX supervision_dispatch behaviours seen on master
///        cc2be553. No Python. One binary; run_repro.sh starts a root and two
///        connect-mode localities A and B, kills B where a case needs it,
///        and prints the lines starting with "R:".
///
/// Cases (--r-case):
///   observer  B's init joins A (B observes A). Kill B. A prints its own
///             admission and lifecycle state. --r-b-join 0 is the control.
///   inflight  A joins B, dispatches a 3 s action to B, B is killed 0.3 s
///             in. A prints when B is fenced and whether the future is ready.
///   remove    A joins B, B is killed. A waits for the fence, calls
///             force_disconnect(B) itself, then asks the root to. Prints
///             snapshot_peers() and find_all_localities() at each step.
///   epoch     A joins B. A prints check_admission and the dispatch_work
///             outcome for epochs 0, join, join + 1 and join + 100.
///   rtt       A joins B and times round trips of a small echo action to B:
///             a plain hpx::async, then the same through dispatch_work. The
///             HPX-only floor for the langx dispatch overhead.
///   leave     hpx#7825. A and B both join until each sees the other. A
///             keeps running. B then leaves: supervision::finalize(), an
///             optional pause, posted hpx::disconnect(), hpx::stop(). B
///             prints "clean" only if hpx::stop() returns.
///
/// Options: --r-role root|a|b, --r-case, --r-dir DIR (handshake files),
/// --r-poll-ms N (failure-detection poll timeout, testing hook),
/// --r-b-join 0|1 (observer case only), --r-pause-ms N (leave case only:
/// pause between finalize and disconnect). Everything else goes to HPX.

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/include/actions.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/init_runtime/finalize.hpp>
#include <hpx/modules/supervision.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>

#include <hpx/supervision_dispatch.hpp>
#include <hpx/supervision_dispatch/server/registry.hpp>
#include <hpx/supervision_dispatch/testing.hpp>

#include <sys/stat.h>

#include <chrono>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>

int repro_sleep(int ms)
{
    hpx::this_thread::sleep_for(std::chrono::milliseconds(ms));
    return ms;
}
HPX_PLAIN_ACTION(repro_sleep, repro_sleep_action)

using repro_fenced_sleep_action =
    hpx::supervision::make_fenced_action_t<repro_sleep_action>;
HPX_REGISTER_ACTION_DECLARATION(repro_fenced_sleep_action)
HPX_REGISTER_ACTION(repro_fenced_sleep_action)

std::string repro_echo(std::string s)
{
    return s;
}
HPX_PLAIN_ACTION(repro_echo, repro_echo_action)

using repro_fenced_echo_action =
    hpx::supervision::make_fenced_action_t<repro_echo_action>;
HPX_REGISTER_ACTION_DECLARATION(repro_fenced_echo_action)
HPX_REGISTER_ACTION(repro_fenced_echo_action)

namespace {

    using clk = std::chrono::steady_clock;
    namespace sv = hpx::supervision;

    std::string dir, role, case_name;
    int poll_ms = 500;
    bool b_join = true;
    sv::registry handle;
    std::uint64_t own_epoch = 0;

    std::string arg(int argc, char** argv, char const* key, char const* dflt)
    {
        for (int i = 1; i + 1 < argc; ++i)
            if (std::strcmp(argv[i], key) == 0)
                return argv[i + 1];
        return dflt;
    }

    void say(char const* fmt, ...)
    {
        std::printf("R: %s %s ", role.c_str(), case_name.c_str());
        va_list ap;
        va_start(ap, fmt);
        std::vprintf(fmt, ap);
        va_end(ap);
        std::printf("\n");
        std::fflush(stdout);
    }

    std::string file(std::string const& name)
    {
        return dir + "/" + name;
    }

    bool exists(std::string const& name)
    {
        struct stat st{};
        return ::stat(file(name).c_str(), &st) == 0;
    }

    void put(std::string const& name, std::string const& text)
    {
        std::string const tmp = file(name) + ".tmp";
        {
            std::ofstream f(tmp, std::ios::trunc);
            f << text;
        }
        std::rename(tmp.c_str(), file(name).c_str());
    }

    std::string get(std::string const& name)
    {
        std::ifstream f(file(name));
        std::stringstream s;
        s << f.rdbuf();
        return s.str();
    }

    void nap(int ms)
    {
        hpx::this_thread::sleep_for(std::chrono::milliseconds(ms));
    }

    bool wait_file(std::string const& name, int timeout_s)
    {
        auto const deadline = clk::now() + std::chrono::seconds(timeout_s);
        while (clk::now() < deadline)
        {
            if (exists(name))
                return true;
            nap(20);
        }
        return false;
    }

    double since(clk::time_point t0)
    {
        return std::chrono::duration<double>(clk::now() - t0).count();
    }

    char const* outcome(sv::dispatch_outcome o)
    {
        return o == sv::dispatch_outcome::admitted ? "admitted" :
            o == sv::dispatch_outcome::rejected_fenced ? "rejected_fenced" :
                                                         "other";
    }

    char const* event_name(sv::event e)
    {
        switch (e)
        {
        case sv::event::unknown:
            return "unknown";
        case sv::event::started:
            return "started";
        case sv::event::running:
            return "running";
        case sv::event::suspending:
            return "suspending";
        case sv::event::completed:
            return "completed";
        case sv::event::failed:
            return "failed";
        case sv::event::losing_locality:
            return "losing_locality";
        }
        return "invalid";
    }

    std::uint32_t loc_of(hpx::id_type const& id)
    {
        return hpx::naming::get_locality_id_from_id(id);
    }

    std::string snapshot()
    {
        std::string s = "[";
        for (auto const& p : handle.snapshot_peers().get())
            s += std::to_string(loc_of(p.peer_locality)) + " ";
        return s + "]";
    }

    std::string membership()
    {
        std::string s = "[";
        for (auto const& id : hpx::find_all_localities())
            s += std::to_string(loc_of(id)) + " ";
        return s + "]";
    }

    /// Joins until a peer other than the root (locality 0) is joined.
    sv::discovered_peer join_b(int timeout_s)
    {
        auto const deadline = clk::now() + std::chrono::seconds(timeout_s);
        while (clk::now() < deadline)
        {
            for (auto const& p :
                sv::discover_and_join(handle, std::chrono::milliseconds(1000)))
            {
                if (loc_of(p.locality) != 0)
                    return p;
            }
        }
        say("FAILED to join B");
        std::_Exit(3);
    }

    void init_supervision(int discovery_ms)
    {
        sv::testing::set_failure_detection_poll_timeout_for_testing(
            std::chrono::milliseconds(poll_ms));
        handle = sv::init(hpx::launch::sync,
            std::chrono::milliseconds(discovery_ms));
        own_epoch = sv::query_state(hpx::launch::sync, handle).epoch;
        sv::publish_event(
            hpx::launch::sync, handle, sv::event::running, own_epoch);
    }

    // ---- roles ---------------------------------------------------------

    void run_root()
    {
        init_supervision(2000);
        put("root.ready", "0");
        for (;;)
        {
            if (exists("root.fd_request") && !exists("root.fd_done"))
            {
                auto const loc = static_cast<std::uint32_t>(
                    std::stoul(get("root.fd_request")));
                hpx::error_code ec(hpx::throwmode::lightweight);
                int const rc = hpx::force_disconnect(
                    hpx::naming::get_id_from_locality_id(loc), ec);
                std::string const err = ec ? ec.get_message() : "";
                say("force_disconnect(%u) rc=%d error='%s' membership=%s", loc,
                    rc, err.c_str(), membership().c_str());
                put("root.fd_done", std::to_string(rc));
            }
            nap(100);
        }
    }

    void run_b()
    {
        bool const joins_a =
            (case_name == "observer" && b_join) || case_name == "leave";
        init_supervision(case_name == "observer" && !b_join ? 0 : 2000);
        if (joins_a)
            join_b(30);    // B joins A, so B becomes an observer of A
        say("ready locality=%u epoch=%llu joined=%s", hpx::get_locality_id(),
            static_cast<unsigned long long>(own_epoch), snapshot().c_str());
        put("b.ready", "1");
        if (case_name == "leave")
        {
            wait_file("b.leave", 120);
            return;    // main() runs the departure, outside any HPX thread
        }
        for (;;)
            nap(1000);
    }

    void a_leave()
    {
        init_supervision(2000);
        join_b(30);    // A joins B too, so each has the other as a peer
        say("ready locality=%u joined=%s", hpx::get_locality_id(),
            snapshot().c_str());
        put("a.ready", "1");
        for (;;)
            nap(1000);
    }

    void a_observer()
    {
        init_supervision(2000);
        put("a.ready", "1");
        wait_file("b.ready", 60);
        auto const self = [] {
            auto const st = sv::query_state(hpx::launch::sync, handle);
            return std::string(outcome(sv::check_admission(
                       hpx::find_here(), own_epoch))) +
                " own_state=" + event_name(st.last_event);
        };
        say("before kill: admission=%s", self().c_str());
        wait_file("b.killed", 60);
        auto const t0 = clk::now();
        std::string last;
        while (since(t0) < 5.0)
        {
            std::string const now = self();
            if (now != last)
            {
                say("t=%.2fs after kill: admission=%s", since(t0), now.c_str());
                last = now;
            }
            nap(50);
        }
        say("t=5.00s after kill: admission=%s", self().c_str());
    }

    void a_inflight()
    {
        init_supervision(2000);
        auto const b = join_b(30);
        put("a.ready", "1");
        auto f = sv::dispatch_work<repro_sleep_action>(
            b.locality, b.join_epoch, 3000);
        say("dispatched 3000 ms action to B (locality %u)", loc_of(b.locality));
        put("a.dispatched", "1");
        wait_file("b.killed", 60);
        auto const t0 = clk::now();
        double fenced_at = -1;
        while (since(t0) < 30.0 && !f.is_ready())
        {
            if (fenced_at < 0 &&
                sv::check_admission(b.locality, b.join_epoch) ==
                    sv::dispatch_outcome::rejected_fenced)
            {
                fenced_at = since(t0);
                say("t=%.2fs after kill: B fenced, future ready=%d", fenced_at,
                    f.is_ready() ? 1 : 0);
            }
            nap(50);
        }
        if (!f.is_ready())
        {
            say("t=%.2fs after kill: future still not ready (B fenced at %.2fs)",
                since(t0), fenced_at);
            return;
        }
        try
        {
            say("t=%.2fs after kill: future ready, value=%d", since(t0), f.get());
        }
        catch (hpx::exception const& e)
        {
            say("t=%.2fs after kill: future ready with error %s", since(t0),
                hpx::get_error_name(hpx::get_error(e)));
        }
    }

    void a_remove()
    {
        init_supervision(2000);
        auto const b = join_b(30);
        auto const bl = loc_of(b.locality);
        put("a.ready", "1");
        say("before kill: B=%u snapshot=%s membership=%s", bl,
            snapshot().c_str(), membership().c_str());
        wait_file("b.killed", 60);
        auto const t0 = clk::now();
        while (since(t0) < 30.0 &&
            sv::check_admission(b.locality, b.join_epoch) !=
                sv::dispatch_outcome::rejected_fenced)
            nap(20);
        say("t=%.2fs after kill: B fenced, snapshot=%s membership=%s",
            since(t0), snapshot().c_str(), membership().c_str());

        hpx::error_code ec(hpx::throwmode::lightweight);
        int const rc = hpx::force_disconnect(b.locality, ec);
        say("force_disconnect(B) from A: rc=%d error='%s' membership=%s", rc,
            ec ? ec.get_message().c_str() : "", membership().c_str());

        put("root.fd_request", std::to_string(bl));
        wait_file("root.fd_done", 30);
        nap(500);
        say("after root force_disconnect(B) rc=%s: snapshot=%s membership=%s",
            get("root.fd_done").c_str(), snapshot().c_str(),
            membership().c_str());
        nap(2000);
        say("2 s later: snapshot=%s membership=%s", snapshot().c_str(),
            membership().c_str());
    }

    /// Times ``n`` sequential calls of ``call``; prints p50, p99 and mean in us.
    template <typename F>
    void time_calls(char const* label, int n, F&& call)
    {
        for (int i = 0; i != 200; ++i)    // warm up connections and caches
            call();
        std::vector<double> us;
        us.reserve(n);
        for (int i = 0; i != n; ++i)
        {
            auto const t0 = clk::now();
            call();
            us.push_back(
                std::chrono::duration<double, std::micro>(clk::now() - t0)
                    .count());
        }
        double sum = 0;
        for (double v : us)
            sum += v;
        std::sort(us.begin(), us.end());
        say("%s n=%d p50_us=%.1f p99_us=%.1f mean_us=%.1f", label, n,
            us[us.size() / 2], us[us.size() * 99 / 100], sum / n);
    }

    void a_rtt()
    {
        init_supervision(2000);
        auto const b = join_b(30);
        put("a.ready", "1");
        std::string const payload(100, 'x');
        int const n = 2000;
        time_calls("plain_async", n, [&] {
            hpx::async(repro_echo_action(), b.locality, payload).get();
        });
        time_calls("dispatch_work", n, [&] {
            hpx::supervision::dispatch_work<repro_echo_action>(
                b.locality, b.join_epoch, payload)
                .get();
        });
    }

    void a_epoch()
    {
        init_supervision(2000);
        auto const b = join_b(30);
        put("a.ready", "1");
        std::uint64_t const je = b.join_epoch;
        for (std::uint64_t e : {std::uint64_t(0), je, je + 1, je + 100})
        {
            std::string result;
            try
            {
                result = "ran, value=" +
                    std::to_string(sv::dispatch_work<repro_sleep_action>(
                        b.locality, e, 10)
                                       .get());
            }
            catch (hpx::exception const& ex)
            {
                result = hpx::get_error_name(hpx::get_error(ex));
            }
            say("join_epoch=%llu epoch=%llu check_admission=%s dispatch_work=%s",
                static_cast<unsigned long long>(je),
                static_cast<unsigned long long>(e),
                outcome(sv::check_admission(b.locality, e)), result.c_str());
        }
    }
}    // namespace

int main(int argc, char** argv)
{
    role = arg(argc, argv, "--r-role", "a");
    case_name = arg(argc, argv, "--r-case", "observer");
    dir = arg(argc, argv, "--r-dir", ".");
    poll_ms = std::stoi(arg(argc, argv, "--r-poll-ms", "500"));
    b_join = arg(argc, argv, "--r-b-join", "1") != "0";

    std::vector<char*> hargs{argv[0]};
    for (int i = 1; i < argc; ++i)
    {
        if (std::strncmp(argv[i], "--r-", 4) == 0)
        {
            ++i;
            continue;
        }
        hargs.push_back(argv[i]);
    }
    int hargc = static_cast<int>(hargs.size());
    hargs.push_back(nullptr);

    hpx::init_params params;
    if (role == "root")
    {
        params.mode = hpx::runtime_mode::console;
        params.cfg = {"hpx.expect_connecting_localities!=1"};
    }
    else
    {
        params.mode = hpx::runtime_mode::connect;
    }
    if (!hpx::start(nullptr, hargc, hargs.data(), params))
        return 2;

    hpx::run_as_hpx_thread([] {
        if (role == "root")
            run_root();
        else if (role == "b")
            run_b();
        else if (case_name == "observer")
            a_observer();
        else if (case_name == "inflight")
            a_inflight();
        else if (case_name == "remove")
            a_remove();
        else if (case_name == "rtt")
            a_rtt();
        else if (case_name == "leave")
            a_leave();
        else
            a_epoch();
    });

    if (case_name == "leave" && role == "b")
    {
        // The sequence from hpx#7825, from the main (non-HPX) thread.
        int const pause_ms = std::stoi(arg(argc, argv, "--r-pause-ms", "0"));
        say("finalize");
        hpx::run_as_hpx_thread([] { hpx::supervision::finalize(); });
        if (pause_ms > 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
        say("disconnect, pause_ms=%d", pause_ms);
        hpx::post([] { hpx::disconnect(); });
        int const rc = hpx::stop();
        say("clean, hpx::stop rc=%d", rc);
        return 0;
    }

    say("done");
    std::_Exit(0);    // shutdown is not what is being tested
}
