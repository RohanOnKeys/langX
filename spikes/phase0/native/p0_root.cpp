//  Copyright (c) 2026 Rohan Pattanayak
//
//  Phase 0 spike root: work-free HPX locality 0 (plan section 6.1). Starts in
//  console mode expecting connecting localities, registers under supervision
//  so connectors can discover it quickly, then idles until <bootdir>/root.stop
//  appears. Shutdown runs under a watchdog because the exp70 Slice 3B root
//  hang is expected to reproduce; the exit path is recorded either way.
//
//  Startup sequence follows RayX exp70 Slice 3B native/root_supervised.cpp
//  (Bita). Spike code only.

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/modules/supervision.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>
#include <hpx/runtime_local/get_locality_id.hpp>
#include <hpx/version.hpp>

#include <hpx/supervision_dispatch.hpp>
#include <hpx/supervision_dispatch/testing.hpp>

#include "p0_membership.hpp"

#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace {

    std::string bootdir = ".";

    std::string arg_value(int argc, char** argv, std::string const& key,
        std::string const& fallback)
    {
        for (int i = 1; i + 1 < argc; ++i)
            if (key == argv[i])
                return argv[i + 1];
        return fallback;
    }

    void write_text(std::string const& name, std::string const& content)
    {
        std::ofstream f(bootdir + "/" + name, std::ios::trunc);
        f << content;
    }

    bool exists(std::string const& name)
    {
        struct stat st{};
        return ::stat((bootdir + "/" + name).c_str(), &st) == 0;
    }

    long long steady_ns()
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    // Strips the root's own flags so HPX does not reject them.
    std::vector<char*> hpx_args(int argc, char** argv)
    {
        std::vector<char*> out{argv[0]};
        for (int i = 1; i < argc; ++i)
        {
            if (std::strncmp(argv[i], "--p0-", 5) == 0)
            {
                ++i;
                continue;
            }
            out.push_back(argv[i]);
        }
        out.push_back(nullptr);
        return out;
    }
}    // namespace

int main(int argc, char** argv)
{
    bootdir = arg_value(argc, argv, "--p0-bootdir", ".");
    int const fd_poll_ms =
        std::stoi(arg_value(argc, argv, "--p0-fd-poll-ms", "500"));
    int const hard_timeout_s =
        std::stoi(arg_value(argc, argv, "--p0-hard-timeout-s", "600"));
    int const shutdown_timeout_s =
        std::stoi(arg_value(argc, argv, "--p0-shutdown-timeout-s", "10"));

    std::thread([hard_timeout_s] {
        std::this_thread::sleep_for(std::chrono::seconds(hard_timeout_s));
        write_text("root.exit", "{\"exit_path\":\"hard_timeout\"}\n");
        std::_Exit(86);
    }).detach();

    std::vector<char*> args = hpx_args(argc, argv);
    int hargc = static_cast<int>(args.size()) - 1;

    hpx::init_params params;
    params.cfg = {"hpx.expect_connecting_localities!=1"};
    params.mode = hpx::runtime_mode::console;
    if (!hpx::start(nullptr, hargc, args.data(), params))
    {
        write_text("root.started", "{\"started\":false}\n");
        return 2;
    }

    std::uint64_t epoch = 0;
    std::string err;
    hpx::run_as_hpx_thread([&] {
        try
        {
            hpx::supervision::testing::
                set_failure_detection_poll_timeout_for_testing(
                    std::chrono::milliseconds(fd_poll_ms));
            auto const handle = hpx::supervision::init(
                hpx::launch::sync, std::chrono::milliseconds(2000));
            epoch = hpx::supervision::query_state(hpx::launch::sync, handle)
                        .epoch;
            hpx::supervision::publish_event(hpx::launch::sync, handle,
                hpx::supervision::event::running, epoch);
        }
        catch (std::exception const& e)
        {
            err = e.what();
        }
    });

    write_text("root.started",
        "{\"started\":true,\"pid\":" + std::to_string(::getpid()) +
            ",\"supervision_epoch\":" + std::to_string(epoch) +
            ",\"supervision_error\":\"" + err + "\",\"hpx_version\":\"" +
            hpx::full_version_as_string() + "\",\"hpx_git_commit\":\"" +
            HPX_HAVE_GIT_COMMIT + "\"}\n");

    while (!exists("root.stop"))
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    long long const t0 = steady_ns();
    std::thread([shutdown_timeout_s, t0] {
        std::this_thread::sleep_for(std::chrono::seconds(shutdown_timeout_s));
        write_text("root.exit",
            "{\"exit_path\":\"shutdown_timeout\",\"waited_ms\":" +
                std::to_string((steady_ns() - t0) / 1000000) +
                ",\"start_ns\":" + std::to_string(t0) +
                ",\"t_ns\":" + std::to_string(steady_ns()) + "}\n");
        std::_Exit(87);
    }).detach();

    hpx::run_as_hpx_thread([] { hpx::supervision::finalize(); });
    write_text("root.exit",
        "{\"exit_path\":\"after_supervision_finalize\",\"start_ns\":" +
            std::to_string(t0) + ",\"t_ns\":" + std::to_string(steady_ns()) +
            "}\n");
    hpx::post([] { hpx::finalize(); });
    int const rc = hpx::stop();
    write_text("root.exit",
        "{\"exit_path\":\"clean\",\"stop_rc\":" + std::to_string(rc) +
            ",\"shutdown_ms\":" + std::to_string((steady_ns() - t0) / 1000000) +
            ",\"start_ns\":" + std::to_string(t0) +
            ",\"t_ns\":" + std::to_string(steady_ns()) + "}\n");
    return 0;
}
