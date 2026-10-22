//  Copyright (c) 2026 Rohan Pattanayak
//
//  SPDX-License-Identifier: BSL-1.0

/// \file langx_root.cpp
/// \brief Work-free supervised root, HPX locality 0 (plan section 6.1).
///
/// Starts in console mode expecting late-connecting localities, registers
/// under supervision so connectors discover it, and serves the membership
/// action from langx/membership.hpp. Runs no application work. Exits on
/// SIGTERM, SIGINT, or when the stop file appears; shutdown runs under a
/// watchdog and the exit path is written to the ready file's directory.
///
/// The startup sequence follows RayX exp70 Slice 3B native/root_supervised.cpp
/// (Bita).
///
/// Options (all others go to HPX):
///   --langx-ready-file PATH        JSON written once the root is up
///   --langx-stop-file PATH         optional; exit when it appears
///   --langx-poll-timeout-ms N      failure-detection poll timeout (testing
///                                  hook); 0 keeps the HPX default
///   --langx-shutdown-timeout-s N   bound on shutdown, default 10

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/modules/supervision.hpp>
#include <hpx/version.hpp>

#include <hpx/supervision_dispatch.hpp>
#include <hpx/supervision_dispatch/testing.hpp>

#include <langx/membership.hpp>

#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace {

    std::atomic<bool> stop_requested{false};

    extern "C" void on_signal(int)
    {
        stop_requested.store(true);
    }

    std::string option(int argc, char** argv, char const* key,
        std::string const& fallback)
    {
        for (int i = 1; i + 1 < argc; ++i)
        {
            if (std::strcmp(argv[i], key) == 0)
                return argv[i + 1];
        }
        return fallback;
    }

    bool exists(std::string const& path)
    {
        struct stat st{};
        return !path.empty() && ::stat(path.c_str(), &st) == 0;
    }

    void write_file(std::string const& path, std::string const& text)
    {
        if (path.empty())
            return;
        std::string const tmp = path + ".tmp";
        {
            std::ofstream f(tmp, std::ios::trunc);
            f << text;
        }
        std::rename(tmp.c_str(), path.c_str());
    }

    long long steady_ns()
    {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count();
    }

    /// Drops --langx-* options and their values so HPX does not reject them.
    std::vector<char*> hpx_args(int argc, char** argv)
    {
        std::vector<char*> out{argv[0]};
        for (int i = 1; i < argc; ++i)
        {
            if (std::strncmp(argv[i], "--langx-", 8) == 0)
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
    std::string const ready_file = option(argc, argv, "--langx-ready-file", "");
    std::string const stop_file = option(argc, argv, "--langx-stop-file", "");
    std::string const exit_file = ready_file.empty() ? "" : ready_file + ".exit";
    int const poll_ms =
        std::stoi(option(argc, argv, "--langx-poll-timeout-ms", "0"));
    int const shutdown_s =
        std::stoi(option(argc, argv, "--langx-shutdown-timeout-s", "10"));

    std::signal(SIGTERM, on_signal);
    std::signal(SIGINT, on_signal);

    std::vector<char*> args = hpx_args(argc, argv);
    int hargc = static_cast<int>(args.size()) - 1;

    hpx::init_params params;
    params.cfg = {"hpx.expect_connecting_localities!=1"};
    params.mode = hpx::runtime_mode::console;
    if (!hpx::start(nullptr, hargc, args.data(), params))
        return 2;

    std::uint64_t epoch = 0;
    std::string err;
    hpx::run_as_hpx_thread([&] {
        try
        {
            if (poll_ms > 0)
            {
                hpx::supervision::testing::
                    set_failure_detection_poll_timeout_for_testing(
                        std::chrono::milliseconds(poll_ms));
            }
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
    for (char& c : err)
    {
        if (c == '"' || c == '\\' || c == '\n')
            c = ' ';
    }

    write_file(ready_file,
        "{\"pid\":" + std::to_string(::getpid()) + ",\"supervision_epoch\":" +
            std::to_string(epoch) + ",\"supervision_error\":\"" + err +
            "\",\"hpx_git_commit\":\"" + HPX_HAVE_GIT_COMMIT + "\"}\n");
    if (!err.empty())
        return 3;

    while (!stop_requested.load() && !exists(stop_file))
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

    long long const t0 = steady_ns();
    std::thread([exit_file, shutdown_s, t0] {
        std::this_thread::sleep_for(std::chrono::seconds(shutdown_s));
        write_file(exit_file,
            "{\"exit_path\":\"shutdown_timeout\",\"waited_ms\":" +
                std::to_string((steady_ns() - t0) / 1000000) + "}\n");
        std::_Exit(87);
    }).detach();

    hpx::run_as_hpx_thread([] { hpx::supervision::finalize(); });
    // finalize() can return with failure-detection checks still pending; let
    // them resolve before runtime teardown (phase 0 results, HPX cc2be553).
    if (poll_ms > 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(2 * poll_ms));
    hpx::post([] { hpx::finalize(); });
    int const rc = hpx::stop();
    write_file(exit_file,
        "{\"exit_path\":\"clean\",\"stop_rc\":" + std::to_string(rc) +
            ",\"shutdown_ms\":" + std::to_string((steady_ns() - t0) / 1000000) +
            "}\n");
    return 0;
}
