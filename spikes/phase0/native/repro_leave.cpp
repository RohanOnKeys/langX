//  Copyright (c) 2026 Rohan Pattanayak
//
//  Minimal reproducer, no Python: a supervised connect-mode locality leaves
//  (supervision::finalize, hpx::disconnect, hpx::stop) while its joined peers
//  keep running. Run against p0_root with one "stayer" and one "leaver":
//
//    repro_leave <hpx args> --r-role stayer --r-stay-s 20
//    repro_leave <hpx args> --r-role leaver --r-pause-ms 0
//
//  The leaver prints "R: clean" if hpx::stop returns. --r-pause-ms sleeps
//  between supervision::finalize and hpx::disconnect.

#include <hpx/hpx.hpp>
#include <hpx/hpx_start.hpp>
#include <hpx/include/run_as.hpp>
#include <hpx/modules/supervision.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>

#include <hpx/supervision_dispatch.hpp>
#include <hpx/supervision_dispatch/testing.hpp>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

namespace {

    std::string arg(int argc, char** argv, char const* key, char const* dflt)
    {
        for (int i = 1; i + 1 < argc; ++i)
            if (std::strcmp(argv[i], key) == 0)
                return argv[i + 1];
        return dflt;
    }

    void say(std::string const& s)
    {
        std::printf("R: %s\n", s.c_str());
        std::fflush(stdout);
    }
}    // namespace

int main(int argc, char** argv)
{
    std::string const role = arg(argc, argv, "--r-role", "leaver");
    int const poll_ms = std::stoi(arg(argc, argv, "--r-poll-ms", "500"));
    int const pause_ms = std::stoi(arg(argc, argv, "--r-pause-ms", "0"));
    int const leave_after_s = std::stoi(arg(argc, argv, "--r-leave-after-s", "3"));
    int const stay_s = std::stoi(arg(argc, argv, "--r-stay-s", "20"));

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
    params.mode = hpx::runtime_mode::connect;
    if (!hpx::start(nullptr, hargc, hargs.data(), params))
    {
        say("start failed");
        return 2;
    }

    std::size_t joined = 0;
    hpx::run_as_hpx_thread([&] {
        hpx::supervision::testing::set_failure_detection_poll_timeout_for_testing(
            std::chrono::milliseconds(poll_ms));
        auto const h = hpx::supervision::init(
            hpx::launch::sync, std::chrono::milliseconds(2000));
        auto const ep = hpx::supervision::query_state(hpx::launch::sync, h).epoch;
        hpx::supervision::publish_event(
            hpx::launch::sync, h, hpx::supervision::event::running, ep);
        // Join the root and the other connector.
        auto const deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(30);
        while (std::chrono::steady_clock::now() < deadline)
        {
            joined = hpx::supervision::discover_and_join(
                h, std::chrono::milliseconds(1000))
                         .size();
            if (joined >= 2)
                break;
        }
    });
    say(role + " joined " + std::to_string(joined));

    if (role == "stayer")
    {
        std::this_thread::sleep_for(std::chrono::seconds(stay_s));
        say("stayer done");
        std::_Exit(0);
    }

    std::this_thread::sleep_for(std::chrono::seconds(leave_after_s));
    say("finalize");
    hpx::run_as_hpx_thread([] { hpx::supervision::finalize(); });
    if (pause_ms > 0)
        std::this_thread::sleep_for(std::chrono::milliseconds(pause_ms));
    say("disconnect");
    hpx::post([] { hpx::disconnect(); });
    int const rc = hpx::stop();
    say("clean rc=" + std::to_string(rc));
    return 0;
}
