//  Copyright (c) 2026 Rohan Pattanayak
//
//  Root membership action (plan section 6.1, option b): a connector cannot
//  call hpx::force_disconnect itself (invalid_status), so it asks the root,
//  locality 0, to do it. Include in exactly one translation unit per binary.

#pragma once

#include <hpx/include/actions.hpp>
#include <hpx/init_runtime/finalize.hpp>
#include <hpx/modules/naming_base.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>

#include <chrono>
#include <cstdint>
#include <exception>
#include <string>

inline std::string p0_root_force_disconnect(std::uint32_t locality)
{
    auto const t0 = std::chrono::steady_clock::now();
    int rc = -1;
    std::string err;
#if defined(HPX_HAVE_FORCE_DISCONNECT)
    try
    {
        hpx::error_code ec(hpx::throwmode::lightweight);
        rc = hpx::force_disconnect(
            hpx::naming::get_id_from_locality_id(locality), ec);
        if (ec)
            err = ec.get_message();
    }
    catch (std::exception const& e)
    {
        err = e.what();
    }
#else
    err = "HPX built without force_disconnect";
#endif
    for (char& c : err)
        if (c == '"' || c == '\\' || c == '\n')
            c = ' ';
    auto const ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0)
                        .count();
    return "{\"rc\":" + std::to_string(rc) + ",\"error\":\"" + err +
        "\",\"elapsed_ms\":" + std::to_string(ms) +
        ",\"root_membership_after\":" +
        std::to_string(hpx::find_all_localities().size()) + "}";
}
HPX_PLAIN_ACTION(p0_root_force_disconnect, p0_root_force_disconnect_action)
