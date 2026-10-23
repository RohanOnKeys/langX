//  Copyright (c) 2026 Rohan Pattanayak
//
//  SPDX-License-Identifier: BSL-1.0

/// \file langx/membership.hpp
/// \brief Root membership action: force_disconnect run on locality 0.
///
/// hpx::force_disconnect fails with invalid_status when called from a
/// late-connecting locality, so the coordinator asks the root to call it
/// (plan section 6.1, option b; phase 0 results). The header defines and
/// registers a plain action, so it must be included in exactly one
/// translation unit per binary: the root executable and the _langx module.

#pragma once

#include <hpx/include/actions.hpp>
#include <hpx/init_runtime/finalize.hpp>
#include <hpx/modules/naming_base.hpp>
#include <hpx/runtime_distributed/find_all_localities.hpp>

#include <chrono>
#include <cstdint>
#include <exception>
#include <string>

/// \brief Remove \p locality from AGAS and the connection caches.
///
/// Runs on the root. Never throws: the outcome is returned as a small JSON
/// object with `rc`, `error`, `elapsed_ms` and `membership_after`.
///
/// \param locality  Locality id of a late-connecting locality.
/// \return JSON text describing the outcome.
inline std::string langx_root_force_disconnect(std::uint32_t locality)
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
    {
        if (c == '"' || c == '\\' || c == '\n')
            c = ' ';
    }
    auto const ms = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - t0)
                        .count();
    return "{\"rc\":" + std::to_string(rc) + ",\"error\":\"" + err +
        "\",\"elapsed_ms\":" + std::to_string(ms) + ",\"membership_after\":" +
        std::to_string(hpx::find_all_localities().size()) + "}";
}
HPX_PLAIN_ACTION(langx_root_force_disconnect, langx_root_force_disconnect_action)
