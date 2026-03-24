# =============================================================================
# Script: State-level Generator Extraction from EI System (Sienna)
#
# Description:
# This script extracts generator-level data from the Eastern Interconnection
# (EI) system built in Sienna (PowerSystems.jl format), and exports
# state-specific datasets as CSV files.
#
# The workflow:
#   1. Load the EI system from a JSON file.
#   2. Retrieve geographic coordinates (lat/lon) for each bus.
#   3. Map generators to their corresponding bus coordinates.
#   4. Use U.S. state shapefiles to determine whether each generator lies
#      within a given state boundary.
#   5. Export generator attributes for each state into a structured DataFrame.
#

## RUN THIS IN KESTREL
using Pkg
Pkg.activate("/projects/ntps/sabrilg/EasternInterconnection.jl/")
using EasternInterconnection
using PowerSystems
using Logging
using DataFrames
using CSV
using Shapefile
using GeoInterface
using ArchGDAL

const EI = EasternInterconnection
const PSY = PowerSystems
_load_year=2023
_weather_year=2012

sys = System("/projects/siennadev/EI/test/test_sys/system.json")
set_units_base_system!(sys, "NATURAL_UNITS")

bus_coords = Dict{String, Tuple{Float64, Float64}}()
for bus in get_components(ACBus, sys)
    geo_attrs = get_supplemental_attributes(GeographicInfo, bus)
    if !isempty(geo_attrs)
        coords = geo_attrs[1].geo_json["coordinates"]
        bus_coords[get_name(bus)] = (coords[2], coords[1])  # (lat, lon)
    end
end

all_generators = collect(get_components(Generator, sys))
gen_data = DataFrame(
    name     = [get_name(g) for g in all_generators],
    bus_name = [get_name(g.bus) for g in all_generators],
    lat      = [get(bus_coords, get_name(g.bus), (missing, missing))[1] for g in all_generators],
    lon      = [get(bus_coords, get_name(g.bus), (missing, missing))[2] for g in all_generators],
)

# ── Load U.S. state boundaries (shapefile) ────────────────────────────────────
# Source: U.S. Census Bureau TIGER/Line shapefiles
# These geometries are used for accurate spatial filtering via point-in-polygon.

const STATE_SHAPES = Shapefile.Table("/projects/ntps/sabrilg/shapefiles/cb_2022_us_state_500k.shp")

function get_state_shape(state_name::String)
    for row in STATE_SHAPES
        if row.NAME == state_name
            return GeoInterface.geometry(row)
        end
    end
    error("State '$state_name' not found in shapefile")
end

# function point_in_polygon(lat::Float64, lon::Float64, shape)::Bool
#     point = ArchGDAL.createpoint(lon, lat)
#     poly  = ArchGDAL.fromWKT(GeoInterface.convert(ArchGDAL, shape) |> ArchGDAL.toWKT)
#     return ArchGDAL.contains(poly, point)
# end

function build_polygon(shape)
    return ArchGDAL.fromWKT(GeoInterface.convert(ArchGDAL, shape) |> ArchGDAL.toWKT)
end

function point_in_polygon(lat, lon, poly)
    point = ArchGDAL.createpoint(lon, lat)
    return ArchGDAL.contains(poly, point)
end


# ── Helper functions for generator classification and metadata ────────────────
# These functions standardize generator attributes across different
# PowerSystems.jl generator types.

const THERMAL_PRIME_MOVERS = Set([
    PrimeMovers.CA, PrimeMovers.CT, PrimeMovers.CC,
    PrimeMovers.GT, PrimeMovers.ST, PrimeMovers.IC
])

function is_misclassified_thermal(gen)
    return (gen isa RenewableDispatch || gen isa RenewableNonDispatch) &&
           gen.prime_mover_type in THERMAL_PRIME_MOVERS
end

function get_fuel(gen)
    if gen isa ThermalStandard || gen isa ThermalMultiStart
        return string(gen.fuel)
    elseif is_misclassified_thermal(gen)
        return "NATURAL_GAS"
    elseif gen isa RenewableDispatch || gen isa RenewableNonDispatch
        return "Renewable"
    elseif gen isa HydroGen
        return "Water"
    else
        return "UNKNOWN"
    end
end

function get_gen_type(gen)
    if gen isa ThermalStandard
        return "ThermalStandard"
    elseif gen isa ThermalMultiStart
        return "ThermalMultiStart"
    elseif is_misclassified_thermal(gen)
        return "ThermalStandard"
    elseif gen isa RenewableDispatch || gen isa RenewableNonDispatch
        pm = gen.prime_mover_type
        if pm == PrimeMovers.PVe
            return "Solar"
        elseif pm == PrimeMovers.WT || pm == PrimeMovers.WS
            return "Wind"
        else
            return "Renewable"
        end
    elseif gen isa HydroGen
        return "Hydro"
    else
        return "Unknown"
    end
end

function get_ts_name(gen)
    if !(gen isa ThermalStandard) && has_time_series(gen)
        ts_keys = get_time_series_keys(gen)
        if !isempty(ts_keys)
            return get_name(gen)
        end
    end
    return "NO_TS"
end

function get_min_power(gen)
    if gen isa ThermalStandard || gen isa ThermalMultiStart
        return get_active_power_limits(gen).min
    elseif gen isa HydroDispatch || gen isa HydroTurbine
        return get_active_power_limits(gen).min
    else
        return 0.0
    end
end

function get_prime_mover_str(gen)
    pm = string(get_prime_mover_type(gen))
    parts = split(pm, ".")
    if length(parts) >= 2
        s = parts[2]
        return split(s, " ")[1]
    else
        return split(pm, " ")[1]
    end
end

function export_state_generators(
    state_name::String;
    output_dir::String = "/projects/ntps/sabrilg/EasternInterconnection.jl/"
)
    shape = get_state_shape(state_name)
    poly = build_polygon(shape)

    state_gen_data = filter(row ->
        !ismissing(row.lat) && !ismissing(row.lon) &&
        point_in_polygon(row.lat, row.lon, poly),
        gen_data
    )

    generators = [get_component(Generator, sys, name) for name in state_gen_data.name]
    filter!(!isnothing, generators)
    println("$(state_name) generators retrieved: ", length(generators))

    # ── Helper to safely get thermal-only fields ──────────────────────────────
    function safe_ramp_limits(g)
        if g isa ThermalStandard || g isa ThermalMultiStart
            rl = PSY.get_ramp_limits(g)
            return isnothing(rl) ? missing : "up=$(rl.up), down=$(rl.down)"
        end
        return missing
    end

    function safe_time_limits(g)
        if g isa ThermalStandard || g isa ThermalMultiStart
            tl = PSY.get_time_limits(g)
            return isnothing(tl) ? missing : "up=$(tl.up), down=$(tl.down)"
        end
        return missing
    end

    function safe_must_run(g)
        if g isa ThermalStandard || g isa ThermalMultiStart
            return PSY.get_must_run(g)
        end
        return missing
    end

    function safe_time_at_status(g)
        if g isa ThermalStandard || g isa ThermalMultiStart
            return PSY.get_time_at_status(g)
        end
        return missing
    end

    function safe_power_factor(g)
        if g isa RenewableDispatch || g isa RenewableNonDispatch
            return PSY.get_power_factor(g)
        end
        return missing
    end

    function safe_reactive_power_limits(g)
        rl = PSY.get_reactive_power_limits(g)
        return isnothing(rl) ? missing : "min=$(rl.min), max=$(rl.max)"
    end
# ── Build unified generator DataFrame ──────────────────────────────────────
# Combines thermal, renewable, and hydro generators into a single schema.
# Fields not applicable to certain generator types are set to `missing`.

resources = DataFrame(
        # ── Common fields ─────────────────────────────────────────────────────
        "generator_type"             => [get_gen_type(g) for g in generators],
        "name"                       => [get_name(g) for g in generators],
        "available"                  => [get_available(g) for g in generators],
        "bus"                        => [get_name(g.bus) for g in generators],
        "bus_number"                 => [get_number(g.bus) for g in generators],
        "active_power"               => [get_active_power(g) for g in generators],
        "reactive_power"             => [get_reactive_power(g) for g in generators],
        "rating"                     => [get_rating(g) for g in generators],
        "active_power_limits_min"    => [get_min_power(g) for g in generators],
        "active_power_limits_max"    => [get_max_active_power(g) for g in generators],
        "reactive_power_limits"      => [safe_reactive_power_limits(g) for g in generators],  # fixed
        "base_power"                 => [get_base_power(g) for g in generators],
        "prime_mover_type"           => [get_prime_mover_str(g) for g in generators],
        "fuel"                       => [get_fuel(g) for g in generators],
        "operation_cost"             => [string(get_operation_cost(g)) for g in generators],
        "ts_column_name"             => [get_ts_name(g) for g in generators],
        # ── Thermal-only fields (missing for renewables) ───────────────────────
        "status"                     => [g isa ThermalStandard || g isa ThermalMultiStart ? PSY.get_status(g) : missing for g in generators],
        "ramp_limits"                => [safe_ramp_limits(g) for g in generators],
        "time_limits"                => [safe_time_limits(g) for g in generators],
        "must_run"                   => [safe_must_run(g) for g in generators],
        "time_at_status"             => [safe_time_at_status(g) for g in generators],
        # ── Renewable-only fields (missing for thermal) ────────────────────────
        "power_factor"               => [safe_power_factor(g) for g in generators],
        # ── Extra geographic/metadata fields ──────────────────────────────────
        "lat"                        => [get(bus_coords, get_name(g.bus), (missing, missing))[1] for g in generators],
        "lon"                        => [get(bus_coords, get_name(g.bus), (missing, missing))[2] for g in generators],
        "plant_name"                 => [get(g.ext, "plant_name", missing) for g in generators],
        "ext_id"                     => [get(g.ext, "id", missing) for g in generators],
    )

filename = lowercase(replace(state_name, " " => "_")) * "_resources.csv"
filepath = joinpath(output_dir, filename)
CSV.write(filepath, resources; transform=(col, val) -> something(val, missing))
println("Saved $(filepath) with ", nrow(resources), " generators")

return resources
end
# ── Previous method: Bounding box (latitude/longitude rectangle) ─────────────
#
# This approach selected generators using manually defined coordinate ranges:
#     lat_min ≤ lat ≤ lat_max
#     lon_min ≤ lon ≤ lon_max
#
# Limitations:
#   - Includes generators outside actual state borders
#   - Poor accuracy for irregularly shaped states
#   - Requires manual tuning of coordinate ranges
#
# This method is retained here for reference but is no longer used.
#
# va_resources = export_state_generators("Virginia",      36.5, 39.5, -83.7, -75.2) 
# md_resources = export_state_generators("Maryland",      37.9, 39.7, -79.5, -75.0)
# wv_resources = export_state_generators("West_Virginia", 37.2, 40.6, -82.6, -77.7)
# il_resources = export_state_generators("Illinois",      36.9, 42.5, -91.5, -87.0)
# mn_resources = export_state_generators("Minnesota",     43.5, 49.4, -97.2, -89.5)

# ── Current method: Shapefile-based state filtering ──────────────────────────
#
# Uses polygon geometries from shapefiles and a point-in-polygon test
# (ArchGDAL) to determine whether each generator belongs to a state.

va_resources = export_state_generators("Virginia")
md_resources = export_state_generators("Maryland")
wv_resources = export_state_generators("West Virginia")   # note: space, not underscore
il_resources = export_state_generators("Illinois")
mn_resources = export_state_generators("Minnesota")