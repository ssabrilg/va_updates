```
EI generators ──→ match by bus_number → EIA2PF (PV)
                      ↓ unmatched Solar EI
                  match by bus_number → MMWG
                      ↓ still unmatched

EIA2PF (PV) ──→ find what's NOT in EI (by bus_number)
                      ↓ has BusID+BusName → EIA_ONLY
                      ↓ has neither → exact name → MMWG
                                    → fuzzy name → MMWG
```

using Pkg
Pkg.activate("EasternInterconnection.jl/")

using PowerSystems
using Logging
using DataFrames
using CSV
using Shapefile
using GeoInterface
using ArchGDAL
using TimeSeries
using Dates
using PowerSystems
using Random
Random.seed!(42)  # Replace 42 with any fixed integer seed


const PSY = PowerSystems

_load_year = 2023
_weather_year = 2012 # TODO confirm

DATA_DIR = "/Users/sabrilg/Documents/GitHub/va_updates"

sys = System("system.json")

set_units_base_system!(sys, "NATURAL_UNITS")


# ── Plot source colors (used by both solar and wind report sections) ───────────
const SOURCE_COLORS = Dict(
    "EI_EIA2PF"      => "#2ecc71",   # green       — highest confidence
    "EI_MMWG"        => "#27ae60",   # dark green  — high confidence
    "EIA_ONLY"       => "#3498db",   # blue        — new, bus known
    "EIA_MMWG_EXACT" => "#e67e22",   # orange      — new, bus inferred exact
    "EIA_MMWG_FUZZY" => "#e74c3c",   # red         — new, bus inferred fuzzy
)

const MANUAL_PM_CHANGES = Dict{String, Union{String, Missing}}(
# EIA data confirms this generator is Solar Photovoltaic (PV), not Wind (WT).
# The bus name "3AA2-088SOLA_315608" and plant name "Southampton Solar, LLC" corroborate this.
# Updating prime mover from WT to PV to correct the misclassification in the original EI data.    
"generator-315608-7245393367" => "PVe" 

)
using XLSX, DataFrames, StringDistances, CSV, PlotlyJS

# ── 1. Load data ──────────────────────────────────────────────────────────────
eia_2_pf_mapping = DataFrame(XLSX.readtable("/Users/sabrilg/Documents/GitHub/va_updates/EIA2PF.xlsx", "EIA2PF"))
eia_2_pf_mapping = unique(eia_2_pf_mapping)
mmwg_data_full   = DataFrame(XLSX.readtable("/Users/sabrilg/Documents/GitHub/va_updates/mmwg-2023-series-data-dictionary.xlsx", "ERAG"))
mmwg_data_full = unique(mmwg_data_full)
eia_plants       = DataFrame(XLSX.readtable("/Users/sabrilg/Documents/GitHub/va_updates/2___Plant_Y2024.xlsx", "Plant", first_row = 2))

VA_ei_gens = CSV.read(joinpath(homedir(), "Downloads", "virginia_resources.csv"), DataFrame)
MD_ei_gens = CSV.read(joinpath(homedir(), "Downloads", "maryland_resources.csv"), DataFrame)
WV_ei_gens = CSV.read(joinpath(homedir(), "Downloads", "west_virginia_resources.csv"), DataFrame)

AR_ei_gens = CSV.read(joinpath(homedir(), "Downloads", "arkansas_resources.csv"), DataFrame)

# ── Apply manual prime mover corrections directly on EI source data ───────────
for (label, df) in [("VA", VA_ei_gens), ("MD", MD_ei_gens), ("WV", WV_ei_gens)]
    df[!, "prime_mover_type"] = String.(df[!, "prime_mover_type"])
    df[!, "generator_type"]   = String.(df[!, "generator_type"])

    for (gen_name, new_pm) in MANUAL_PM_CHANGES
        ismissing(new_pm) && continue
        idx = findall(r -> coalesce(df[r, "name"], "") == gen_name, 1:nrow(df))
        if !isempty(idx)
            old_pm = df[first(idx), "prime_mover_type"]
            old_gt = df[first(idx), "generator_type"]

            # ── Derive correct generator_type from new prime mover ────────────
            new_gt = if new_pm in ("PVe", "PVf")
                "Solar"
            elseif new_pm in ("WT", "WS")
                "Wind"
            else
                old_gt  # unknown mapping — leave unchanged
            end

            df[idx, "prime_mover_type"] .= new_pm
            df[idx, "generator_type"]   .= new_gt
            println("✅ $label — corrected '$gen_name':",
                    " prime_mover_type: $old_pm → $new_pm",
                    " | generator_type: $old_gt → $new_gt")
        end
    end
end

# ── 2. Prepare EIA slim (done once, shared across states) ────────────────────
eia_locations = DataFrames.select(eia_plants, [
    "Plant Code", "Plant Name", "Street Address", "City",
    "State", "Zip", "County", "Latitude", "Longitude",
])
# Normalize Plant Code to Int for joining
eia_locations[!, "Plant Code"] = [ismissing(x) ? missing : Int(x) for x in eia_locations[!, "Plant Code"]]

cap_col = filter(c -> occursin("apacit", c), names(eia_2_pf_mapping))[1]
eia_slim = DataFrames.select(eia_2_pf_mapping, [
    "uid",                                          # ← unique row key
    "Utility ID", "Utility Name",                   # ← utility identifiers
    "Plant ID", "Plant Name", "State", "County",
    "Generator ID", "Technology", "Prime Mover Code",
    "BusID", "BusName", "kV", cap_col,
])
DataFrames.rename!(eia_slim, cap_col => "eia_capacity_mw")

# Normalize Plant ID to Int for joining
eia_slim[!, "Plant ID"] = [ismissing(x) ? missing : Int(x) for x in eia_slim[!, "Plant ID"]]
eia_slim[!, "Utility ID"] = [ismissing(x) ? missing : Int(x) for x in eia_slim[!, "Utility ID"]]
# Join lat/lon from eia_locations into eia_slim on Plant ID = Plant Code
eia_slim = leftjoin(
    eia_slim,
    DataFrames.select(eia_locations, ["Plant Code", "Latitude", "Longitude"]),
    on = "Plant ID" => "Plant Code",
    matchmissing = :notequal,
)

DataFrames.rename!(eia_slim, "Latitude" => "eia_lat", "Longitude" => "eia_lon")

# ── Check match quality ───────────────────────────────────────────────────────
n_with_loc    = count(!ismissing, eia_slim[!, "eia_lat"])
n_without_loc = count(ismissing,  eia_slim[!, "eia_lat"])
println("\nEIA slim rows:          ", nrow(eia_slim))
println("  ✅ With lat/lon:       ", n_with_loc)
println("  ⚠️  Missing lat/lon:   ", n_without_loc)


# ── 3. Prepare MMWG slim (done once, shared across states) ───────────────────
DataFrames.rename!(mmwg_data_full, [c => strip(c) for c in names(mmwg_data_full)])
mmwg_slim = DataFrames.select(mmwg_data_full, [
    "Bus Number", "Load Flow  Bus Name", "English Name", "EIA Plant Code", "Bus kV",
    "Region/PC",   # ← add this for filtering
    "AreaName",    # ← optional: useful for debugging
])
mmwg_slim[!, "Bus Number"] = [ismissing(x) ? missing : Int(x) for x in mmwg_slim[!, "Bus Number"]]

# ── State → Region/PC mapping ─────────────────────────────────────────────────
const STATE_REGION = Dict(
    "VA" => "PJM",
    "MD" => "PJM",
    "WV" => "PJM",
)
# ── 4. Parse BusID — unique() collapses repeated circuits to same bus ─────────
function parse_all_busids(x)
    ismissing(x) && return Int[]
    isnothing(x) && return Int[]
    s = strip(string(x))
    isempty(s) && return Int[]
    matches = [tryparse(Int, m.match) for m in eachmatch(r"\d+", s)]
    filtered = filter(!isnothing, matches)
    return unique(filtered)
end

# ── 5. Expand EIA — take FIRST BusID only ────────────────────────────────────
# NOTE: The EIA2PF mapping sometimes lists multiple buses for a single generator
# (e.g., BusID = "[232404, 232406, 232408, 232410]"). These are NOT multiple
# physical connections — they represent mapper uncertainty (multiple candidate
# buses at the same substation). Taking all buses would artificially inflate
# the number of EIA rows and cause one EIA plant to match multiple EI generators.
#
# Decision: take only the FIRST bus in the list as the primary interconnection
# bus. This keeps a 1:1 relationship between EIA generators and bus assignments
# and preserves nrow(eia_expanded) == nrow(eia_slim) == 24962 unique generators.
#
# Evidence: buses 232404, 232406, 232408, 232410 (Eastern Shore Solar, Plant 60127)
# are all Oak Hall 138 kV variants at the same physical location (-75.57, 37.95),
# confirming they are alternative mappings, not separate connections.
eia_expanded = DataFrame()
for row in eachrow(eia_slim)
    bus_ids = parse_all_busids(row["BusID"])
    new_row = copy(DataFrame(row))
    if isempty(bus_ids)
        new_row[!, "BusID_int"] = [missing]
    else
        new_row[!, "BusID_int"] = [first(bus_ids)]   # ← take first only
    end
    append!(eia_expanded, new_row, promote=true)
end
println("EIA slim rows: ", nrow(eia_slim), " → expanded: ", nrow(eia_expanded))
@assert nrow(eia_expanded) == nrow(eia_slim) "BUG: expansion changed row count — multi-bus entries not collapsed correctly"

include("/Users/sabrilg/Documents/GitHub/va_updates/EIA860_comparison.jl")

# ── 6. Column sets ────────────────────────────────────────────────────────────
const KEY_COLS = [
    "name", "bus_number", "bus", "generator_type", "prime_mover_type",
    "rating", "eia_capacity_mw", "ts_column_name",
    "lat", "lon", "plant_name",
    "Plant ID", "Plant Name", "Technology", "Prime Mover Code", "BusName", "kV",
]
const UNMATCHED_COLS = [
    "name", "bus_number", "bus", "generator_type", "prime_mover_type",
    "rating", "ts_column_name", "lat", "lon", "plant_name",
]
const MMWG_COLS = [
    "name", "bus_number", "bus", "rating", "ts_column_name",
    "Load Flow  Bus Name", "English Name", "EIA Plant Code", "Bus kV",
]

#### MANUAL_OVERRIDES
const MANUAL_OVERRIDES = Dict{Int, Union{Int, Missing}}(
    59685 => 314078,    # Remington Solar Facility → Remington 115 kV
    59904 => 314477,    # Western Branch High School → Hodges Ferry 115 kV
    65462 => 242544,    # Axton Solar → AXTON
    65665 => 242721,    # Hecate Energy Pulaski → 05MORGAN_242721 
    66312 => 316034,    # Courthouse Solar → best match, consistent with DVP's naming convention for solar interconnections in Virginia.
    67352 => 314268,    # CPV County Line Solar → Briery Del Pt 230 kV by location
    68040 => 316290,    # Sycamore → PJM Gen Queue AC1-161 by location
    68071 => 314426,    # Bellflower Solar (VA) → Fort Pickett 115 kV BUT most likeyly not assigned bus until 2030 operation date.
    68076 => 314676,    # Rocky Run Solar → 3BRUNWICK_314676 Bus
    68194 => 316322,    # Riverstone Solar → 230 kV bus
    68309 => 314846,    # Sunday Solar → closer match in buses location
    66844 => 314538,    # Spring Grove Solar 2 → Surry 230 kV by EIA data
    68313 => 242858,    #  May Solar → WYTHE 1
    68314 => 242675,    # Issa Solar → HUFFMAN
    68311 => 314808,    # Twins Solar -> North River Del Pt 115 kV
    66133 => 235444,    # Bartonsville Energy Facility -> Bartonsville   #1
    68418 => 316165,    # Keydet Solar Center -> PJM Gen Queue AC1-164
    67884 => 314232,    # Two Oaks Solar and Storage LLC -> North Anna 230 kV
    68310 => 235483,    # Iris Solar  -> Meadow Brook
    67881 => 314312,    #  Wyatt Mill Solar -> Jarratt 115 kV
    64134 => 314282,    # Lily Pond Solar, LLC -> Carson 230 kV
    66315 => 314686,    # Clover Creek Solar -> Clover 230 kV
    68075 => 316322,    # Pineside Solar -> PJM Gen Queue AB2-045 230 kV BUT not really something near by. This is 13.7 km away
    68074 => 314173,    # Cerulean Solar -> Garner Del Pt 115 kV
    67871 => 314729,    # Gladys Solar LLC -> Gladys Del Pt 69 kV
    64767 => 314729,    # Pigeon Run Solar Project -> Gladys Del Pt 69 kV
    64768 => 316214,    # Zenith Solar -> PJM Gen Queue AC1-145
    66313 => 314173,    # Moon Corner Solar -> Garner Del Pt 115 kV
    67877 => 314719,    # Spout Spring Solar -> South Creek 115 kV
    68970 => 314539,    # Ho-Fel Solar -> Union Camp 115 kV
    65286 => 243925,    # Caden Energix Piney River LLC -> PINEY RIVER 46 kV
    67215 => 314802,    # Sun Ridge Solar  -> Grottoes 115 kV
    65144 => 242792,    # Deer Wood Energy, LLC  -> SCOTTSVILLE
    65930 => 235502,    # Redbud Run Solar -> Redbud 138 kV
    66695 => 314250,    # Horsepen Branch Solar -> Rockville 230 kV
    60915 => 314375,    # Correctional Solar LLC  -> Correctional 230 Kv
    60916 => 314435,    # Sussex Drive, LLC -> Sapony 230 kV
    61406 => 314172,    # Essex Solar Center -> Dunnsville 230 kV
    63084 => 314476,    # Caden Energix Hickory LLC -> Hickory 115 kV
    64817 => 242545,    # Energix Leatherwood, LLC ->AXTON 138 kV
    65314 => 242858,    # Caden Energix Wytheville LLC -> WYTHE 1 138 kV
    68775 => 313828,    # Winterberry Solar  -> White Marsh Del Pt 115 kV
    65320 => 314302,    # Sebera Solar -> Harvell 115 kV
    67217 => 242797,    # Sunny Rock Solar, LLC  -> SHEFFIELD 138 kV
    67218 => 244166,    # River Trail Solar, LLC -> GALAX 69 kV
    67219 => 314828,    # Prairie Solar (VA)     -> Chuckatuck 230 kV
    68072 => 314257,    # Bridleton Solar -> White Oak 230 kV
    68077 => 314322,    # North Ridge Solar  -> Midlothian 230 kV
    68278 => 324385,    # Greater Wise Solar Project   -> Big Stone Gap 69 kV
    61849 => 243995,    # Danville -> S. SIDE 69 kV
    
    # Manual override due to not exitence of the assigned bus on on EI system
    62011 => 314934,    # Richmond Spider Solar -> Spotsylvania Solar 230 kV 
    62014 => 316037,    # Highlander Solar Energy Station 1 -> Spotsylvania Solar 500 kV
    62768 => 314684,    # Piney Creek Solar -> Mount Laurel 115 kV
    62814 => 316085,    # Sadler Solar -> Ivory Lane 115 kV
    64010 => 314241,    # Energix Hollyfield, LLC -> Old Church 230 kV
    66314 => 314173,    # Bookers Mill Solar -> Garner Del Pt 115 kV
    66635 => 242545,    # Caden Energix Axton LLC -> AXTON 138 kV
    66841 => 235483,    # Foxglove Solar -> Meadow Brook 138 kV
    67171 => 313817,    # Foxhound  -> Clover 230 kV
    65043 => 313863,    # Dulles Solar and Storage -> Yardley Ridge Del Pt 230 kV
    67342 => 314176,    # Carvers Creek Solar -> Harmony Village 230 kV
    61023 => 314241,    # Hollyfield 
    63031 => 314187,    # Gloucester Solar
    65317 => 316223,    # Springfield Solar 
    68073 => 316116,    # Blue Ridge Solar 115 kV
    
    ##### MD ##################
    67022 => 235327,    # CPV Backbone Solar -> Elk Garden 138 kV
    67444 => 232801,    # Cherrywood Solar I -> Oil City 138 kV
    63910 => 233979,    # Bluegrass Solar -> AB2-133 POI
    66843 => 233979,    # Morgnec Solar  -> AB2-133 POI
    68262 => 220982,    # Fairview Farms   -> Otter Point 2360

    # Manual override due to not exitence of the assigned bus on on EI system
    67214 => 237366,    # Jade Meadow LLC -> Carlos Juction 34.5 kV
    66854 => 232270,

    ##### WV##################
    #Manual override due to not exitence of the assigned bus on on EI system
    66276 => 235486,     # Blake Solar Plant -> Millville 138 kV
    64848 => 235101,     #Bedington Energy Facility, LLC - > Bedington 500 kV
    # add more rows here...
)
# ── 7. Match EI → EIA2PF ─────────────────────────────────────────────────────
function match_ei_to_eia(ei_gens::DataFrame, eia_expanded::DataFrame, state::String)
    ei = copy(ei_gens)
    ei.bus_number = [ismissing(x) ? missing : Int(x) for x in ei.bus_number]

    eia_state = filter(row -> coalesce(row["State"] == state, false) &&
                              coalesce(row["Prime Mover Code"] == "PV", false), eia_expanded)
    DataFrames.rename!(eia_state, "BusID_int" => "BusID_int_join")

    println("\n$state: EIA PV rows after expansion: ", nrow(eia_state))

    bus_counts = combine(groupby(eia_state, "BusID_int_join"), nrow => :n_eia_entries)
    multi = filter(row -> coalesce(row[:n_eia_entries] > 1, false), bus_counts)
    if nrow(multi) > 0
        println("  ⚠️  Buses with multiple EIA entries (many-to-one):")
        show(multi, allrows=true)
    end

    result = leftjoin(ei, eia_state, on = "bus_number" => "BusID_int_join", matchmissing = :notequal)

    matched   = filter(row -> !ismissing(row["Plant ID"]), result)
    unmatched = filter(row ->  ismissing(row["Plant ID"]), result)

    println("\n", "="^60)
    println("$state EI Generators — EIA2PF Bus Match Summary")
    println("="^60)
    println("Total $state EI generators:    ", nrow(ei))
    println("  ✅ Matched to EIA2PF bus:    ", nrow(matched))
    println("  ⚠️  No EIA2PF bus match:      ", nrow(unmatched))
    println("─"^60)
    println("Solar generators in $state EI:")
    println("  Total:        ", count(==("Solar"), ei.generator_type))
    println("  ✅ Matched:   ", count(==("Solar"), matched.generator_type))
    println("  ⚠️  Unmatched: ", count(==("Solar"), unmatched.generator_type))
    println("="^60)

    return result
end


# ── 8. Summary + display matched/unmatched solar ──────────────────────────────
function summarize_solar(label::String, result::DataFrame)
    # Matched = any EI gen that found an EIA PV entry (regardless of EI classification)
    matched   = filter(row -> !ismissing(row["Plant ID"]), result)
    # Unmatched = Solar-classified EI gens with no EIA match (may miss misclassified ones)
    unmatched = filter(row -> ismissing(row["Plant ID"]) && row["generator_type"] == "Solar", result)
    # ── Diagnose: non-solar EI gens that matched an EIA PV entry ─────────────
    non_solar_matched = filter(row -> coalesce(row["generator_type"], "") != "Solar", matched)
    if nrow(non_solar_matched) > 0
        println("\n⚠️  $label — Non-solar EI gens matched to EIA PV (misclassification candidates):")
        show(DataFrames.select(non_solar_matched,
             intersect(["eia_lat", "eia_lon", "Utility Name", "Plant ID", "Plant Name",
                        "name", "bus_number", "bus", "generator_type",
                        "prime_mover_type", "rating", "eia_capacity_mw",
                        "Technology", "Prime Mover Code"], names(non_solar_matched))),
             allrows=true)
    end
    println("\n", "="^60)
    println("$label — Solar Matched vs Unmatched")
    println("="^60)
    println("  ✅ Matched (any EI type → EIA PV):  ", nrow(matched))
    println("  ⚠️  Unmatched Solar EI gens:          ", nrow(unmatched))
    println("─"^60)
    println("  Matched by EI generator_type (misclassification check):")
    for gt in sort(unique(skipmissing(matched.generator_type)))
        n = count(==(gt), skipmissing(matched.generator_type))
        println("    $gt: $n")
    end
    println("="^60)

    println("\n📋 $label matched generators (", nrow(matched), " rows):")
    show(sort(DataFrames.select(matched, KEY_COLS), "bus_number"), allrows=true)

    println("\n⚠️  $label unmatched Solar EI generators (", nrow(unmatched), " rows):")
    show(sort(DataFrames.select(unmatched, UNMATCHED_COLS), "bus_number"), allrows=true)

    return matched, unmatched
end

# ── 9. MMWG fallback lookup for unmatched solar ───────────────────────────────
function mmwg_lookup(label::String, unmatched::DataFrame, mmwg_slim::DataFrame)
    result = leftjoin(unmatched, mmwg_slim,
                      on = "bus_number" => "Bus Number", matchmissing = :notequal)

    mmwg_matched   = filter(row -> !ismissing(row["EIA Plant Code"]), result)
    mmwg_unmatched = filter(row ->  ismissing(row["EIA Plant Code"]), result)

    println("\n", "="^60)
    println("$label — Unmatched Solar → MMWG Fallback")
    println("="^60)
    println("  ✅ Found in MMWG:    ", nrow(mmwg_matched))
    println("  ⚠️  Still unmatched: ", nrow(mmwg_unmatched))
    println("="^60)

    println("\n📋 $label found in MMWG (", nrow(mmwg_matched), " rows):")
    show(sort(DataFrames.select(mmwg_matched, MMWG_COLS), "bus_number"), allrows=true)

    println("\n⚠️  $label still unmatched after MMWG (", nrow(mmwg_unmatched), " rows):")
    show(sort(DataFrames.select(mmwg_unmatched, UNMATCHED_COLS), "bus_number"), allrows=true)

    return mmwg_matched, mmwg_unmatched
end

# Final output columns
const OUTPUT_COLS = [
    "state",
    "name",            # EI generator name
    "lat", "lon",      # coordinates
    "bus_number",      # bus ID
    "bus",             # bus name (EI)
    "rating",          # EI capacity (MW)
    "eia_capacity_mw", # EIA nameplate capacity (MW)
    "kV",              # bus voltage
    "Plant ID", "Plant Name",
    "source",          # "EIA2PF" or "MMWG"
    "eia860_status"    # EIa 860 status
]

# ── 10. Run for each state and store results ──────────────────────────────────
va_ei_eia_result   = match_ei_to_eia(VA_ei_gens, eia_expanded, "VA")
md_ei_eia_result   = match_ei_to_eia(MD_ei_gens, eia_expanded, "MD")
wv_ei_eia_result   = match_ei_to_eia(WV_ei_gens, eia_expanded, "WV")
 
# ── EIA2PF matched/unmatched solar ───────────────────────────────────────────
va_solar_matched,   va_solar_unmatched   = summarize_solar("VA", va_ei_eia_result)
md_solar_matched,   md_solar_unmatched   = summarize_solar("MD", md_ei_eia_result)
wv_solar_matched,   wv_solar_unmatched   = summarize_solar("WV", wv_ei_eia_result)

# ── MMWG fallback for unmatched solar ────────────────────────────────────────
va_mmwg_matched,   va_still_unmatched   = mmwg_lookup("VA", va_solar_unmatched, mmwg_slim)
md_mmwg_matched,   md_still_unmatched   = mmwg_lookup("MD", md_solar_unmatched, mmwg_slim)
wv_mmwg_matched,   wv_still_unmatched   = mmwg_lookup("WV", wv_solar_unmatched, mmwg_slim)

println("\n", "="^60)
println("📊 Final Summary by State")
println("="^60)
for (label, matched, mmwg, unmatched) in [
    ("VA", va_solar_matched, va_mmwg_matched, va_still_unmatched),
    ("MD", md_solar_matched, md_mmwg_matched, md_still_unmatched),
    ("WV", wv_solar_matched, wv_mmwg_matched, wv_still_unmatched),
]
    println("\n$label:")
    println("  ✅ EIA2PF matched:      ", nrow(matched))
    println("  ✅ MMWG matched:        ", nrow(mmwg))
    println("  ⚠️  Still unmatched:    ", nrow(unmatched))
end
println("="^60)

# Generate original EI solar data

# ── Export original EI solar generators (already in model) ───────────────────

# # Columns relevant for the report
# const EI_SOLAR_REPORT_COLS = [
#     "name", "bus_number", "bus", "rating",
#     "lat", "lon", "plant_name",
#     "Plant ID", "Plant Name", "Technology",
#     "BusName", "kV", "eia_capacity_mw",
# ]

# function write_ei_solar_report(label::String, matched::DataFrame,
#                                 unmatched::DataFrame, out_dir::String)

#     # ── Matched: EI solar gens that found an EIA PV entry ────────────────────
#     matched_out = DataFrames.select(matched,
#         intersect(EI_SOLAR_REPORT_COLS, names(matched)))
#     matched_out[!, "state"]  .= label
#     matched_out[!, "status"] .= "matched"

#     # ── Unmatched: Solar-classified EI gens with no EIA match ────────────────
#     unmatched_solar = filter(r -> r["generator_type"] == "Solar", unmatched)
#     unmatched_out   = DataFrames.select(unmatched_solar,
#         intersect(EI_SOLAR_REPORT_COLS, names(unmatched_solar)))
#     unmatched_out[!, "state"]  .= label
#     unmatched_out[!, "status"] .= "unmatched"

#     # ── Combined into one CSV per state ──────────────────────────────────────
#     combined = vcat(matched_out, unmatched_out, cols=:union)
#     sort!(combined, :bus_number)

#     path = joinpath(out_dir, "original_solar_$(label).csv")
#     CSV.write(path, combined)

#     println("\n✅ $label — EI solar report written: $path")
#     println("   Matched:   ", nrow(matched_out),   " generators")
#     println("   Unmatched: ", nrow(unmatched_out), " generators")
#     println("   Total:     ", nrow(combined),      " generators")

#     return combined
# end

# OUTPUT_DIR = "/Users/sabrilg/Documents/GitHub/va_updates"

# # ── Run for each state ────────────────────────────────────────────────────────
# ei_solar_VA = write_ei_solar_report("VA", va_solar_matched, va_solar_unmatched, OUTPUT_DIR)
# ei_solar_MD = write_ei_solar_report("MD", md_solar_matched, md_solar_unmatched, OUTPUT_DIR)
# ei_solar_WV = write_ei_solar_report("WV", wv_solar_matched, wv_solar_unmatched, OUTPUT_DIR)

# ── 11. Find EIA solar plants not matched to any EI generator ─────────────────
# ── Deduplicate eia_has_both before passing to build_eia_only_solar_df ────────
# Keep only one row per Plant ID + Generator ID (take first occurrence)
function dedup_eia_has_both(label::String, df::DataFrame)
    before = nrow(df)
    deduped = unique(df, [ "uid"])
    after = nrow(deduped)
    println("$label — eia_has_both dedup: $before → $after rows (removed $(before - after) duplicates)")
    return deduped
end

function find_eia_unmatched(label::String, ei_eia_result::DataFrame, eia_expanded::DataFrame, state::String)

    # ── Use uid to identify exactly which EIA rows were matched ──────────────
    matched_uids = Set(skipmissing(ei_eia_result[!, "uid"]))

    eia_state_pv = filter(row -> coalesce(row["State"] == state, false) &&
                                 coalesce(row["Prime Mover Code"] == "PV", false), eia_expanded)

    # ── An EIA row is unmatched only if its uid never appeared in a join ──────
    eia_unmatched = filter(row -> !in(coalesce(row["uid"], ""), matched_uids), eia_state_pv)
    #eia_unmatched  = dedup_eia_has_both(state, eia_unmatched)

    has_both    = filter(row -> !ismissing(row["BusID_int"]) && !ismissing(row["BusName"]) && row["BusName"] != "", eia_unmatched)
    #has_both  = dedup_eia_has_both(state, has_both)
    has_neither = filter(row -> ismissing(row["BusID_int"]) && (ismissing(row["BusName"]) || row["BusName"] == ""), eia_unmatched)
    #has_neither   = dedup_eia_has_both(state, has_neither )

    println("\n", "="^60)
    println("$label — EIA Solar Plants NOT in EI model")
    println("="^60)
    println("  Total EIA PV entries for $label:      ", nrow(eia_state_pv))
    println("  ⚠️  Not matched to any EI generator:  ", nrow(eia_unmatched))
    println("─"^60)
    println("  Classification:")
    println("    ✅ Has BusID + BusName:  ", nrow(has_both))
    println("    ❌ Has neither:          ", nrow(has_neither))
    println("="^60)

    EIA_UNMATCHED_COLS = [
        "Plant ID", "Plant Name", "Generator ID",
        "BusID_int", "BusName", "kV",
        "eia_capacity_mw", "Technology", "Prime Mover Code",
    ]

    println("\n  ✅ Has BusID + BusName (", nrow(has_both), " rows):")
    show(DataFrames.select(has_both, EIA_UNMATCHED_COLS), allrows=true)

    println("\n  ❌ Has neither BusID nor BusName (", nrow(has_neither), " rows):")
    show(DataFrames.select(has_neither, EIA_UNMATCHED_COLS), allrows=true)

    return eia_unmatched, has_both, has_neither
end

# ── Apply MANUAL_OVERRIDES to any EIA-derived DataFrame ───────────────────────
# Works on three different column schemas:
#   :eia_has_both      → columns BusID_int, BusName, kV
#   :neither_mmwg      → columns Bus Number, Load Flow  Bus Name, Bus kV
#   :fuzzy             → columns Bus Number, Load Flow  Bus Name, Bus kV, match_score
#
# For each row whose Plant ID is in overrides:
#   - missing override value  → row is dropped (plant not in EI model)
#   - Int override value      → bus columns are rewritten from MMWG lookup
#
# Returns (patched_df, n_overridden, n_dropped)
function apply_overrides_to_eia_df(
    label::String,
    df::DataFrame,
    schema::Symbol,           # :eia_has_both | :neither_mmwg | :fuzzy
    mmwg_slim::DataFrame,
    overrides::Dict,
)
    nrow(df) == 0 && return df, 0, 0

    keep    = Int[]
    patched = DataFrame()
    n_overridden = 0
    n_dropped    = 0

    for (i, row) in enumerate(eachrow(df))
        pid = coalesce(row["Plant ID"], missing)
        ismissing(pid) && (push!(keep, i); continue)
        haskey(overrides, Int(pid)) || (push!(keep, i); continue)

        bus_override = overrides[Int(pid)]

        if ismissing(bus_override)
            println("  ❌ [$label] Plant $pid ($(row["Plant Name"])) — dropped (not in EI model)")
            n_dropped += 1
            continue   # don't push to keep, don't add to patched
        end

        # ── look up replacement bus in MMWG ───────────────────────────────────
        mmwg_row = filter(r -> coalesce(r["Bus Number"] == bus_override, false), mmwg_slim)
        if nrow(mmwg_row) == 0
            println("  ⚠️  [$label] Plant $pid: override bus $bus_override not in MMWG — kept as-is")
            push!(keep, i)
            continue
        end

        new_row = copy(DataFrame(row))

        if schema == :eia_has_both
            # BusID_int / BusName / kV  (used by build_eia_only_solar_df → s3)
            new_row[!, "BusID_int"] .= bus_override
            new_row[!, "BusName"]   .= mmwg_row[1, "Load Flow  Bus Name"]
            new_row[!, "kV"]        .= mmwg_row[1, "Bus kV"]
        else
            # neither_mmwg + fuzzy both use MMWG column names
            new_row[!, "Bus Number"]          .= bus_override
            new_row[!, "Load Flow  Bus Name"] .= mmwg_row[1, "Load Flow  Bus Name"]
            new_row[!, "Bus kV"]              .= mmwg_row[1, "Bus kV"]
            new_row[!, "English Name"]        .= mmwg_row[1, "English Name"]
            new_row[!, "EIA Plant Code"]      .= mmwg_row[1, "EIA Plant Code"]
            new_row[!, "Region/PC"]           .= mmwg_row[1, "Region/PC"]
            if schema == :fuzzy && hasproperty(new_row, :match_score)
                new_row[!, "match_score"] .= 1.0
            end
        end

        println("  ✅ [$label/$(schema)] Plant $pid ($(row["Plant Name"])) → bus $bus_override",
                " ($(mmwg_row[1, "Load Flow  Bus Name"]))")
        append!(patched, new_row, promote=true)
        n_overridden += 1
    end

    result = vcat(df[keep, :], patched, cols=:union)

    println("  [$label/$(schema)] overrides: $n_overridden corrected, $n_dropped dropped",
            " → $(nrow(result)) rows remaining")
    return result, n_overridden, n_dropped
end

# ── Build claimed MMWG bus sets from EI pipeline BEFORE calling EIA pipeline ──
# va_claimed_mmwg_buses = Set{Any}(skipmissing(va_mmwg_matched[!, "bus_number"]))
# md_claimed_mmwg_buses = Set{Any}(skipmissing(md_mmwg_matched[!, "bus_number"]))
# wv_claimed_mmwg_buses = Set{Any}(skipmissing(wv_mmwg_matched[!, "bus_number"]))

# ── 12. Find EIA unmatched ───────────────────────────────────────────────────
va_eia_unmatched, va_eia_has_both, va_eia_neither =
    find_eia_unmatched("VA", va_ei_eia_result, eia_expanded, "VA")
md_eia_unmatched, md_eia_has_both, md_eia_neither =
    find_eia_unmatched("MD", md_ei_eia_result, eia_expanded, "MD")
wv_eia_unmatched, wv_eia_has_both, wv_eia_neither =
    find_eia_unmatched("WV", wv_ei_eia_result, eia_expanded, "WV")

va_eia_unmatched = dedup_eia_has_both("VA", va_eia_unmatched)
md_eia_unmatched = dedup_eia_has_both("MD", md_eia_unmatched)
wv_eia_unmatched = dedup_eia_has_both("WV", wv_eia_unmatched)

va_eia_has_both  = dedup_eia_has_both("VA", va_eia_has_both)
md_eia_has_both  = dedup_eia_has_both("MD", md_eia_has_both)
wv_eia_has_both  = dedup_eia_has_both("WV", wv_eia_has_both)

# ── 12b. Patch has_both: override bad EIA buses BEFORE build_eia_only_solar_df
println("\n── Applying MANUAL_OVERRIDES to eia_has_both (EIA_ONLY source) ──")
va_eia_has_both, _, _ = apply_overrides_to_eia_df("VA", va_eia_has_both, :eia_has_both, mmwg_slim, MANUAL_OVERRIDES)
md_eia_has_both, _, _ = apply_overrides_to_eia_df("MD", md_eia_has_both, :eia_has_both, mmwg_slim, MANUAL_OVERRIDES)
wv_eia_has_both, _, _ = apply_overrides_to_eia_df("WV", wv_eia_has_both, :eia_has_both, mmwg_slim, MANUAL_OVERRIDES)


# ── 13. Match EIA "neither" plants to MMWG by Plant Name ≈ English Name ───────
function match_neither_to_mmwg(label::String, neither::DataFrame, mmwg_slim::DataFrame)
    println("\n", "="^60)
    println("$label — EIA 'neither' plants → MMWG name match")
    println("="^60)

    region = get(STATE_REGION, label, missing)
    mmwg_available = !ismissing(region) ?
        filter(r -> coalesce(r["Region/PC"] == region, false), mmwg_slim) : mmwg_slim

    println("  MMWG available: ", nrow(mmwg_available), " (region filtered)")

    normalize(s) = ismissing(s) ? "" : lowercase(strip(string(s)))
    matches  = DataFrame()
    no_match = DataFrame()

    for row in eachrow(neither)
        plant_name = normalize(row["Plant Name"])
        mmwg_match = filter(r -> normalize(r["English Name"]) == plant_name, mmwg_available)
        if nrow(mmwg_match) > 1
            println("  ⚠️  Multiple MMWG matches for '$(row["Plant Name"])' ($(nrow(mmwg_match)) rows) — taking first")
        end
        if nrow(mmwg_match) > 0
            new_row = copy(DataFrame(row))
            new_row[!, "Bus Number"]          .= mmwg_match[1, "Bus Number"]
            new_row[!, "Load Flow  Bus Name"] .= mmwg_match[1, "Load Flow  Bus Name"]
            new_row[!, "English Name"]        .= mmwg_match[1, "English Name"]
            new_row[!, "EIA Plant Code"]      .= mmwg_match[1, "EIA Plant Code"]
            new_row[!, "Bus kV"]              .= mmwg_match[1, "Bus kV"]
            new_row[!, "Region/PC"]           .= mmwg_match[1, "Region/PC"]
            append!(matches, new_row, promote=true)
        else
            append!(no_match, DataFrame(row), promote=true)
        end
    end

    println("  Total 'neither':          ", nrow(neither))
    println("  ✅ Matched via name:      ", nrow(matches))
    println("  ⚠️  Still no match:       ", nrow(no_match))
    println("="^60)

    MATCH_COLS = [
        "Plant ID", "Plant Name", "Generator ID", "eia_capacity_mw",
        "Bus Number", "Load Flow  Bus Name", "English Name", "EIA Plant Code", "Bus kV", "Region/PC",
    ]
    NO_MATCH_COLS = [
        "Plant ID", "Plant Name", "Generator ID", "eia_capacity_mw",
    ]

    println("\n  ✅ Matched (", nrow(matches), " rows):")
    show(DataFrames.select(matches, MATCH_COLS), allrows=true)

    println("\n  ⚠️  Still no match (", nrow(no_match), " rows):")
    show(DataFrames.select(no_match, NO_MATCH_COLS), allrows=true)

    return matches, no_match
end

# ── 14. Exact MMWG match — pass claimed buses ─────────────────────────────────
# va_neither_mmwg_matched, va_neither_still_unmatched =
#     match_neither_to_mmwg("VA", va_eia_neither, mmwg_slim, claimed_bus_numbers=va_claimed_mmwg_buses)
# md_neither_mmwg_matched, md_neither_still_unmatched =
#     match_neither_to_mmwg("MD", md_eia_neither, mmwg_slim, claimed_bus_numbers=md_claimed_mmwg_buses)
# wv_neither_mmwg_matched, wv_neither_still_unmatched =
#     match_neither_to_mmwg("WV", wv_eia_neither, mmwg_slim, claimed_bus_numbers=wv_claimed_mmwg_buses)

# ── 13. Exact MMWG name match ────────────────────────────────────────────────
va_neither_mmwg_matched, va_neither_still_unmatched =
    match_neither_to_mmwg("VA", va_eia_neither, mmwg_slim)
md_neither_mmwg_matched, md_neither_still_unmatched =
    match_neither_to_mmwg("MD", md_eia_neither, mmwg_slim)
wv_neither_mmwg_matched, wv_neither_still_unmatched =
    match_neither_to_mmwg("WV", wv_eia_neither, mmwg_slim)

# ── 13b. Patch neither_mmwg_matched: override bad MMWG buses BEFORE fuzzy ────
println("\n── Applying MANUAL_OVERRIDES to neither_mmwg_matched (EIA_MMWG_EXACT source) ──")
va_neither_mmwg_matched, _, _ = apply_overrides_to_eia_df("VA", va_neither_mmwg_matched, :neither_mmwg, mmwg_slim, MANUAL_OVERRIDES)
md_neither_mmwg_matched, _, _ = apply_overrides_to_eia_df("MD", md_neither_mmwg_matched, :neither_mmwg, mmwg_slim, MANUAL_OVERRIDES)
wv_neither_mmwg_matched, _, _ = apply_overrides_to_eia_df("WV", wv_neither_mmwg_matched, :neither_mmwg, mmwg_slim, MANUAL_OVERRIDES)



# ── 15. Fuzzy name match for still-unmatched plants ───────────────────────────
function fuzzy_match_to_mmwg(label::String, still_unmatched::DataFrame, mmwg_slim::DataFrame;
    threshold::Float64 = 0.7)
    println("\n", "="^60)
    println("$label — Fuzzy name match → MMWG (threshold = $threshold)")
    println("="^60)

    if isempty(still_unmatched)
        println("  ℹ️  No unmatched plants — skipping")
        return DataFrame(), DataFrame()
    end

    region = get(STATE_REGION, label, missing)
    mmwg_available = !ismissing(region) ?
        filter(r -> coalesce(r["Region/PC"] == region, false), mmwg_slim) : mmwg_slim

    if isempty(mmwg_available)
        println("  ⚠️  No MMWG entries available — skipping fuzzy match")
        return DataFrame(), copy(still_unmatched)
    end
    println("  MMWG available: ", nrow(mmwg_available), " (region filtered)")

    normalize(s) = ismissing(s) ? "" : lowercase(strip(string(s)))

    mmwg_names    = [normalize(r["English Name"]) for r in eachrow(mmwg_available)]
    mmwg_bus_nums = mmwg_available[!, "Bus Number"]
    mmwg_bus_kv   = mmwg_available[!, "Bus kV"]
    mmwg_lf_names = mmwg_available[!, "Load Flow  Bus Name"]
    mmwg_eia_code = mmwg_available[!, "EIA Plant Code"]

    fuzzy_matched   = DataFrame()
    fuzzy_unmatched = DataFrame()

    for row in eachrow(still_unmatched)
        plant_name = normalize(row["Plant Name"])

        scores     = [compare(plant_name, mn, Jaro()) for mn in mmwg_names]
        best_idx   = argmax(scores)
        best_score = scores[best_idx]

        if best_score >= threshold
            new_row = copy(DataFrame(row))
            new_row[!, "Bus Number"]          .= mmwg_bus_nums[best_idx]
            new_row[!, "Load Flow  Bus Name"] .= mmwg_lf_names[best_idx]
            new_row[!, "English Name"]        .= mmwg_available[best_idx, "English Name"]
            new_row[!, "EIA Plant Code"]      .= mmwg_eia_code[best_idx]
            new_row[!, "Bus kV"]              .= mmwg_bus_kv[best_idx]
            new_row[!, "match_score"]         .= round(best_score, digits=3)
            new_row[!, "Region/PC"]           .= mmwg_available[best_idx, "Region/PC"]
            append!(fuzzy_matched, new_row, promote=true)
        else
            new_row = copy(DataFrame(row))
            new_row[!, "best_mmwg_name"] .= mmwg_available[best_idx, "English Name"]
            new_row[!, "match_score"]    .= round(best_score, digits=3)
            append!(fuzzy_unmatched, new_row, promote=true)
        end
    end

    println("  Total still unmatched:          ", nrow(still_unmatched))
    println("  ✅ Fuzzy matched (≥$threshold):  ", nrow(fuzzy_matched))
    println("  ⚠️  No fuzzy match:              ", nrow(fuzzy_unmatched))
    println("="^60)

    FUZZY_MATCH_COLS = [
        "Plant ID", "Plant Name", "Generator ID", "eia_capacity_mw",
        "Bus Number", "English Name", "Bus kV", "Region/PC", "match_score",
    ]
    FUZZY_NO_MATCH_COLS = [
        "Plant ID", "Plant Name", "Generator ID", "eia_capacity_mw",
        "best_mmwg_name", "match_score",
    ]

    println("\n  ✅ Fuzzy matched (", nrow(fuzzy_matched), " rows) — sorted by capacity ↓:")
    if nrow(fuzzy_matched) > 0
        show(
            sort(DataFrames.select(fuzzy_matched, FUZZY_MATCH_COLS),
                :eia_capacity_mw, rev=true),
            allrows=true
        )
    else
        println("  (none)")
    end

    println("\n  ⚠️  No fuzzy match (", nrow(fuzzy_unmatched), " rows) — sorted by capacity ↓:")
    if nrow(fuzzy_unmatched) > 0
        show(
            sort(DataFrames.select(fuzzy_unmatched, FUZZY_NO_MATCH_COLS),
                :eia_capacity_mw, rev=true),
            allrows=true
        )
    else
        println("  (none)")
    end

    return fuzzy_matched, fuzzy_unmatched
end

# ── 15b. Manual overrides: Plant ID → (Bus Number, English Name, Bus kV) ──────
include("update_utils.jl")



function apply_manual_overrides!(fuzzy_matched::DataFrame, fuzzy_unmatched::DataFrame,
                                  mmwg_slim::DataFrame, overrides::Dict)
    corrected = DataFrame()
    removed   = Int[]

    for (i, row) in enumerate(eachrow(fuzzy_matched))
        pid = row["Plant ID"]
        ismissing(pid) && continue
        haskey(overrides, Int(pid)) || continue

        bus_override = overrides[Int(pid)]
        push!(removed, i)

        if ismissing(bus_override)
            println("  ❌ Removing Plant ID $pid ($(row["Plant Name"])) — marked not in model")
        else
            # look up all MMWG fields from bus number alone
            mmwg_row = filter(r -> coalesce(r["Bus Number"] == bus_override, false), mmwg_slim)
            if nrow(mmwg_row) == 0
                println("  ⚠️  Plant ID $pid: bus $bus_override not found in MMWG — skipping override")
                pop!(removed)   # don't remove if we can't resolve it
                continue
            end
            new_row = copy(DataFrame(row))
            new_row[!, "Bus Number"]          .= bus_override
            new_row[!, "English Name"]        .= mmwg_row[1, "English Name"]
            new_row[!, "Bus kV"]              .= mmwg_row[1, "Bus kV"]
            new_row[!, "Load Flow  Bus Name"] .= mmwg_row[1, "Load Flow  Bus Name"]
            new_row[!, "EIA Plant Code"]      .= mmwg_row[1, "EIA Plant Code"]
            new_row[!, "Region/PC"]           .= mmwg_row[1, "Region/PC"]
            new_row[!, "match_score"]         .= 1.0
            println("  ✅ Override Plant ID $pid ($(row["Plant Name"])) → bus $bus_override ($(mmwg_row[1, "English Name"]))")
            append!(corrected, new_row, promote=true)
        end
    end

    keep  = setdiff(1:nrow(fuzzy_matched), removed)
    final = vcat(fuzzy_matched[keep, :], corrected, cols=:union)

    println("\n  Manual overrides applied: ", length(removed), " rows replaced/removed")
    println("  Final fuzzy_matched rows: ", nrow(final))
    return final
end

# ── grow claimed set with exact matches before fuzzy ─────────────────────────

#union!(va_claimed_mmwg_buses, Set{Any}(skipmissing(va_neither_mmwg_matched[!, "Bus Number"])))
#union!(md_claimed_mmwg_buses, Set{Any}(skipmissing(md_neither_mmwg_matched[!, "Bus Number"])))
#union!(wv_claimed_mmwg_buses, Set{Any}(skipmissing(wv_neither_mmwg_matched[!, "Bus Number"])))

# ── 16. Fuzzy match — pass grown claimed set ──────────────────────────────────
# va_fuzzy_matched, va_fuzzy_unmatched =
#     fuzzy_match_to_mmwg("VA", va_neither_still_unmatched, mmwg_slim, claimed_bus_numbers=va_claimed_mmwg_buses)
# md_fuzzy_matched, md_fuzzy_unmatched =
#     fuzzy_match_to_mmwg("MD", md_neither_still_unmatched, mmwg_slim, claimed_bus_numbers=md_claimed_mmwg_buses)
# wv_fuzzy_matched, wv_fuzzy_unmatched =
#     fuzzy_match_to_mmwg("WV", wv_neither_still_unmatched, mmwg_slim, claimed_bus_numbers=wv_claimed_mmwg_buses)

va_fuzzy_matched, va_fuzzy_unmatched =
    fuzzy_match_to_mmwg("VA", va_neither_still_unmatched, mmwg_slim)
md_fuzzy_matched, md_fuzzy_unmatched =
    fuzzy_match_to_mmwg("MD", md_neither_still_unmatched, mmwg_slim)
wv_fuzzy_matched, wv_fuzzy_unmatched =
    fuzzy_match_to_mmwg("WV", wv_neither_still_unmatched, mmwg_slim)

# ── 16b. Apply manual overrides ───────────────────────────────────────────────
va_fuzzy_matched = apply_manual_overrides!(va_fuzzy_matched, va_fuzzy_unmatched, mmwg_slim, MANUAL_OVERRIDES)
md_fuzzy_matched = apply_manual_overrides!(md_fuzzy_matched, md_fuzzy_unmatched, mmwg_slim, MANUAL_OVERRIDES)
wv_fuzzy_matched = apply_manual_overrides!(wv_fuzzy_matched, wv_fuzzy_unmatched, mmwg_slim, MANUAL_OVERRIDES)

# ── 16c. Auto-assign closest bus by location for small plants ─────────────────
"""
    auto_assign_by_location(label, df, eia_plants_loc, bus_coords; max_cap_mw=20.0)

For plants with eia_capacity_mw <= max_cap_mw that are NOT already in MANUAL_OVERRIDES,
find the closest bus by haversine distance and assign it.
Returns a DataFrame with the same structure as fuzzy_matched, with Bus Number filled in.
"""
function auto_assign_by_location(
    label::String,
    df::DataFrame,
    eia_plants_loc::DataFrame,
    bus_coords::DataFrame;
    max_cap_mw::Float64 = 20.0,
    overrides::Dict = MANUAL_OVERRIDES,
)
    result     = DataFrame()
    no_coords  = DataFrame()

    n_overridden  = 0
    n_too_large   = 0
    n_auto        = 0
    n_no_coords   = 0

    for row in eachrow(df)
        pid = coalesce(row["Plant ID"], -1)
        new_row = copy(DataFrame(row))

        if !hasproperty(new_row, :dist_km)
            new_row[!, "dist_km"] .= missing
        end

        if haskey(overrides, pid)
            n_overridden += 1
            append!(result, new_row, promote=true)
            continue
        end

        cap = coalesce(row["eia_capacity_mw"], 0.0)
        if cap > max_cap_mw
            n_too_large += 1
            append!(result, new_row, promote=true)
            continue
        end

        # ── try inline eia_lat/eia_lon first, then fall back to eia_plants_loc ──
        plant_lat = hasproperty(row, :eia_lat) ? coalesce(row[:eia_lat], missing) : missing
        plant_lon = hasproperty(row, :eia_lon) ? coalesce(row[:eia_lon], missing) : missing

        if ismissing(plant_lat) || ismissing(plant_lon)
            plant_row = filter(r -> coalesce(r["Plant Code"] == pid, false), eia_plants_loc)
            if nrow(plant_row) > 0
                plant_lat = plant_row[1, "Latitude"]
                plant_lon = plant_row[1, "Longitude"]
            end
        end

        if ismissing(plant_lat) || ismissing(plant_lon)
            n_no_coords += 1
            append!(no_coords, new_row, promote=true)
            continue
        end

        dists    = [haversine_km(plant_lat, plant_lon, r.lat, r.lon) for r in eachrow(bus_coords)]
        best_idx = argmin(dists)

        new_row[!, "Bus Number"]          .= bus_coords[best_idx, :bus_number]
        new_row[!, "Load Flow  Bus Name"] .= bus_coords[best_idx, :bus_name]
        new_row[!, "Bus kV"]              .= bus_coords[best_idx, :base_voltage]
        new_row[!, "dist_km"]             .= round(dists[best_idx], digits=2)
        n_auto += 1
        append!(result, new_row, promote=true)
    end

    println("\n", "="^60)
    println("$label — Auto-assigned by location (≤$(max_cap_mw) MW)")
    println("="^60)
    println("  Total input rows:              ", nrow(df))
    println("  ─"^30)
    println("  ✅ Auto-assigned by location:  ", n_auto)
    println("  🔒 Kept from MANUAL_OVERRIDES: ", n_overridden)
    println("  🔼 Skipped (> $(max_cap_mw) MW):       ", n_too_large)
    println("  ⚠️  No coordinates found:       ", n_no_coords)
    println("  ─"^30)
    println("  Total output rows:             ", nrow(result))
    println("="^60)

    if nrow(no_coords) > 0
        println("\n  ⚠️  Could not locate (no EIA coords):")
        show(DataFrames.select(no_coords, ["Plant ID", "Plant Name", "Generator ID", "eia_capacity_mw"]), allrows=true)
    end

    return result, no_coords
end

# ── Run on fuzzy_matched (existing) ──────────────────────────────────────────
va_fuzzy_matched, va_no_coords = auto_assign_by_location("VA", va_fuzzy_matched, eia_plants_loc, bus_coords)
md_fuzzy_matched, md_no_coords = auto_assign_by_location("MD", md_fuzzy_matched, eia_plants_loc, bus_coords)
wv_fuzzy_matched, wv_no_coords = auto_assign_by_location("WV", wv_fuzzy_matched, eia_plants_loc, bus_coords)

# ── Run on fuzzy_unmatched and merge into fuzzy_matched ───────────────────────
va_unmatched_located, va_unmatched_no_coords =
    auto_assign_by_location("VA unmatched", va_fuzzy_unmatched, eia_plants_loc, bus_coords, max_cap_mw=Inf)

md_unmatched_located, md_unmatched_no_coords =
    auto_assign_by_location("MD unmatched", md_fuzzy_unmatched, eia_plants_loc, bus_coords, max_cap_mw=Inf)

wv_unmatched_located, wv_unmatched_no_coords =
    auto_assign_by_location("WV unmatched", wv_fuzzy_unmatched, eia_plants_loc, bus_coords, max_cap_mw=Inf)

va_fuzzy_matched = vcat(va_fuzzy_matched, va_unmatched_located, cols=:union)
md_fuzzy_matched = vcat(md_fuzzy_matched, md_unmatched_located, cols=:union)
wv_fuzzy_matched = vcat(wv_fuzzy_matched, wv_unmatched_located, cols=:union)    

# ── 17. Build final per-state solar DataFrames ────────────────────────────────
function build_state_solar_df(label::String,
    solar_matched::DataFrame,   # EI → EIA2PF matched
    mmwg_matched::DataFrame)    # EI unmatched → MMWG matched

    # ── Source 1: EI → EIA2PF matched ────────────────────────────────────────
    s1 = DataFrame(
        source          = fill("EI_EIA2PF", nrow(solar_matched)),
        gen_name        = solar_matched[!, "name"],
        ts_column_name  = solar_matched[!, "ts_column_name"],
        ei_lat          = solar_matched[!, "lat"],
        ei_lon          = solar_matched[!, "lon"],
        eia_lat         = solar_matched[!, "eia_lat"],
        eia_lon         = solar_matched[!, "eia_lon"],
        bus_id          = solar_matched[!, "bus_number"],
        bus_name        = solar_matched[!, "bus"],
        ei_capacity_mw  = solar_matched[!, "rating"],
        eia_capacity_mw = solar_matched[!, "eia_capacity_mw"],
        bus_voltage_kv  = [clean_bus_voltage(x) for x in solar_matched[!, "kV"]],
        eia860_status   = solar_matched[!, "eia860_status"],
    )

    # ── Source 2: EI unmatched → MMWG matched ────────────────────────────────
    s2 = DataFrame(
        source          = fill("EI_MMWG", nrow(mmwg_matched)),
        gen_name        = mmwg_matched[!, "name"],
        ts_column_name  = mmwg_matched[!, "ts_column_name"],
        ei_lat          = mmwg_matched[!, "lat"],
        ei_lon          = mmwg_matched[!, "lon"],
        eia_lat         = fill(missing, nrow(mmwg_matched)),
        eia_lon         = fill(missing, nrow(mmwg_matched)),
        bus_id          = mmwg_matched[!, "bus_number"],
        bus_name        = mmwg_matched[!, "bus"],
        ei_capacity_mw  = mmwg_matched[!, "rating"],
        eia_capacity_mw = fill(missing, nrow(mmwg_matched)),
        bus_voltage_kv  = [clean_bus_voltage(x) for x in mmwg_matched[!, "Bus kV"]],
        eia860_status   = fill(missing, nrow(mmwg_matched)),  # not available via MMWG
    )

    final_df = vcat(s1, s2, cols=:union)
    final_df[!, "state"] .= label

    println("\n", "="^60)
    println("$label — Final Solar DataFrame (EI generators only)")
    println("="^60)
    println("  EI_EIA2PF:  ", nrow(s1), " generators")
    println("  EI_MMWG:    ", nrow(s2), " generators")
    println("  Total:      ", nrow(final_df), " generators")
    println("="^60)
    show(sort(final_df, :bus_id), allrows=true)

    return final_df
end

function clean_bus_voltage(x)
    ismissing(x) && return missing
    s = strip(string(x))
    # Extract first number found
    m = match(r"[\d.]+", s)
    isnothing(m) && return missing
    val = tryparse(Float64, m.match)
    return val
end


# ── 18. Build per-state DataFrames ───────────────────────────────────────────
va_solar = build_state_solar_df("VA", va_solar_matched, va_mmwg_matched);
md_solar = build_state_solar_df("MD", md_solar_matched, md_mmwg_matched);
wv_solar = build_state_solar_df("WV", wv_solar_matched, wv_mmwg_matched);


va_solar[!, "bus_voltage_kv"] = [clean_bus_voltage(x) for x in va_solar[!, "bus_voltage_kv"]];
md_solar[!, "bus_voltage_kv"] = [clean_bus_voltage(x) for x in md_solar[!, "bus_voltage_kv"]];
wv_solar[!, "bus_voltage_kv"] = [clean_bus_voltage(x) for x in wv_solar[!, "bus_voltage_kv"]];

# ── Reproducible generator name suffixes ──────────────────────────────────────
Random.seed!(1234)
# ── Helper: generate synthetic EI-style generator name ───────────────────────
function make_gen_name(bus_id::Any, idx::Int)
    bus    = ismissing(bus_id) ? string(rand(1000000000:9999999999)) : string(Int(bus_id))
    suffix = string(rand(1000000000:9999999999))
    return "generator-$(bus)-$(suffix)"
end

# ── Helpers: clean bus_name and bus_voltage from EIA raw strings ──────────────
function clean_bus_name(x)
    ismissing(x) && return missing
    s = strip(string(x))
    # Remove outer brackets and quotes: ['A', 'B'] → A, B
    s = replace(s, r"^\[|\]$" => "")          # remove [ ]
    s = replace(s, r"'" => "")                 # remove '
    s = strip(s)
    return s
end


function build_eia_only_solar_df(label::String,
                                  eia_has_both::DataFrame,
                                  neither_mmwg_matched::DataFrame,
                                  fuzzy_matched::DataFrame)

    # ── Source 3: EIA has BusID+BusName, not in EI ───────────────────────────
    s3 = DataFrame(
        source          = fill("EIA_ONLY", nrow(eia_has_both)),
        gen_name        = [make_gen_name(eia_has_both[i, "BusID_int"], i) for i in 1:nrow(eia_has_both)],
        ts_column_name  = fill(missing, nrow(eia_has_both)),
        ei_lat          = fill(missing, nrow(eia_has_both)),
        ei_lon          = fill(missing, nrow(eia_has_both)),
        eia_lat         = eia_has_both[!, "eia_lat"],
        eia_lon         = eia_has_both[!, "eia_lon"],
        bus_id          = eia_has_both[!, "BusID_int"],
        bus_name        = [clean_bus_name(x) for x in eia_has_both[!, "BusName"]],
        ei_capacity_mw  = fill(missing, nrow(eia_has_both)),
        eia_capacity_mw = eia_has_both[!, "eia_capacity_mw"],
        bus_voltage_kv  = [clean_bus_voltage(x) for x in eia_has_both[!, "kV"]],
    )

    # ── Source 4: EIA neither → MMWG exact name match ────────────────────────
    s4 = DataFrame(
        source          = fill("EIA_MMWG_EXACT", nrow(neither_mmwg_matched)),
        gen_name        = [make_gen_name(neither_mmwg_matched[i, "Bus Number"], i)
                           for i in 1:nrow(neither_mmwg_matched)],
        ts_column_name  = fill(missing, nrow(neither_mmwg_matched)),
        ei_lat          = fill(missing, nrow(neither_mmwg_matched)),
        ei_lon          = fill(missing, nrow(neither_mmwg_matched)),
        eia_lat         = neither_mmwg_matched[!, "eia_lat"],
        eia_lon         = neither_mmwg_matched[!, "eia_lon"],
        bus_id          = neither_mmwg_matched[!, "Bus Number"],
        bus_name        = neither_mmwg_matched[!, "Load Flow  Bus Name"],
        ei_capacity_mw  = fill(missing, nrow(neither_mmwg_matched)),
        eia_capacity_mw = neither_mmwg_matched[!, "eia_capacity_mw"],
        bus_voltage_kv  = neither_mmwg_matched[!, "Bus kV"],
    )

    # ── Source 5: EIA neither → MMWG fuzzy match ─────────────────────────────
    s5 = DataFrame(
        source          = fill("EIA_MMWG_FUZZY", nrow(fuzzy_matched)),
        gen_name        = [make_gen_name(fuzzy_matched[i, "Bus Number"], i)
                           for i in 1:nrow(fuzzy_matched)],
        ts_column_name  = fill(missing, nrow(fuzzy_matched)),
        ei_lat          = fill(missing, nrow(fuzzy_matched)),
        ei_lon          = fill(missing, nrow(fuzzy_matched)),
        eia_lat         = fuzzy_matched[!, "eia_lat"],
        eia_lon         = fuzzy_matched[!, "eia_lon"],
        bus_id          = fuzzy_matched[!, "Bus Number"],
        bus_name        = fuzzy_matched[!, "Load Flow  Bus Name"],
        ei_capacity_mw  = fill(missing, nrow(fuzzy_matched)),
        eia_capacity_mw = fuzzy_matched[!, "eia_capacity_mw"],
        bus_voltage_kv  = fuzzy_matched[!, "Bus kV"],
    )

    final_df = vcat(s3, s4, s5, cols=:union)
    final_df[!, "state"] .= label

    # Sort by source priority: EIA_ONLY → EIA_MMWG_EXACT → EIA_MMWG_FUZZY
    source_order = Dict("EIA_ONLY" => 1, "EIA_MMWG_EXACT" => 2, "EIA_MMWG_FUZZY" => 3)
    final_df[!, "source_order"] = [source_order[s] for s in final_df.source]
    sort!(final_df, [:source_order, :bus_id])
    select!(final_df, Not(:source_order))

    println("\n", "="^60)
    println("$label — EIA-only Solar DataFrame (not in EI model)")
    println("="^60)
    println("  EIA_ONLY:       ", nrow(s3), " generators")
    println("  EIA_MMWG_EXACT: ", nrow(s4), " generators")
    println("  EIA_MMWG_FUZZY: ", nrow(s5), " generators")
    println("  Total:          ", nrow(final_df), " generators")
    println("="^60)
    show(final_df, allrows=true)

    return final_df
end

# ── Run for each state ────────────────────────────────────────────────────────
va_solar_eia_only = build_eia_only_solar_df("VA", va_eia_has_both, va_neither_mmwg_matched, va_fuzzy_matched);
md_solar_eia_only = build_eia_only_solar_df("MD", md_eia_has_both, md_neither_mmwg_matched, md_fuzzy_matched);
wv_solar_eia_only = build_eia_only_solar_df("WV", wv_eia_has_both, wv_neither_mmwg_matched, wv_fuzzy_matched);

function build_export_df(ei_df::DataFrame, eia_only_df::DataFrame, label::String)

    ei_out = DataFrame(
        gen_name       = ei_df[!, "gen_name"],
        bus_id         = ei_df[!, "bus_id"],
        bus_name       = ei_df[!, "bus_name"],
        lat            = coalesce.(ei_df[!, "eia_lat"], ei_df[!, "ei_lat"]),
        lon            = coalesce.(ei_df[!, "eia_lon"], ei_df[!, "ei_lon"]),
        capacity_mw    = coalesce.(ei_df[!, "eia_capacity_mw"], ei_df[!, "ei_capacity_mw"]),
        bus_voltage_kv = ei_df[!, "bus_voltage_kv"],
        source         = ei_df[!, "source"],
        state          = fill(label, nrow(ei_df)),
    )

    eia_out = DataFrame(
        gen_name       = eia_only_df[!, "gen_name"],
        bus_id         = eia_only_df[!, "bus_id"],
        bus_name       = eia_only_df[!, "bus_name"],
        lat            = eia_only_df[!, "eia_lat"],
        lon            = eia_only_df[!, "eia_lon"],
        capacity_mw    = eia_only_df[!, "eia_capacity_mw"],
        bus_voltage_kv = eia_only_df[!, "bus_voltage_kv"],
        source         = eia_only_df[!, "source"],
        state          = fill(label, nrow(eia_only_df)),
    )

    combined = vcat(ei_out, eia_out, cols=:union)
    combined = sort(combined, :bus_id)

    println("\n", "="^60)
    println("$label — Export Summary")
    println("="^60)
    println("  EI generators:    ", nrow(ei_out))
    println("  EIA-only:         ", nrow(eia_out))
    println("  Total:            ", nrow(combined))
    for src in sort(unique(skipmissing(combined.source)))
        println("    $(src): ", count(==(src), skipmissing(combined.source)))
    end
    println("="^60)

    return combined
end
# ── Build export DataFrames ───────────────────────────────────────────────────
va_export = build_export_df(va_solar, va_solar_eia_only, "VA")
md_export = build_export_df(md_solar, md_solar_eia_only, "MD")
wv_export = build_export_df(wv_solar, wv_solar_eia_only, "WV")

skip_generators_va = [
    ("generator-316283-6731062071", "AC2-100 C_316283"),    # Row 2:  Hybrid plant + CC + 30km gap
    ("generator-316237-3396887655", "AB2-079_GEN_316237"),  # Row 13: CC + large cap mismatch (21 vs 60 MW)
    ("generator-316169-692393578",  "AC1-164 GEN_316169"),  # Row 15: CC + large cap mismatch (342 vs 175 MW)
    ("generator-270197-3774998538", "AC1-083 GEN_270197"),  # Row 31: Large cap mismatch (346 vs 120 MW)
    ("generator-316152-4933756787", "AE1-098 GEN_316152"),  # Row 39: Large cap mismatch (16.5 vs 31.4 MW)
    ("generator-316294-8067157756", "AC1-161 GEN_316294"),  # Row 40/41: Two EIA plants mapped to one bus
    ("generator-316118-8056473898", "AC1-105 GEN_316118"),  # Row 45: Hybrid plant + CC
    ("generator-316222-415485405",  "AC1-221 GEN_316222"),  # Row 46: CC + large cap mismatch (32 vs 75 MW) + 25km gap
]

skip_names = Set(name for (name, _) in skip_generators_va)

# Verify both gen_name and bus_name match before filtering
for (gen_name, bus_name) in skip_generators_va
    match = filter(row -> row.gen_name == gen_name, va_export)
    if nrow(match) == 0
        @warn "Generator not found: $gen_name"
    elseif match[1, :bus_name] != bus_name
        @warn "Bus mismatch for $gen_name: expected $bus_name, got $(match[1, :bus_name])"
    end
end

va_export_filtered = filter(row -> !(row.gen_name in skip_names), va_export)
n_removed = nrow(va_export) - nrow(va_export_filtered)
# n_removed may exceed skip list length if a gen_name has duplicate rows in va_export
@assert n_removed >= length(unique(first.(skip_generators_va))) """
    Row count mismatch after filtering: removed $n_removed but expected at least $(length(unique(first.(skip_generators_va))))
"""
@info "✅ Filtered va_export: removed $n_removed rows for $(length(skip_generators_va)) skipped generators"
@info "Removed $n_removed generators from va_export"

va_export = va_export_filtered

skip_generators_md = [
    ("generator-233923-4444098988", "AA1-102 GEN_233923"),  # Row 6: CC + large cap mismatch (175 vs 75 MW)
]

skip_names_md = Set(name for (name, _) in skip_generators_md)

# Verify both gen_name and bus_name match before filtering
for (gen_name, bus_name) in skip_generators_md
    match = filter(row -> row.gen_name == gen_name, md_export)
    if nrow(match) == 0
        @warn "Generator not found: $gen_name"
    elseif match[1, :bus_name] != bus_name
        @warn "Bus mismatch for $gen_name: expected $bus_name, got $(match[1, :bus_name])"
    end
end

md_export_filtered = filter(row -> !(row.gen_name in skip_names_md), md_export)


n_removed = nrow(md_export) - nrow(md_export_filtered)
@assert n_removed == length(unique(first.(skip_generators_md))) "Row count mismatch after filtering: removed $n_removed but expected $(length(unique(first.(skip_generators_md))))"
@info "Removed $n_removed generators from md_export"
md_export = md_export_filtered
# ── Write CSVs ────────────────────────────────────────────────────────────────
const OUTPUT_DIR = "/Users/sabrilg/Documents/GitHub/va_updates/VREdata"

CSV.write(joinpath(OUTPUT_DIR, "solar_RE_VA_EI_buses.csv"), va_export)
CSV.write(joinpath(OUTPUT_DIR, "solar_RE_MD_EI_buses.csv"), md_export)
CSV.write(joinpath(OUTPUT_DIR, "solar_RE_WV_EI_buses.csv"), wv_export)

println("\n✅ CSVs written:")
println("  → solar_RE_VA_EI_buses.csv (", nrow(va_export), " rows)")
println("  → solar_RE_MD_EI_buses.csv (", nrow(md_export), " rows)")
println("  → solar_RE_WV_EI_buses.csv (", nrow(wv_export), " rows)")


# ── Check which buses in solar exports don't exist in EI ──────────────────────
ei_valid_buses = Set{Int}(bus_coords[!, :bus_number])

println("\n", "█"^70)
println("  SOLAR EXPORT — INVALID BUS VALIDATION")
println("█"^70)

for (state_label, df, state_buses) in [
    ("VA", va_export, va_buses),
    ("MD", md_export, md_buses),
    ("WV", wv_export, wv_buses),
]
    bus_col = "bus_id" in names(df) ? "bus_id" : "bus_number"

    valid       = filter(r -> !ismissing(r[bus_col]) &&
                               Int(r[bus_col]) in ei_valid_buses, df)
    invalid     = filter(r -> !ismissing(r[bus_col]) &&
                              !(Int(r[bus_col]) in ei_valid_buses), df)
    missing_bus = filter(r ->  ismissing(r[bus_col]), df)

    state_valid_buses = Set{Int}(state_buses[!, :bus_number])
    wrong_state = filter(r -> !ismissing(r[bus_col]) &&
                               Int(r[bus_col]) in ei_valid_buses &&
                              !(Int(r[bus_col]) in state_valid_buses), df)

    println("\n── $state_label solar export ($(nrow(df)) total) ───────────────────")
    println("  ✅ Valid EI bus:                  ", nrow(valid))
    println("  ✅   of which in $state_label state buses:  ",
            nrow(valid) - nrow(wrong_state))
    println("  ⚠️   of which in OTHER state buses: ", nrow(wrong_state))
    println("  ❌ Bus NOT in EI at all:           ", nrow(invalid))
    println("  ❓ Missing bus_id:                 ", nrow(missing_bus))

    if nrow(invalid) > 0
        println("\n  ❌ Generators pointing to non-existent EI bus:")
        show(DataFrames.select(invalid,
            intersect(["gen_name", "bus_id", "bus_name",
                       "source", "capacity_mw", "bus_voltage_kv",
                       "lat", "lon"],
                      names(invalid))), allrows=true)
    end

    if nrow(wrong_state) > 0
        println("\n  ⚠️  Generators on a valid EI bus but outside $state_label:")
        show(DataFrames.select(wrong_state,
            intersect(["gen_name", "bus_id", "bus_name",
                       "source", "capacity_mw", "bus_voltage_kv"],
                      names(wrong_state))), allrows=true)
    end

    if nrow(missing_bus) > 0
        println("\n  ❓ Generators with no bus assigned:")
        show(DataFrames.select(missing_bus,
            intersect(["gen_name", "bus_id", "bus_name",
                       "source", "capacity_mw"],
                      names(missing_bus))), allrows=true)
    end
end
println("█"^70)

# ── Solar EI Update Report & Diagnosis ───────────────────────────────────────

# ── Combine all state updates ─────────────────────────────────────────────────
all_solar_updates = vcat(va_export, md_export, wv_export, cols=:union)

# ── EIA2PF reference totals ───────────────────────────────────────────────────
eia_pv_ref = filter(row -> coalesce(row["Prime Mover Code"] == "PV", false) &&
                            coalesce(row["State"] in ["VA", "MD", "WV"], false),
                    eia_2_pf_mapping)

eia_pv_ref_by_state = combine(groupby(eia_pv_ref, "State"),
    "Nameplate Capacity (MW)" => (x -> sum(skipmissing(x))) => "eia2pf_total_mw",
    nrow => "eia2pf_n_generators",
)

# ── Update totals by state ────────────────────────────────────────────────────
solar_update_by_state = combine(groupby(all_solar_updates, "state"),
    "capacity_mw" => (x -> sum(skipmissing(x))) => "update_total_mw",
    nrow => "update_n_generators",
)
DataFrames.rename!(solar_update_by_state, "state" => "State")

# ── Update totals by source ───────────────────────────────────────────────────
solar_update_by_source = sort(
    combine(groupby(all_solar_updates, ["state", "source"]),
        "capacity_mw" => (x -> sum(skipmissing(x))) => "total_mw",
        nrow => "n_generators",
    ), ["state", "source"])

# ── Join for comparison ───────────────────────────────────────────────────────
solar_comparison = leftjoin(eia_pv_ref_by_state, solar_update_by_state, on = "State")
solar_comparison[!, "diff_mw"] = solar_comparison[!, "update_total_mw"] .-
                                   solar_comparison[!, "eia2pf_total_mw"]
solar_comparison[!, "pct_captured"] = round.(
    solar_comparison[!, "update_total_mw"] ./
    solar_comparison[!, "eia2pf_total_mw"] .* 100, digits = 1)

# ── Source priority legend ────────────────────────────────────────────────────
const SOLAR_SOURCE_LABELS = Dict(
    "EI_EIA2PF"      => "EI gen already in EI model, matched to EIA2PF bus",
    "EI_MMWG"        => "EI gen already in EI model, matched via MMWG bus",
    "EIA_ONLY"       => "New gen: in EIA2PF with BusID+BusName, NOT in EI",
    "EIA_MMWG_EXACT" => "New gen: EIA has no bus → found via MMWG exact name",
    "EIA_MMWG_FUZZY" => "New gen: EIA has no bus → found via MMWG fuzzy/location",
)

# ─────────────────────────────────────────────────────────────────────────────
println("\n", "█"^70)
println("  SOLAR GENERATORS — EI UPDATE REPORT")
println("  States: VA | MD | WV       Tech: Solar PV (Prime Mover: PV)")
println("█"^70)

# ── Section 1: Source methodology ────────────────────────────────────────────
println("\n📖 SOURCE METHODOLOGY")
println("─"^70)
println("  Each solar generator update is assigned one of 5 sources,")
println("  in priority order (highest → lowest confidence):\n")
for (i, src) in enumerate(["EI_EIA2PF", "EI_MMWG", "EIA_ONLY",
                            "EIA_MMWG_EXACT", "EIA_MMWG_FUZZY"])
    println("  $i. $src")
    println("     └─ $(SOLAR_SOURCE_LABELS[src])")
end

# ── Section 2: Manual overrides applied ──────────────────────────────────────
println("\n\n🔧 MANUAL BUS OVERRIDES APPLIED")
println("─"^70)
println("  $(length(MANUAL_OVERRIDES)) plant-level bus overrides were applied")
println("  (see MANUAL_OVERRIDES dict in script for full list)\n")

# Group overrides by state using eia_slim for lookup
for st in ["VA", "MD", "WV"]
    state_overrides = filter(kv -> begin
        pid = kv[1]
        row = filter(r -> coalesce(r["Plant ID"] == pid, false) &&
                          coalesce(r["State"] == st, false), eia_slim)
        nrow(row) > 0
    end, MANUAL_OVERRIDES)
    println("  $st: $(length(state_overrides)) overrides")
end

# ── Section 3: State-level capacity comparison ───────────────────────────────
println("\n\n📊 CAPACITY COMPARISON — EI UPDATE vs EIA2PF REFERENCE")
println("─"^70)
println("  EIA2PF = reference total from EIA Form 860 plant-to-bus mapping")
println("  EI Update = generators included in this update\n")

show(solar_comparison, allrows=true)

println("\n\n  ⚠️  Notes on gaps (diff_mw < 0 or pct_captured < 100):")
for row in eachrow(solar_comparison)
    pct  = coalesce(row.pct_captured, 0.0)
    diff = coalesce(row.diff_mw, 0.0)
    if pct < 95.0
        println("  • $(row.State): capturing $(pct)% — missing ~$(abs(round(diff, digits=1))) MW")
        println("    Likely causes: plant has no bus assignment in EIA2PF or MMWG,")
        println("    EI model pre-dates EIA entry, or project not yet in service.")
    elseif pct > 105.0
        println("  • $(row.State): capturing $(pct)% — OVER EIA2PF by $(round(diff, digits=1)) MW")
        println("    Possible cause: EI model includes generators not yet in EIA2PF (proposed/new).")
    else
        println("  • $(row.State): ✅ $(pct)% captured — within ±5% of EIA2PF reference")
    end
end

# ── Section 4: Breakdown by source ───────────────────────────────────────────
println("\n\n📋 BREAKDOWN BY MATCH SOURCE")
println("─"^70)
show(solar_update_by_source, allrows=true)

println("\n\n  Source mix interpretation:")
for st in ["VA", "MD", "WV"]
    st_rows = filter(r -> r.state == st, solar_update_by_source)
    nrow(st_rows) == 0 && continue
    total_mw = sum(skipmissing(st_rows.total_mw))
    ei_mw    = sum(skipmissing(
        filter(r -> startswith(r.source, "EI_"),  st_rows).total_mw))
    new_mw   = sum(skipmissing(
        filter(r -> startswith(r.source, "EIA_"), st_rows).total_mw))
    println("  $st:  EI existing = $(round(ei_mw,    digits=1)) MW",
            " | New to add = $(round(new_mw, digits=1)) MW",
            " | Total = $(round(total_mw,   digits=1)) MW")
end

# ── Section 5: Data quality flags ────────────────────────────────────────────
println("\n\n🔍 DATA QUALITY FLAGS")
println("─"^70)

for (label, df) in [("VA", va_export),
                    ("MD", md_export),
                    ("WV", wv_export)]
    no_bus   = filter(r -> ismissing(r.bus_id),      df)
    no_cap   = filter(r -> ismissing(r.capacity_mw), df)
    no_loc   = filter(r -> ismissing(r.lat) || ismissing(r.lon), df)

    # Sources that need human verification:
    # EIA_MMWG_EXACT    → manually overridden bus assignment (was fuzzy, corrected by MANUAL_OVERRIDES)
    # EIA_MMWG_FUZZY    → nearest-bus by haversine distance (auto_assign_by_location)
    needs_review = filter(r -> coalesce(r.source, "") in
                               ["EIA_MMWG_EXACT", "EIA_MMWG_FUZZY"], df)
    manual_ovrd  = filter(r -> coalesce(r.source, "") == "EIA_MMWG_EXACT", df)
    loc_assigned = filter(r -> coalesce(r.source, "") == "EIA_MMWG_FUZZY",  df)

    println("\n  $label ($(nrow(df)) total generators):")
    println("    ❌ Missing bus_id:            ", nrow(no_bus),
            nrow(no_bus)  > 0 ? "  ← cannot be added to EI model" : "  ✅")
    println("    ⚠️  Missing capacity_mw:      ", nrow(no_cap),
            nrow(no_cap)  > 0 ? "  ← will need manual capacity lookup" : "  ✅")
    println("    📍 Missing lat/lon:           ", nrow(no_loc),
            nrow(no_loc)  > 0 ? "  ← time series assignment may fail" : "  ✅")
    println("    🔧 Manual override (MMWG):    ", nrow(manual_ovrd),
            nrow(manual_ovrd)  > 0 ? "  ← bus set via MANUAL_OVERRIDES dict" : "  ✅")
    println("    📍 Location-assigned (≤20MW): ", nrow(loc_assigned),
            nrow(loc_assigned) > 0 ? "  ← nearest bus by haversine, verify dist_km" : "  ✅")

    if nrow(no_bus) > 0
        println("\n    ❌ $label generators with no bus (cannot update EI):")
        println("       → ", nrow(no_bus), " generators: ",
                join(no_bus[!, "gen_name"], ", "))
    end

    if nrow(manual_ovrd) > 0
        println("\n    🔧 $label manually overridden: ",
                join(manual_ovrd[!, "gen_name"], ", "))
    end

    if nrow(loc_assigned) > 0
        println("\n    📍 $label location-assigned: ",
                join(loc_assigned[!, "gen_name"], ", "))
    end
end

# ── Section 6: EI misclassification check ────────────────────────────────────
println("\n\n🔁 EI GENERATOR MISCLASSIFICATION CHECK")
println("─"^70)
println("  Solar PV generators in EI model matched to EIA PV entries.")
println("  Any non-Solar type here is misclassified in EI.\n")

for (label, result) in [("VA", va_ei_eia_result),
                         ("MD", md_ei_eia_result),
                         ("WV", wv_ei_eia_result)]
    matched = filter(row -> !ismissing(row["Plant ID"]), result)
    nrow(matched) == 0 && continue
    println("  $label:")
    for gt in sort(unique(skipmissing(matched.generator_type)))
        n    = count(==(gt), skipmissing(matched.generator_type))
        flag = gt == "Solar" ? "✅" : "⚠️  MISCLASSIFIED"
        println("    $flag  $gt: $n")
    end
end

# ── Section 7: Summary counts ─────────────────────────────────────────────────
println("\n\n📌 FINAL SUMMARY — SOLAR GENERATORS TO UPDATE IN EI")
println("─"^70)
println("  'EI_*'  sources = existing EI generators with updated metadata")
println("  'EIA_*' sources = NEW generators to be added to EI model\n")
safe_mw(df) = isempty(df) ? 0.0 :
              round(sum(Float64.(coalesce.(df.capacity_mw, 0.0))), digits=1)

total_solar_update = 0
total_solar_new    = 0

for (label, df) in [("VA", va_export),
                    ("MD", md_export),
                    ("WV", wv_export)]
    ei_rows  = filter(r -> startswith(coalesce(r.source, ""), "EI_"),  df)
    new_rows = filter(r -> startswith(coalesce(r.source, ""), "EIA_"), df)
    global total_solar_update += nrow(ei_rows)
    global total_solar_new    += nrow(new_rows)

    ei_mw  = safe_mw(ei_rows)
    new_mw = safe_mw(new_rows)

    n_manual = count(kv -> begin
        pid = kv[1]
        row = filter(r -> coalesce(r["Plant ID"] == pid, false) &&
                          coalesce(r["State"] == label, false), eia_slim)
        nrow(row) > 0
    end, MANUAL_OVERRIDES)

    println("  $label:")
    println("    Update existing EI gens:  $(nrow(ei_rows))  ($(ei_mw) MW)")
    println("    Add new gens to EI:       $(nrow(new_rows))  ($(new_mw) MW)")
    println("    Manual bus overrides:     $n_manual")
end

println("\n  ─"^35)
println("  TOTAL update existing: $total_solar_update generators")
println("  TOTAL add new:         $total_solar_new generators")
println("  TOTAL manual overrides: $(length(MANUAL_OVERRIDES))")
println("█"^70)

# ── Plots ─────────────────────────────────────────────────────────────────────
sp1 = plot(
    [bar(x    = solar_comparison[!, "State"],
         y    = solar_comparison[!, "eia2pf_total_mw"],
         name = "EIA2PF Reference (MW)",
         marker_color = "steelblue"),
     bar(x    = solar_comparison[!, "State"],
         y    = solar_comparison[!, "update_total_mw"],
         name = "EI Update Total (MW)",
         marker_color = "darkorange")],
    Layout(
        title       = "Solar: EI Update vs EIA2PF Reference (MW)",
        barmode     = "group",
        xaxis_title = "State",
        yaxis_title = "Capacity (MW)",
        legend      = attr(orientation="h", y=-0.2),
    )
)

sp2 = plot(
    [bar(x    = filter(r -> r.source == src, solar_update_by_source)[!, "state"],
         y    = filter(r -> r.source == src, solar_update_by_source)[!, "total_mw"],
         name = src,
         marker_color = get(SOURCE_COLORS, src, "gray"))
     for src in ["EI_EIA2PF", "EI_MMWG", "EIA_ONLY",
                 "EIA_MMWG_EXACT", "EIA_MMWG_FUZZY"]
     if src in unique(solar_update_by_source[!, "source"])],
    Layout(
        title       = "Solar EI Update — Capacity by Match Source (MW)",
        barmode     = "stack",
        xaxis_title = "State",
        yaxis_title = "Capacity (MW)",
        legend      = attr(orientation="h", y=-0.2),
    )
)

sp3 = plot(
    bar(
        x    = solar_comparison[!, "State"],
        y    = solar_comparison[!, "pct_captured"],
        marker_color = [
            pct >= 95 ? "#2ecc71" : pct >= 80 ? "#e67e22" : "#e74c3c"
            for pct in coalesce.(solar_comparison[!, "pct_captured"], 0.0)
        ],
        text         = [string(p, "%") for p in
                        coalesce.(solar_comparison[!, "pct_captured"], 0.0)],
        textposition = "outside",
    ),
    Layout(
        title       = "Solar EI Update — % of EIA2PF Capacity Captured",
        xaxis_title = "State",
        yaxis_title = "% Captured",
        yaxis       = attr(range=[0, 115]),
        shapes      = [attr(type="line", x0=-0.5, x1=2.5,
                            y0=100, y1=100,
                            line=attr(color="black", dash="dash", width=1))],
    )
)

display(sp1)
display(sp2)
display(sp3)


#### WIND ─────────────────────────────────────────────────────────────────────

# ── Wind prime mover codes ────────────────────────────────────────────────────
const WIND_PM_CODES = ["WT", "WS"]   # WT = onshore, WS = offshore

# NOTE: eia_slim, eia_expanded, mmwg_slim, STATE_REGION, parse_all_busids
#       are already defined in the solar section above — NOT redefined here.

# ── Column sets ───────────────────────────────────────────────────────────────
const KEY_COLS_WIND = [
    "name", "bus_number", "bus", "generator_type", "prime_mover_type",
    "rating", "eia_capacity_mw", "ts_column_name",
    "lat", "lon", "plant_name",
    "Plant ID", "Plant Name", "Technology", "Prime Mover Code", "BusName", "kV",
]
const UNMATCHED_COLS_WIND = [
    "name", "bus_number", "bus", "generator_type", "prime_mover_type",
    "rating", "ts_column_name", "lat", "lon", "plant_name",
]
const MMWG_COLS_WIND = [
    "name", "bus_number", "bus", "rating", "ts_column_name",
    "Load Flow  Bus Name", "English Name", "EIA Plant Code", "Bus kV",
]

# ── Manual overrides: Plant ID => Bus Number ──────────────────────────────────
# Add entries here for wind plants that fuzzy/location matching gets wrong
const WIND_MANUAL_OVERRIDES = Dict{Int,Union{Int,Missing}}(
    # VA
    64550 => 314295,    # Coastal Virginia Offshore Wind (CVOW) -> Birdneck 230 kV
    # MD
    57240 => 237513,    # Roth Rock North Wind Farm, LLC  -> Roth Rock 138 kV
    #64083 => 232122,    # Skipjack Wind Farm 120 MW -> Bethany 138 kW
    #65388 => 232006,    # Skipjack Wind Farm Phase 2 -> Indian River 4 230 kV
    #60211 => 200747,    # Terrapin Hills Wind Farm -> Penn Mar 115 kW
    59147 => 237314,    # Fair Wind -> Kelso Gap collector 34.5 kV

    #MD manual due to not exitence of the assigned bus on on EI system
    58904 => 235449,     # Fourmile Ridge to 138 kV
    67962 => 235449,       # Dans Mountain 138 kV

    # WV manual due to not exitence of the assigned bus on on EI system
    60132 => 314941,        #  New Creek Wind

)
# ── Cancelled wind plants — exclude from ALL wind processing ─────────────────
# Format: (Plant ID, Generator ID, state) — checked against eia_expanded
# Sources: FERC queue, EIA form 860 cancellations
const WIND_CANCELLED = [
    # MD — cancelled offshore/wind projects
    (60211, "1",       "MD"),   # Terrapin Hills Wind Farm (Big Sky Wind)
    (64083, "SJW01",   "MD"),   # Skipjack Wind Farm
    (65388, "SJW02",   "MD"),   # Skipjack Wind Farm Phase 2
]

# ── Helper: remove cancelled plants from any EIA-derived DataFrame ────────────
function remove_cancelled_wind(label::String, df::DataFrame)
    nrow(df) == 0 && return df

    cancelled_set = Set(
        (string(pid), string(gid))
        for (pid, gid, _) in WIND_CANCELLED
    )

    before = nrow(df)

    pid_col = "Plant ID"     in names(df) ? "Plant ID"     : "plant_id"
    gid_col = "Generator ID" in names(df) ? "Generator ID" : "generator_id"

    # ── Guard: if neither ID column exists, nothing to filter ─────────────────
    if !(pid_col in names(df)) || !(gid_col in names(df))
        println("  ℹ️  $label: no Plant/Generator ID columns found — skipping cancellation filter")
        return df
    end

    filtered = filter(df) do row
        pid = row[Symbol(pid_col)]
        gid = row[Symbol(gid_col)]
        # Keep rows with missing IDs (can't match to a cancelled entry)
        (ismissing(pid) || ismissing(gid)) && return true
        !in((string(pid), string(gid)), cancelled_set)
    end

    removed = before - nrow(filtered)

    if removed > 0
        println("  🚫 $label: removed $removed cancelled wind plant(s):")
        # ── Fixed: wrap each condition in parens to avoid & type error ────────
        for (pid, gid, st) in WIND_CANCELLED
            pid_str = string(pid)
            gid_str = string(gid)
            mask = (
                (.!ismissing.(df[!, pid_col])) .&
                (string.(coalesce.(df[!, pid_col], "")) .== pid_str) .&
                (.!ismissing.(df[!, gid_col])) .&
                (string.(coalesce.(df[!, gid_col], "")) .== gid_str)
            )
            any(mask) && println("       Plant $pid / Gen $gid ($st)")
        end
    else
        println("  ✅ $label: no cancelled plants found in this DataFrame")
    end

    return filtered
end

# ── W1. Match EI → EIA2PF (wind) ─────────────────────────────────────────────
function match_ei_to_eia_wind(ei_gens::DataFrame, eia_expanded::DataFrame, state::String)
    ei = copy(ei_gens)
    ei.bus_number = [ismissing(x) ? missing : Int(x) for x in ei.bus_number]

    eia_state = filter(row -> coalesce(row["State"] == state, false) &&
                              coalesce(row["Prime Mover Code"] in WIND_PM_CODES, false),
                       eia_expanded)
    DataFrames.rename!(eia_state, "BusID_int" => "BusID_int_join")

    println("\n$state: EIA Wind rows after expansion: ", nrow(eia_state))
    println("  Breakdown by Prime Mover Code:")
    for pm in WIND_PM_CODES
        n = count(==(pm), skipmissing(eia_state[!, "Prime Mover Code"]))
        println("    $pm: $n")
    end

    # ── Detect what wind label is actually used in this EI export ─────────────
    wind_labels = unique(filter(
        t -> occursin(r"(?i)wind", string(t)),
        skipmissing(ei[!, "generator_type"])
    ))
    println("\n  ℹ️  Wind generator_type labels found in EI export: ", wind_labels)

    result = leftjoin(ei, eia_state,
                      on = "bus_number" => "BusID_int_join",
                      matchmissing = :notequal)

    matched   = filter(row -> !ismissing(row["Plant ID"]), result)
    unmatched = filter(row ->  ismissing(row["Plant ID"]), result)

    # ── Count wind gens using whatever label exists ────────────────────────────
    is_wind(t) = occursin(r"(?i)wind", string(coalesce(t, "")))

    n_wind_total     = count(is_wind, ei[!, "generator_type"])
    n_wind_matched   = count(is_wind, matched[!, "generator_type"])
    n_wind_unmatched = count(is_wind, unmatched[!, "generator_type"])

    println("\n", "="^60)
    println("$state EI Generators — EIA2PF Wind Bus Match Summary")
    println("="^60)
    println("  Total $state EI generators:    ", nrow(ei))
    println("  ✅ Matched to EIA2PF bus:      ", nrow(matched))
    println("  ⚠️  No EIA2PF bus match:        ", nrow(unmatched))
    println("─"^60)
    println("  Wind generators in $state EI:")
    println("    Total:        ", n_wind_total)
    println("    ✅ Matched:   ", n_wind_matched)
    println("    ⚠️  Unmatched: ", n_wind_unmatched)
    println("="^60)

    return result
end

# ── W2. Summarize wind matched/unmatched ──────────────────────────────────────
function summarize_wind(label::String, result::DataFrame)
    is_wind(t) = occursin(r"(?i)wind", string(coalesce(t, "")))

    matched   = filter(row -> !ismissing(row["Plant ID"]), result)
    unmatched = filter(row ->  ismissing(row["Plant ID"]) &&
                               is_wind(row["generator_type"]), result)

    # ── Diagnose: non-wind EI gens that matched an EIA Wind entry ────────────
    non_wind_matched = filter(row -> !is_wind(row["generator_type"]), matched)
    if nrow(non_wind_matched) > 0
        println("\n⚠️  $label — Non-wind EI gens matched to EIA Wind (misclassification candidates):")
        show(DataFrames.select(non_wind_matched,
             intersect(["eia_lat", "eia_lon", "Utility Name", "Plant ID", "Plant Name",
                        "name", "bus_number", "bus", "generator_type",
                        "prime_mover_type", "rating", "eia_capacity_mw",
                        "Technology", "Prime Mover Code"], names(non_wind_matched))),
             allrows=true)
    end

    println("\n", "="^60)
    println("$label — Wind Matched vs Unmatched")
    println("="^60)
    println("  ✅ Matched (any EI type → EIA Wind):  ", nrow(matched))
    println("  ⚠️  Unmatched Wind EI gens:            ", nrow(unmatched))
    println("─"^60)
    println("  Matched by EI generator_type (misclassification check):")
    for gt in sort(unique(skipmissing(matched.generator_type)))
        n = count(==(gt), skipmissing(matched.generator_type))
        println("    $gt: $n")
    end
    println("="^60)

    println("\n📋 $label matched generators (", nrow(matched), " rows):")
    show(sort(DataFrames.select(matched, KEY_COLS_WIND), "bus_number"), allrows=true)

    println("\n⚠️  $label unmatched Wind EI generators (", nrow(unmatched), " rows):")
    nrow(unmatched) > 0 ?
        show(sort(DataFrames.select(unmatched, UNMATCHED_COLS_WIND), "bus_number"), allrows=true) :
        println("  (none)")

    return matched, unmatched
end

# ── W3. MMWG fallback for unmatched EI wind ──────────────────────────────────
function mmwg_lookup_wind(label::String, unmatched::DataFrame, mmwg_slim::DataFrame)
    result = leftjoin(unmatched, mmwg_slim,
                      on = "bus_number" => "Bus Number",
                      matchmissing = :notequal)

    mmwg_matched   = filter(row -> !ismissing(row["EIA Plant Code"]), result)
    mmwg_unmatched = filter(row ->  ismissing(row["EIA Plant Code"]), result)

    println("\n", "="^60)
    println("$label — Unmatched Wind → MMWG Fallback")
    println("="^60)
    println("  ✅ Found in MMWG:    ", nrow(mmwg_matched))
    println("  ⚠️  Still unmatched: ", nrow(mmwg_unmatched))
    println("="^60)

    println("\n📋 $label found in MMWG (", nrow(mmwg_matched), " rows):")
    show(sort(DataFrames.select(mmwg_matched, MMWG_COLS_WIND), "bus_number"), allrows=true)

    println("\n⚠️  $label still unmatched after MMWG (", nrow(mmwg_unmatched), " rows):")
    show(sort(DataFrames.select(mmwg_unmatched, UNMATCHED_COLS_WIND), "bus_number"), allrows=true)

    return mmwg_matched, mmwg_unmatched
end

# ── Deduplicate wind matched: sum eia_capacity_mw for multi-EIA matches ───────
function dedup_wind_matched(label::String, df::DataFrame)
    dupes = combine(groupby(df, "name"), nrow => :n)
    dupe_names = dupes[dupes.n .> 1, "name"]

    if isempty(dupe_names)
        println("✅ $label — no duplicate generators")
        return df
    end

    println("⚠️  $label — deduplicating $(length(dupe_names)) generator(s):")
    result = DataFrame()

    for gen in dupe_names
        idx = findall(r -> df[r, "name"] == gen, 1:nrow(df))
        ei_rating = df[first(idx), "rating"]
        
        # Sum all EIA capacities for this generator
        eia_caps = [coalesce(tryparse(Float64, string(df[i, "eia_capacity_mw"])), 0.0) 
                    for i in idx]
        total_eia = sum(eia_caps)
        
        # Keep first row, update eia_capacity_mw with sum
        keep_row = copy(DataFrame(df[first(idx), :]))
        keep_row[!, "eia_capacity_mw"] .= total_eia
        
        println("  $gen | EI: $ei_rating MW | EIA entries: $(join(eia_caps, " + ")) = $total_eia MW")
        
        append!(result, keep_row, promote=true)
    end

    # Keep non-duplicates unchanged
    non_dupe_idx = [!(df[i, "name"] in dupe_names) for i in 1:nrow(df)]
    append!(result, df[non_dupe_idx, :], promote=true)

    println("  Rows before: $(nrow(df)) | after: $(nrow(result))")
    return result
end

# ── W4. Run EI → EIA match ───────────────────────────────────────────────────
va_ei_wind_result = match_ei_to_eia_wind(VA_ei_gens, eia_expanded, "VA")
md_ei_wind_result = match_ei_to_eia_wind(MD_ei_gens, eia_expanded, "MD")
wv_ei_wind_result = match_ei_to_eia_wind(WV_ei_gens, eia_expanded, "WV")

va_wind_matched, va_wind_unmatched = summarize_wind("VA", va_ei_wind_result)
md_wind_matched, md_wind_unmatched = summarize_wind("MD", md_ei_wind_result)
wv_wind_matched, wv_wind_unmatched = summarize_wind("WV", wv_ei_wind_result)

va_wind_matched = dedup_wind_matched("VA", va_wind_matched)
md_wind_matched = dedup_wind_matched("MD", md_wind_matched)
wv_wind_matched = dedup_wind_matched("WV", wv_wind_matched)

va_wind_mmwg_matched, va_wind_still_unmatched = mmwg_lookup_wind("VA", va_wind_unmatched, mmwg_slim)
md_wind_mmwg_matched, md_wind_still_unmatched = mmwg_lookup_wind("MD", md_wind_unmatched, mmwg_slim)
wv_wind_mmwg_matched, wv_wind_still_unmatched = mmwg_lookup_wind("WV", wv_wind_unmatched, mmwg_slim)

println("\n", "="^60)
println("📊 Final Wind EI Summary by State")
println("="^60)
for (label, matched, mmwg, unmatched) in [
    ("VA", va_wind_matched, va_wind_mmwg_matched, va_wind_still_unmatched),
    ("MD", md_wind_matched, md_wind_mmwg_matched, md_wind_still_unmatched),
    ("WV", wv_wind_matched, wv_wind_mmwg_matched, wv_wind_still_unmatched),
]
    println("\n  $label:")
    println("    ✅ EIA2PF matched:   ", nrow(matched))
    println("    ✅ MMWG matched:     ", nrow(mmwg))
    println("    ⚠️  Still unmatched: ", nrow(unmatched))
end
println("="^60)





# ── Export original EI wind generators (already in model) ────────────────────

const EI_WIND_REPORT_COLS = [
    "name", "bus_number", "bus", "generator_type", "prime_mover_type",
    "rating", "lat", "lon", "plant_name",
    "Plant ID", "Plant Name", "Technology", "Prime Mover Code",
    "BusName", "kV", "eia_capacity_mw",
]

function write_ei_wind_report(label::String, matched::DataFrame,
    unmatched::DataFrame, out_dir::String)

    matched_out = DataFrames.select(matched,
    intersect(EI_WIND_REPORT_COLS, names(matched)))
    matched_out[!, "state"]  .= label
    matched_out[!, "status"] .= "matched"

    # ── Use regex so "Wind" and "WindGenerator" both work ─────────────────────
    unmatched_wind = filter(
    r -> occursin(r"(?i)wind", string(coalesce(r["generator_type"], ""))),
    unmatched)
    unmatched_out  = DataFrames.select(unmatched_wind,
        intersect(EI_WIND_REPORT_COLS, names(unmatched_wind)))
    unmatched_out[!, "state"]  .= label
    unmatched_out[!, "status"] .= "unmatched"

    # ── Combined into one CSV per state ──────────────────────────────────────
    combined = vcat(matched_out, unmatched_out, cols=:union)
    sort!(combined, :bus_number)

    path = joinpath(out_dir, "original_wind_$(label).csv")
    CSV.write(path, combined)

    # ── Summary ───────────────────────────────────────────────────────────────
    matched_mw   = round(sum(Float64.(coalesce.(matched_out.rating,   0.0))), digits=1)
    unmatched_mw = round(sum(Float64.(coalesce.(unmatched_out.rating, 0.0))), digits=1)

    println("\n✅ $label — EI wind report written: $path")
    println("   Matched:   ", nrow(matched_out),   " generators | $(matched_mw) MW")
    println("   Unmatched: ", nrow(unmatched_out), " generators | $(unmatched_mw) MW")
    println("   Total:     ", nrow(combined),      " generators | $(matched_mw + unmatched_mw) MW")

    # ── Breakdown by prime mover ──────────────────────────────────────────────
    if "Prime Mover Code" in names(combined)
        println("   Breakdown by Prime Mover Code:")
        for pm in WIND_PM_CODES
            n = count(==(pm), skipmissing(combined[!, "Prime Mover Code"]))
            n == 0 && continue
            println("     $pm: $n generators")
        end
    end

    return combined
end

# ── Run for each state ────────────────────────────────────────────────────────
#ei_wind_VA = write_ei_wind_report("VA", va_wind_matched, va_wind_unmatched, OUTPUT_DIR)
#ei_wind_MD = write_ei_wind_report("MD", md_wind_matched, md_wind_unmatched, OUTPUT_DIR)
#ei_wind_WV = write_ei_wind_report("WV", wv_wind_matched, wv_wind_unmatched, OUTPUT_DIR)

# ── W5. Find EIA wind plants NOT in EI ───────────────────────────────────────
# Uses Plant ID + Generator ID uid (same approach as solar) — avoids false
# positives when multiple generators share a bus number.
function find_eia_wind_not_in_ei(label::String, ei_wind_result::DataFrame,
                                  eia_expanded::DataFrame, state::String)

    # Build uid set from matched EI result
    matched_uids = Set{String}()
    for row in eachrow(ei_wind_result)
        ismissing(row["Plant ID"]) && continue
        ismissing(row["Generator ID"]) && continue
        push!(matched_uids, string(row["Plant ID"]) * "_" * string(row["Generator ID"]))
    end

    eia_state_wind = filter(row -> coalesce(row["State"] == state, false) &&
                                   coalesce(row["Prime Mover Code"] in WIND_PM_CODES, false),
                            eia_expanded)

    # Tag each EIA row with its uid
    eia_state_wind[!, "uid"] = [
        (ismissing(r["Plant ID"]) || ismissing(r["Generator ID"])) ? missing :
        string(r["Plant ID"]) * "_" * string(r["Generator ID"])
        for r in eachrow(eia_state_wind)
    ]

    eia_unmatched = filter(row -> ismissing(row["uid"]) ||
                                  !in(row["uid"], matched_uids), eia_state_wind)

    has_both    = filter(row -> !ismissing(row["BusID_int"]) &&
                                !ismissing(row["BusName"]) &&
                                row["BusName"] != "", eia_unmatched)
    has_neither = filter(row ->  ismissing(row["BusID_int"]) &&
                                (ismissing(row["BusName"]) || row["BusName"] == ""),
                         eia_unmatched)

    println("\n", "="^60)
    println("$label — EIA Wind Plants NOT in EI model")
    println("="^60)
    println("  Total EIA Wind entries for $label:    ", nrow(eia_state_wind))
    println("  ⚠️  Not matched to any EI generator:  ", nrow(eia_unmatched))
    println("─"^60)
    println("    ✅ Has BusID + BusName:  ", nrow(has_both))
    println("    ❌ Has neither:          ", nrow(has_neither))
    println("="^60)

    EIA_COLS = ["Plant ID", "Plant Name", "Generator ID",
                "BusID_int", "BusName", "kV",
                "eia_capacity_mw", "Technology", "Prime Mover Code",
                "eia_lat", "eia_lon"]

    println("\n  ✅ Has BusID + BusName (", nrow(has_both), " rows):")
    nrow(has_both) > 0 && show(DataFrames.select(has_both, EIA_COLS), allrows=true)

    println("\n  ❌ Has neither (", nrow(has_neither), " rows):")
    nrow(has_neither) > 0 && show(DataFrames.select(has_neither, EIA_COLS), allrows=true)

    return eia_unmatched, has_both, has_neither
end

va_wind_eia_unmatched, va_wind_eia_has_both, va_wind_eia_neither =
    find_eia_wind_not_in_ei("VA", va_ei_wind_result, eia_expanded, "VA")
md_wind_eia_unmatched, md_wind_eia_has_both, md_wind_eia_neither =
    find_eia_wind_not_in_ei("MD", md_ei_wind_result, eia_expanded, "MD")
wv_wind_eia_unmatched, wv_wind_eia_has_both, wv_wind_eia_neither =
    find_eia_wind_not_in_ei("WV", wv_ei_wind_result, eia_expanded, "WV")


    # ── Strip cancelled plants BEFORE EIA-not-in-EI search ───────────────────────
va_wind_eia_unmatched = remove_cancelled_wind("VA eia_unmatched", va_wind_eia_unmatched)
md_wind_eia_unmatched = remove_cancelled_wind("MD eia_unmatched", md_wind_eia_unmatched)
wv_wind_eia_unmatched = remove_cancelled_wind("WV eia_unmatched", wv_wind_eia_unmatched)

va_wind_eia_has_both, va_wind_eia_neither =
    begin
        has  = remove_cancelled_wind("VA has_both",  va_wind_eia_has_both)
        neit = remove_cancelled_wind("VA neither",   va_wind_eia_neither)
        has, neit
    end
md_wind_eia_has_both, md_wind_eia_neither =
    begin
        has  = remove_cancelled_wind("MD has_both",  md_wind_eia_has_both)
        neit = remove_cancelled_wind("MD neither",   md_wind_eia_neither)
        has, neit
    end
wv_wind_eia_has_both, wv_wind_eia_neither =
    begin
        has  = remove_cancelled_wind("WV has_both",  wv_wind_eia_has_both)
        neit = remove_cancelled_wind("WV neither",   wv_wind_eia_neither)
        has, neit
    end

# ── W-12b. Patch has_both: override bad EIA buses BEFORE build_eia_only_wind_df
println("\n── Applying WIND_MANUAL_OVERRIDES to wind_eia_has_both (EIA_ONLY source) ──")
va_wind_eia_has_both, _, _ = apply_overrides_to_eia_df("VA", va_wind_eia_has_both, :eia_has_both, mmwg_slim, WIND_MANUAL_OVERRIDES)
md_wind_eia_has_both, _, _ = apply_overrides_to_eia_df("MD", md_wind_eia_has_both, :eia_has_both, mmwg_slim, WIND_MANUAL_OVERRIDES)
wv_wind_eia_has_both, _, _ = apply_overrides_to_eia_df("WV", wv_wind_eia_has_both, :eia_has_both, mmwg_slim, WIND_MANUAL_OVERRIDES)


# ── W6. Exact name match → MMWG ──────────────────────────────────────────────
function match_neither_to_mmwg_wind(label::String, neither::DataFrame, mmwg_slim::DataFrame)
    println("\n", "="^60)
    println("$label — EIA Wind 'neither' plants → MMWG exact name match")
    println("="^60)

    nrow(neither) == 0 && (println("  ℹ️  No 'neither' rows to match."); println("="^60);
                           return DataFrame(), DataFrame())

    region      = get(STATE_REGION, label, missing)
    mmwg_region = !ismissing(region) ?
        filter(r -> coalesce(r["Region/PC"] == region, false), mmwg_slim) : mmwg_slim

    normalize(s) = ismissing(s) ? "" : lowercase(strip(string(s)))

    matches  = DataFrame()
    no_match = DataFrame()

    for row in eachrow(neither)
        plant_name = normalize(row["Plant Name"])
        mmwg_match = filter(r -> normalize(r["English Name"]) == plant_name, mmwg_region)
        if nrow(mmwg_match) > 0
            new_row = copy(DataFrame(row))
            new_row[!, "Bus Number"]          .= mmwg_match[1, "Bus Number"]
            new_row[!, "Load Flow  Bus Name"] .= mmwg_match[1, "Load Flow  Bus Name"]
            new_row[!, "English Name"]        .= mmwg_match[1, "English Name"]
            new_row[!, "EIA Plant Code"]      .= mmwg_match[1, "EIA Plant Code"]
            new_row[!, "Bus kV"]              .= mmwg_match[1, "Bus kV"]
            new_row[!, "Region/PC"]           .= mmwg_match[1, "Region/PC"]
            append!(matches, new_row, promote=true)
        else
            append!(no_match, DataFrame(row), promote=true)
        end
    end

    println("  Total 'neither':      ", nrow(neither))
    println("  ✅ Matched via name:  ", nrow(matches))
    println("  ⚠️  Still no match:   ", nrow(no_match))
    println("="^60)

    return matches, no_match
end

va_wind_neither_mmwg, va_wind_neither_unmatched =
    match_neither_to_mmwg_wind("VA", va_wind_eia_neither, mmwg_slim)
md_wind_neither_mmwg, md_wind_neither_unmatched =
    match_neither_to_mmwg_wind("MD", md_wind_eia_neither, mmwg_slim)
wv_wind_neither_mmwg, wv_wind_neither_unmatched =
    match_neither_to_mmwg_wind("WV", wv_wind_eia_neither, mmwg_slim)

# ── W-13b. Patch neither_mmwg_matched: override bad MMWG buses BEFORE fuzzy ──
println("\n── Applying WIND_MANUAL_OVERRIDES to wind_neither_mmwg_matched (EIA_MMWG_EXACT source) ──")
va_wind_neither_mmwg, _, _ = apply_overrides_to_eia_df("VA", va_wind_neither_mmwg, :neither_mmwg, mmwg_slim, WIND_MANUAL_OVERRIDES)
md_wind_neither_mmwg, _, _ = apply_overrides_to_eia_df("MD", md_wind_neither_mmwg, :neither_mmwg, mmwg_slim, WIND_MANUAL_OVERRIDES)
wv_wind_neither_mmwg, _, _ = apply_overrides_to_eia_df("WV", wv_wind_neither_mmwg, :neither_mmwg, mmwg_slim, WIND_MANUAL_OVERRIDES)

# ── W7. Fuzzy match → MMWG ───────────────────────────────────────────────────
function fuzzy_match_to_mmwg_wind(label::String, still_unmatched::DataFrame,
                                   mmwg_slim::DataFrame; threshold::Float64 = 0.7)
    println("\n", "="^60)
    println("$label — Fuzzy Wind match → MMWG (threshold = $threshold)")
    println("="^60)

    nrow(still_unmatched) == 0 && (println("  ℹ️  Nothing to fuzzy match.");
                                   println("="^60);
                                   return DataFrame(), DataFrame())

    region      = get(STATE_REGION, label, missing)
    mmwg_region = !ismissing(region) ?
        filter(r -> coalesce(r["Region/PC"] == region, false), mmwg_slim) : mmwg_slim

    normalize(s) = ismissing(s) ? "" : lowercase(strip(string(s)))
    mmwg_names    = [normalize(r["English Name"]) for r in eachrow(mmwg_region)]
    mmwg_bus_nums = mmwg_region[!, "Bus Number"]
    mmwg_bus_kv   = mmwg_region[!, "Bus kV"]
    mmwg_lf_names = mmwg_region[!, "Load Flow  Bus Name"]
    mmwg_eia_code = mmwg_region[!, "EIA Plant Code"]

    fuzzy_matched   = DataFrame()
    fuzzy_unmatched = DataFrame()

    for row in eachrow(still_unmatched)
        plant_name = normalize(row["Plant Name"])
        scores     = [compare(plant_name, mn, Jaro()) for mn in mmwg_names]
        best_idx   = argmax(scores)
        best_score = scores[best_idx]

        new_row = copy(DataFrame(row))
        if best_score >= threshold
            new_row[!, "Bus Number"]          .= mmwg_bus_nums[best_idx]
            new_row[!, "Load Flow  Bus Name"] .= mmwg_lf_names[best_idx]
            new_row[!, "English Name"]        .= mmwg_region[best_idx, "English Name"]
            new_row[!, "EIA Plant Code"]      .= mmwg_eia_code[best_idx]
            new_row[!, "Bus kV"]              .= mmwg_bus_kv[best_idx]
            new_row[!, "match_score"]         .= round(best_score, digits=3)
            new_row[!, "Region/PC"]           .= mmwg_region[best_idx, "Region/PC"]
            append!(fuzzy_matched, new_row, promote=true)
        else
            new_row[!, "best_mmwg_name"] .= mmwg_region[best_idx, "English Name"]
            new_row[!, "match_score"]    .= round(best_score, digits=3)
            append!(fuzzy_unmatched, new_row, promote=true)
        end
    end

    println("  Total still unmatched:          ", nrow(still_unmatched))
    println("  ✅ Fuzzy matched (≥$threshold):  ", nrow(fuzzy_matched))
    println("  ⚠️  No fuzzy match:               ", nrow(fuzzy_unmatched))

    if nrow(fuzzy_matched) > 0
        println("\n  Fuzzy match details:")
        show(DataFrames.select(fuzzy_matched,
            ["Plant Name", "Bus Number", "English Name", "match_score",
             "eia_capacity_mw"]), allrows=true)
    end
    if nrow(fuzzy_unmatched) > 0
        println("\n  ⚠️  No match found (best candidate shown):")
        show(DataFrames.select(fuzzy_unmatched,
            ["Plant Name", "best_mmwg_name", "match_score", "eia_capacity_mw"]),
            allrows=true)
    end
    println("="^60)

    return fuzzy_matched, fuzzy_unmatched
end

va_wind_fuzzy_matched, va_wind_fuzzy_unmatched =
    fuzzy_match_to_mmwg_wind("VA", va_wind_neither_unmatched, mmwg_slim)
md_wind_fuzzy_matched, md_wind_fuzzy_unmatched =
    fuzzy_match_to_mmwg_wind("MD", md_wind_neither_unmatched, mmwg_slim)
wv_wind_fuzzy_matched, wv_wind_fuzzy_unmatched =
    fuzzy_match_to_mmwg_wind("WV", wv_wind_neither_unmatched, mmwg_slim)

# ── W8. Apply manual overrides to fuzzy matched ──────────────────────────────
function apply_wind_manual_overrides!(label::String, fuzzy_matched::DataFrame,
                                      overrides::Dict)
    isempty(overrides) && return fuzzy_matched
    corrected = 0
    for i in 1:nrow(fuzzy_matched)
        pid = coalesce(fuzzy_matched[i, "Plant ID"], -1)
        haskey(overrides, pid) || continue
        new_bus = overrides[pid]
        if !ismissing(new_bus)
            fuzzy_matched[i, "Bus Number"] = new_bus
            println("  ✅ $label wind override: Plant $pid → bus $new_bus")
            corrected += 1
        end
    end
    println("$label wind: $corrected manual overrides applied")
    return fuzzy_matched
end

va_wind_fuzzy_matched = apply_wind_manual_overrides!("VA", va_wind_fuzzy_matched, WIND_MANUAL_OVERRIDES)
md_wind_fuzzy_matched = apply_wind_manual_overrides!("MD", md_wind_fuzzy_matched, WIND_MANUAL_OVERRIDES)
wv_wind_fuzzy_matched = apply_wind_manual_overrides!("WV", wv_wind_fuzzy_matched, WIND_MANUAL_OVERRIDES)

# ── W9. Location-based fallback for fuzzy_unmatched ──────────────────────────
# Reuses auto_assign_by_location() defined in the solar section
va_wind_unmatched_located, va_wind_unmatched_no_coords =
    auto_assign_by_location("VA wind unmatched", va_wind_fuzzy_unmatched,
                            eia_plants_loc, bus_coords, max_cap_mw=Inf)
md_wind_unmatched_located, md_wind_unmatched_no_coords =
    auto_assign_by_location("MD wind unmatched", md_wind_fuzzy_unmatched,
                            eia_plants_loc, bus_coords, max_cap_mw=Inf)
wv_wind_unmatched_located, wv_wind_unmatched_no_coords =
    auto_assign_by_location("WV wind unmatched", wv_wind_fuzzy_unmatched,
                            eia_plants_loc, bus_coords, max_cap_mw=Inf)

# Merge location-assigned back into fuzzy_matched
va_wind_fuzzy_matched = vcat(va_wind_fuzzy_matched, va_wind_unmatched_located, cols=:union)
md_wind_fuzzy_matched = vcat(md_wind_fuzzy_matched, md_wind_unmatched_located, cols=:union)
wv_wind_fuzzy_matched = vcat(wv_wind_fuzzy_matched, wv_wind_unmatched_located, cols=:union)

println("\n", "="^60)
println("📊 Final Wind EIA-not-in-EI Summary")
println("="^60)
for (label, has_both, neither_mmwg, fuzzy, fuzzy_unc, loc, no_coords) in [
    ("VA", va_wind_eia_has_both, va_wind_neither_mmwg,
           va_wind_fuzzy_matched, va_wind_fuzzy_unmatched,
           va_wind_unmatched_located, va_wind_unmatched_no_coords),
    ("MD", md_wind_eia_has_both, md_wind_neither_mmwg,
           md_wind_fuzzy_matched, md_wind_fuzzy_unmatched,
           md_wind_unmatched_located, md_wind_unmatched_no_coords),
    ("WV", wv_wind_eia_has_both, wv_wind_neither_mmwg,
           wv_wind_fuzzy_matched, wv_wind_fuzzy_unmatched,
           wv_wind_unmatched_located, wv_wind_unmatched_no_coords),
]
    total_assigned = nrow(has_both) + nrow(neither_mmwg) + nrow(fuzzy)
    println("\n  $label:")
    println("    EIA_ONLY (has BusID+BusName):  ", nrow(has_both))
    println("    EIA_MMWG_EXACT:                ", nrow(neither_mmwg))
    println("    EIA_MMWG_FUZZY (post-override): ", nrow(fuzzy))
    println("    ─── of which location-assigned: ", nrow(loc))
    println("    ❌ Still no bus (no coords):    ", nrow(no_coords))
    println("    ─────────────────────────────────")
    println("    Total assigned:                ", total_assigned)
end
println("="^60)

# ── W10. Helpers (wind-specific) ─────────────────────────────────────────────
# clean_bus_voltage, clean_bus_name, make_gen_name reused from solar section

# ── W11. Build EI wind DataFrames ─────────────────────────────────────────────
function build_state_wind_df(label::String,
                              wind_matched::DataFrame,
                              mmwg_matched::DataFrame)
    s1 = DataFrame(
        source          = fill("EI_EIA2PF", nrow(wind_matched)),
        gen_name        = wind_matched.name,
        ts_column_name  = wind_matched.ts_column_name,
        ei_lat          = wind_matched.lat,
        ei_lon          = wind_matched.lon,
        eia_lat         = wind_matched[!, "eia_lat"],
        eia_lon         = wind_matched[!, "eia_lon"],
        bus_id          = wind_matched.bus_number,
        bus_name        = wind_matched.bus,
        ei_capacity_mw  = wind_matched.rating,
        eia_capacity_mw = wind_matched.eia_capacity_mw,
        bus_voltage_kv  = [clean_bus_voltage(x) for x in wind_matched.kV],
    )
    s2 = DataFrame(
        source          = fill("EI_MMWG", nrow(mmwg_matched)),
        gen_name        = mmwg_matched.name,
        ts_column_name  = mmwg_matched.ts_column_name,
        ei_lat          = mmwg_matched.lat,
        ei_lon          = mmwg_matched.lon,
        eia_lat         = fill(missing, nrow(mmwg_matched)),
        eia_lon         = fill(missing, nrow(mmwg_matched)),
        bus_id          = mmwg_matched.bus_number,
        bus_name        = mmwg_matched.bus,
        ei_capacity_mw  = mmwg_matched.rating,
        eia_capacity_mw = fill(missing, nrow(mmwg_matched)),
        bus_voltage_kv  = [clean_bus_voltage(x) for x in mmwg_matched[!, "Bus kV"]],
    )
    final_df = vcat(s1, s2, cols=:union)
    final_df[!, "state"] .= label

    println("\n", "="^60)
    println("$label — Final Wind DataFrame (EI generators)")
    println("="^60)
    println("  EI_EIA2PF: ", nrow(s1),
            "  |  EI_MMWG: ", nrow(s2),
            "  |  Total: ", nrow(final_df))
    println("="^60)
    show(sort(final_df, :bus_id), allrows=true)
    return final_df
end

va_wind = build_state_wind_df("VA", va_wind_matched, va_wind_mmwg_matched)
md_wind = build_state_wind_df("MD", md_wind_matched, md_wind_mmwg_matched)
wv_wind = build_state_wind_df("WV", wv_wind_matched, wv_wind_mmwg_matched)

# ── W12. Build EIA-only wind DataFrames ──────────────────────────────────────
function build_eia_only_wind_df(label::String,
                                 eia_has_both::DataFrame,
                                 neither_mmwg_matched::DataFrame,
                                 fuzzy_matched::DataFrame)

    safe_col(df, col) = nrow(df) == 0 || !(col in names(df)) ?
        fill(missing, nrow(df)) : df[!, col]

    # Source 3: EIA has BusID+BusName, not in EI
    s3 = if nrow(eia_has_both) == 0
        println("  ℹ️  $label: no EIA_ONLY wind entries")
        DataFrame(source=String[], gen_name=String[], ts_column_name=Any[],
                  ei_lat=Any[], ei_lon=Any[], eia_lat=Any[], eia_lon=Any[],
                  bus_id=Any[], bus_name=Any[], ei_capacity_mw=Any[],
                  eia_capacity_mw=Any[], bus_voltage_kv=Any[])
    else
        DataFrame(
            source          = fill("EIA_ONLY", nrow(eia_has_both)),
            gen_name        = [make_gen_name(eia_has_both[i, "BusID_int"], i)
                               for i in 1:nrow(eia_has_both)],
            ts_column_name  = fill(missing, nrow(eia_has_both)),
            ei_lat          = fill(missing, nrow(eia_has_both)),
            ei_lon          = fill(missing, nrow(eia_has_both)),
            eia_lat         = safe_col(eia_has_both, "eia_lat"),
            eia_lon         = safe_col(eia_has_both, "eia_lon"),
            bus_id          = safe_col(eia_has_both, "BusID_int"),
            bus_name        = [clean_bus_name(x)
                               for x in safe_col(eia_has_both, "BusName")],
            ei_capacity_mw  = fill(missing, nrow(eia_has_both)),
            eia_capacity_mw = safe_col(eia_has_both, "eia_capacity_mw"),
            bus_voltage_kv  = [clean_bus_voltage(x)
                               for x in safe_col(eia_has_both, "kV")],
        )
    end

    # Source 4: EIA neither → MMWG exact match
    s4 = if nrow(neither_mmwg_matched) == 0
        println("  ℹ️  $label: no EIA_MMWG_EXACT wind entries")
        DataFrame(source=String[], gen_name=String[], ts_column_name=Any[],
                  ei_lat=Any[], ei_lon=Any[], eia_lat=Any[], eia_lon=Any[],
                  bus_id=Any[], bus_name=Any[], ei_capacity_mw=Any[],
                  eia_capacity_mw=Any[], bus_voltage_kv=Any[])
    else
        DataFrame(
            source          = fill("EIA_MMWG_EXACT", nrow(neither_mmwg_matched)),
            gen_name        = [make_gen_name(neither_mmwg_matched[i, "Bus Number"], i)
                               for i in 1:nrow(neither_mmwg_matched)],
            ts_column_name  = fill(missing, nrow(neither_mmwg_matched)),
            ei_lat          = fill(missing, nrow(neither_mmwg_matched)),
            ei_lon          = fill(missing, nrow(neither_mmwg_matched)),
            eia_lat         = safe_col(neither_mmwg_matched, "eia_lat"),
            eia_lon         = safe_col(neither_mmwg_matched, "eia_lon"),
            bus_id          = safe_col(neither_mmwg_matched, "Bus Number"),
            bus_name        = safe_col(neither_mmwg_matched, "Load Flow  Bus Name"),
            ei_capacity_mw  = fill(missing, nrow(neither_mmwg_matched)),
            eia_capacity_mw = safe_col(neither_mmwg_matched, "eia_capacity_mw"),
            bus_voltage_kv  = [clean_bus_voltage(x)
                               for x in safe_col(neither_mmwg_matched, "Bus kV")],
        )
    end

    # Source 5: EIA neither → MMWG fuzzy match (includes location-assigned)
    s5 = if nrow(fuzzy_matched) == 0
        println("  ℹ️  $label: no EIA_MMWG_FUZZY wind entries")
        DataFrame(source=String[], gen_name=String[], ts_column_name=Any[],
                  ei_lat=Any[], ei_lon=Any[], eia_lat=Any[], eia_lon=Any[],
                  bus_id=Any[], bus_name=Any[], ei_capacity_mw=Any[],
                  eia_capacity_mw=Any[], bus_voltage_kv=Any[])
    else
        DataFrame(
            source          = fill("EIA_MMWG_FUZZY", nrow(fuzzy_matched)),
            gen_name        = [make_gen_name(fuzzy_matched[i, "Bus Number"], i)
                               for i in 1:nrow(fuzzy_matched)],
            ts_column_name  = fill(missing, nrow(fuzzy_matched)),
            ei_lat          = fill(missing, nrow(fuzzy_matched)),
            ei_lon          = fill(missing, nrow(fuzzy_matched)),
            eia_lat         = safe_col(fuzzy_matched, "eia_lat"),
            eia_lon         = safe_col(fuzzy_matched, "eia_lon"),
            bus_id          = safe_col(fuzzy_matched, "Bus Number"),
            bus_name        = safe_col(fuzzy_matched, "English Name"),
            ei_capacity_mw  = fill(missing, nrow(fuzzy_matched)),
            eia_capacity_mw = safe_col(fuzzy_matched, "eia_capacity_mw"),
            bus_voltage_kv  = [clean_bus_voltage(x)
                               for x in safe_col(fuzzy_matched, "Bus kV")],
        )
    end

    final_df = vcat(s3, s4, s5, cols=:union)
    final_df[!, "state"] .= label

    if nrow(final_df) > 0
        source_order = Dict("EIA_ONLY" => 1, "EIA_MMWG_EXACT" => 2, "EIA_MMWG_FUZZY" => 3)
        final_df[!, "source_order"] = [source_order[s] for s in final_df.source]
        sort!(final_df, [:source_order, :bus_id])
        select!(final_df, Not(:source_order))
    end

    println("\n", "="^60)
    println("$label — EIA-only Wind DataFrame")
    println("="^60)
    println("  EIA_ONLY: $(nrow(s3))  |  EIA_MMWG_EXACT: $(nrow(s4))  |",
            "  EIA_MMWG_FUZZY: $(nrow(s5))  |  Total: $(nrow(final_df))")
    println("="^60)
    nrow(final_df) > 0 && show(final_df, allrows=true)

    return final_df
end

va_wind_eia_only = build_eia_only_wind_df("VA", va_wind_eia_has_both,
    va_wind_neither_mmwg, va_wind_fuzzy_matched)
md_wind_eia_only = build_eia_only_wind_df("MD", md_wind_eia_has_both,
    md_wind_neither_mmwg, md_wind_fuzzy_matched)
wv_wind_eia_only = build_eia_only_wind_df("WV", wv_wind_eia_has_both,
    wv_wind_neither_mmwg, wv_wind_fuzzy_matched)

# ── W13. Build export DataFrames ──────────────────────────────────────────────
function build_wind_export_df(ei_df::DataFrame, eia_only_df::DataFrame, label::String)
    ei_out = if nrow(ei_df) == 0
        DataFrame(gen_name=String[], bus_id=Int[], bus_name=String[],
                  lat=Float64[], lon=Float64[], capacity_mw=Any[],
                  bus_voltage_kv=Float64[], source=String[], state=String[])
    else
        DataFrame(
            gen_name       = ei_df[!, "gen_name"],
            bus_id         = ei_df[!, "bus_id"],
            bus_name       = ei_df[!, "bus_name"],
            lat            = coalesce.(ei_df[!, "eia_lat"], ei_df[!, "ei_lat"]),
            lon            = coalesce.(ei_df[!, "eia_lon"], ei_df[!, "ei_lon"]),
            capacity_mw    = coalesce.(ei_df[!, "eia_capacity_mw"],
                                       ei_df[!, "ei_capacity_mw"]),
            bus_voltage_kv = ei_df[!, "bus_voltage_kv"],
            source         = ei_df[!, "source"],
            state          = fill(label, nrow(ei_df)),
        )
    end

    eia_out = if nrow(eia_only_df) == 0
        DataFrame(gen_name=String[], bus_id=Any[], bus_name=Any[],
                  lat=Any[], lon=Any[], capacity_mw=Any[],
                  bus_voltage_kv=Any[], source=String[], state=String[])
    else
        DataFrame(
            gen_name       = eia_only_df[!, "gen_name"],
            bus_id         = eia_only_df[!, "bus_id"],
            bus_name       = eia_only_df[!, "bus_name"],
            lat            = eia_only_df[!, "eia_lat"],
            lon            = eia_only_df[!, "eia_lon"],
            capacity_mw    = eia_only_df[!, "eia_capacity_mw"],
            bus_voltage_kv = eia_only_df[!, "bus_voltage_kv"],
            source         = eia_only_df[!, "source"],
            state          = fill(label, nrow(eia_only_df)),
        )
    end

    combined = sort(vcat(ei_out, eia_out, cols=:union), :bus_id)

    println("\n", "="^60)
    println("$label — Wind Export Summary")
    println("="^60)
    println("  EI generators: $(nrow(ei_out))  |  EIA-only: $(nrow(eia_out))",
            "  |  Total: $(nrow(combined))")
    for src in sort(unique(skipmissing(combined.source)))
        n   = count(==(src), skipmissing(combined.source))
        mw  = round(sum(skipmissing(combined[combined.source .== src, "capacity_mw"])),
                    digits=1)
        println("    $src: $n gens | $(mw) MW")
    end
    println("="^60)
    return combined
end

va_wind_export = build_wind_export_df(va_wind, va_wind_eia_only, "VA")
md_wind_export = build_wind_export_df(md_wind, md_wind_eia_only, "MD")
wv_wind_export = build_wind_export_df(wv_wind, wv_wind_eia_only, "WV")


# ── Check which buses in solar exports don't exist in EI ──────────────────────
ei_valid_buses = Set{Int}(bus_coords[!, :bus_number])

println("\n", "█"^70)
println("  WIND EXPORT — INVALID BUS VALIDATION")
println("█"^70)

for (state_label, df, state_buses) in [
    ("VA", va_wind_export, va_buses),
    ("MD", md_wind_export, md_buses),
    ("WV", wv_wind_export, wv_buses),
]
    bus_col = "bus_id" in names(df) ? "bus_id" : "bus_number"

    valid       = filter(r -> !ismissing(r[bus_col]) &&
                               Int(r[bus_col]) in ei_valid_buses, df)
    invalid     = filter(r -> !ismissing(r[bus_col]) &&
                              !(Int(r[bus_col]) in ei_valid_buses), df)
    missing_bus = filter(r ->  ismissing(r[bus_col]), df)

    state_valid_buses = Set{Int}(state_buses[!, :bus_number])
    wrong_state = filter(r -> !ismissing(r[bus_col]) &&
                               Int(r[bus_col]) in ei_valid_buses &&
                              !(Int(r[bus_col]) in state_valid_buses), df)

    println("\n── $state_label wind export ($(nrow(df)) total) ───────────────────")
    println("  ✅ Valid EI bus:                    ", nrow(valid))
    println("  ✅   of which in $state_label state buses:  ",
            nrow(valid) - nrow(wrong_state))
    println("  ⚠️   of which in OTHER state buses:  ", nrow(wrong_state))
    println("  ❌ Bus NOT in EI at all:             ", nrow(invalid))
    println("  ❓ Missing bus_id:                   ", nrow(missing_bus))

    DISPLAY_COLS = ["gen_name", "bus_id", "bus_name",
                    "Utility ID", "source", "capacity_mw",
                    "bus_voltage_kv", "lat", "lon"]

    if nrow(invalid) > 0
        println("\n  ❌ Generators pointing to non-existent EI bus:")
        show(DataFrames.select(invalid,
            intersect(DISPLAY_COLS, names(invalid))), allrows=true)
    end

    if nrow(wrong_state) > 0
        println("\n  ⚠️  Generators on a valid EI bus but outside $state_label:")
        show(DataFrames.select(wrong_state,
            intersect(DISPLAY_COLS, names(wrong_state))), allrows=true)
    end

    if nrow(missing_bus) > 0
        println("\n  ❓ Generators with no bus assigned:")
        show(DataFrames.select(missing_bus,
            intersect(DISPLAY_COLS, names(missing_bus))), allrows=true)
    end
end
println("█"^70)

# ── W14. Wind overlap audit ───────────────────────────────────────────────────
va_wind_dups = audit_plant_overlap("VA", va_wind_matched, va_wind_mmwg_matched,
    va_wind_eia_has_both, va_wind_neither_mmwg, va_wind_fuzzy_matched, tech_label="Wind")
md_wind_dups = audit_plant_overlap("MD", md_wind_matched, md_wind_mmwg_matched,
    md_wind_eia_has_both, md_wind_neither_mmwg, md_wind_fuzzy_matched, tech_label="Wind")
wv_wind_dups = audit_plant_overlap("WV", wv_wind_matched, wv_wind_mmwg_matched,
    wv_wind_eia_has_both, wv_wind_neither_mmwg, wv_wind_fuzzy_matched, tech_label="Wind")

# ── W15. Write CSVs ───────────────────────────────────────────────────────────
CSV.write(joinpath(OUTPUT_DIR, "wind_RE_VA_EI_buses.csv"), va_wind_export)
CSV.write(joinpath(OUTPUT_DIR, "wind_RE_MD_EI_buses.csv"), md_wind_export)
CSV.write(joinpath(OUTPUT_DIR, "wind_RE_WV_EI_buses.csv"), wv_wind_export)

println("\n✅ Wind CSVs written:")
println("  → wind_RE_VA_EI_buses.csv (", nrow(va_wind_export), " rows)")
println("  → wind_RE_MD_EI_buses.csv (", nrow(md_wind_export), " rows)")
println("  → wind_RE_W_EI_busesV.csv (", nrow(wv_wind_export), " rows)")

# ── W16. Wind EI Update Report & Diagnosis ───────────────────────────────────

# ── Combine all state updates ─────────────────────────────────────────────────
all_wind_updates = vcat(va_wind_export, md_wind_export, wv_wind_export, cols=:union)

# ── Source colors (shared with solar section, redefined here for safety) ──────
const WIND_SOURCE_COLORS = Dict(
    "EI_EIA2PF"      => "#2ecc71",
    "EI_MMWG"        => "#27ae60",
    "EIA_ONLY"       => "#e74c3c",
    "EIA_MMWG_EXACT" => "#e67e22",
    "EIA_MMWG_FUZZY" => "#f39c12",
)


# ── Combine all state updates ─────────────────────────────────────────────────
all_wind_updates = vcat(va_wind_export, md_wind_export, wv_wind_export, cols=:union)

# ── EIA2PF reference totals ───────────────────────────────────────────────────
eia_wind_ref = filter(row -> coalesce(row["Prime Mover Code"] in WIND_PM_CODES, false) &&
                              coalesce(row["State"] in ["VA", "MD", "WV"], false),
                      eia_2_pf_mapping)

eia_wind_ref_by_state = combine(groupby(eia_wind_ref, "State"),
    "Nameplate Capacity (MW)" => (x -> sum(skipmissing(x))) => "eia2pf_total_mw",
    nrow => "eia2pf_n_generators",
)

# ── Update totals by state ────────────────────────────────────────────────────
update_by_state = combine(groupby(all_wind_updates, "state"),
    "capacity_mw" => (x -> sum(skipmissing(x))) => "update_total_mw",
    nrow => "update_n_generators",
)
DataFrames.rename!(update_by_state, "state" => "State")

# ── Update totals by source ───────────────────────────────────────────────────
update_by_source = sort(
    combine(groupby(all_wind_updates, ["state", "source"]),
        "capacity_mw" => (x -> sum(skipmissing(x))) => "total_mw",
        nrow => "n_generators",
    ), ["state", "source"])

# ── Join for comparison ───────────────────────────────────────────────────────
wind_comparison = leftjoin(eia_wind_ref_by_state, update_by_state, on = "State")
wind_comparison[!, "diff_mw"] = wind_comparison[!, "update_total_mw"] .-
                                  wind_comparison[!, "eia2pf_total_mw"]
wind_comparison[!, "pct_captured"] = round.(
    wind_comparison[!, "update_total_mw"] ./
    wind_comparison[!, "eia2pf_total_mw"] .* 100, digits = 1)

# ── Source priority legend ────────────────────────────────────────────────────
const SOURCE_LABELS = Dict(
    "EI_EIA2PF"      => "EI gen already in EI model, matched to EIA2PF bus",
    "EI_MMWG"        => "EI gen already in EI model, matched via MMWG bus",
    "EIA_ONLY"       => "New gen: in EIA2PF with BusID+BusName, NOT in EI",
    "EIA_MMWG_EXACT" => "New gen: EIA has no bus → found via MMWG exact name",
    "EIA_MMWG_FUZZY" => "New gen: EIA has no bus → found via MMWG fuzzy name",
)

# ── Cancelled plants log ──────────────────────────────────────────────────────
cancelled_log = [
    (pid, gid, st, name) for (pid, gid, st, name) in [
        (60211, "1",     "MD", "Terrapin Hills Wind Farm"),
        (64083, "SJW01", "MD", "Skipjack Wind Farm"),
        (65388, "SJW02", "MD", "Skipjack Wind Farm Phase 2"),
    ]
]

# ─────────────────────────────────────────────────────────────────────────────
println("\n", "█"^70)
println("  WIND GENERATORS — EI UPDATE REPORT")
println("  States: VA | MD | WV       Tech: Wind (WT = onshore, WS = offshore)")
println("█"^70)

# ── Section 1: Source methodology ────────────────────────────────────────────
println("\n📖 SOURCE METHODOLOGY")
println("─"^70)
println("  Each wind generator update is assigned one of 5 sources,")
println("  in priority order (highest → lowest confidence):\n")
for (i, src) in enumerate(["EI_EIA2PF", "EI_MMWG", "EIA_ONLY",
                            "EIA_MMWG_EXACT", "EIA_MMWG_FUZZY"])
    println("  $i. $src")
    println("     └─ $(SOURCE_LABELS[src])")
end

# ── Section 2: Cancelled plants ──────────────────────────────────────────────
println("\n\n🚫 CANCELLED WIND PLANTS (excluded from all updates)")
println("─"^70)
for (pid, gid, st, name) in cancelled_log
    println("  [$st] Plant $pid / Gen $gid — $name")
end
println("  Source: FERC interconnection queue + EIA Form 860 cancellations")

# ── Section 3: State-level capacity comparison ───────────────────────────────
println("\n\n📊 CAPACITY COMPARISON — EI UPDATE vs EIA2PF REFERENCE")
println("─"^70)
println("  EIA2PF = reference total from EIA Form 860 plant-to-bus mapping")
println("  EI Update = generators included in this update\n")

show(wind_comparison, allrows=true)

println("\n\n  ⚠️  Notes on gaps (diff_mw < 0 or pct_captured < 100):")
for row in eachrow(wind_comparison)
    pct = coalesce(row.pct_captured, 0.0)
    diff = coalesce(row.diff_mw, 0.0)
    if pct < 95.0
        println("  • $(row.State): capturing $(pct)% — missing ~$(abs(round(diff, digits=1))) MW")
        println("    Likely causes: cancelled projects excluded, EI model pre-dates EIA entry,")
        println("    or plant has no bus assignment in EIA2PF or MMWG.")
    elseif pct > 105.0
        println("  • $(row.State): capturing $(pct)% — OVER EIA2PF by $(round(diff, digits=1)) MW")
        println("    Possible cause: EI model includes generators not yet in EIA2PF (proposed/new).")
    else
        println("  • $(row.State): ✅ $(pct)% captured — within ±5% of EIA2PF reference")
    end
end

# ── Section 4: Breakdown by source ───────────────────────────────────────────
println("\n\n📋 BREAKDOWN BY MATCH SOURCE")
println("─"^70)
show(update_by_source, allrows=true)

println("\n\n  Source mix interpretation:")
for st in ["VA", "MD", "WV"]
    st_rows = filter(r -> r.state == st, update_by_source)
    nrow(st_rows) == 0 && continue
    total_mw = sum(skipmissing(st_rows.total_mw))
    ei_mw    = sum(skipmissing(
        filter(r -> startswith(r.source, "EI_"), st_rows).total_mw))
    new_mw   = sum(skipmissing(
        filter(r -> startswith(r.source, "EIA_"), st_rows).total_mw))
    println("  $st:  EI existing = $(round(ei_mw, digits=1)) MW",
            " | New to add = $(round(new_mw, digits=1)) MW",
            " | Total = $(round(total_mw, digits=1)) MW")
end

# ── Section 5: Data quality flags ────────────────────────────────────────────
println("\n\n🔍 DATA QUALITY FLAGS")
println("─"^70)

for (label, df) in [("VA", va_wind_export),
                    ("MD", md_wind_export),
                    ("WV", wv_wind_export)]
    no_bus  = filter(r -> ismissing(r.bus_id),      df)
    no_cap  = filter(r -> ismissing(r.capacity_mw), df)
    no_loc  = filter(r -> ismissing(r.lat) || ismissing(r.lon), df)
    fuzzy   = filter(r -> coalesce(r.source, "") == "EIA_MMWG_FUZZY", df)

    println("\n  $label ($(nrow(df)) total generators):")
    println("    ❌ Missing bus_id:       ", nrow(no_bus),
            nrow(no_bus)  > 0 ? "  ← cannot be added to EI model" : "  ✅")
    println("    ⚠️  Missing capacity_mw: ", nrow(no_cap),
            nrow(no_cap)  > 0 ? "  ← will need manual capacity lookup" : "  ✅")
    println("    📍 Missing lat/lon:      ", nrow(no_loc),
            nrow(no_loc)  > 0 ? "  ← time series assignment may fail" : "  ✅")
    println("    🔶 Fuzzy-matched:        ", nrow(fuzzy),
            nrow(fuzzy)   > 0 ? "  ← verify bus assignments manually" : "  ✅")

    if nrow(no_bus) > 0
        println("\n    ❌ $label generators with no bus (cannot update EI):")
        show(DataFrames.select(no_bus,
            ["gen_name", "source", "capacity_mw"]), allrows=true)
    end

    if nrow(fuzzy) > 0
        println("\n    🔶 $label fuzzy-matched — please verify:")
        show(DataFrames.select(fuzzy,
            ["gen_name", "bus_id", "bus_name", "capacity_mw", "source"]),
            allrows=true)
    end
end

# ── Section 6: EI misclassification check ────────────────────────────────────
println("\n\n🔁 EI GENERATOR MISCLASSIFICATION CHECK")
println("─"^70)
println("  Wind generators in EI model matched to EIA wind entries.")
println("  Any non-WindGenerator type here is misclassified in EI.\n")

for (label, result) in [("VA", va_ei_wind_result),
                         ("MD", md_ei_wind_result),
                         ("WV", wv_ei_wind_result)]
    matched = filter(row -> !ismissing(row["Plant ID"]), result)
    nrow(matched) == 0 && continue
    println("  $label:")
    for gt in sort(unique(skipmissing(matched.generator_type)))
        n = count(==(gt), skipmissing(matched.generator_type))
        flag = gt == "WindGenerator" ? "✅" : "⚠️  MISCLASSIFIED"
        println("    $flag  $gt: $n")
    end
end

# ── Section 7: Summary counts ─────────────────────────────────────────────────
println("\n\n📌 FINAL SUMMARY — GENERATORS TO UPDATE IN EI")
println("─"^70)
println("  'EI_*'  sources = existing EI generators with updated metadata")
println("  'EIA_*' sources = NEW generators to be added to EI model\n")

total_ei_update  = 0
total_ei_new     = 0

for (label, df) in [("VA", va_wind_export),
                    ("MD", md_wind_export),
                    ("WV", wv_wind_export)]
    ei_rows  = filter(r -> startswith(coalesce(r.source, ""), "EI_"),  df)
    new_rows = filter(r -> startswith(coalesce(r.source, ""), "EIA_"), df)
    global total_ei_update += nrow(ei_rows)
    global total_ei_new    += nrow(new_rows)

    ei_mw  = round(sum(skipmissing(ei_rows.capacity_mw)),  digits=1)
    new_mw = round(sum(skipmissing(new_rows.capacity_mw)), digits=1)

    println("  $label:")
    println("    Update existing EI gens:  $(nrow(ei_rows))  ($(ei_mw) MW)")
    println("    Add new gens to EI:       $(nrow(new_rows))  ($(new_mw) MW)")
    println("    Cancelled (excluded):     $(count(r -> r[3] == label,
                                            cancelled_log))")
end

println("\n  ─"^35)
println("  TOTAL update existing: $total_ei_update generators")
println("  TOTAL add new:         $total_ei_new generators")
println("  TOTAL cancelled:       $(length(cancelled_log)) plants excluded")
println("█"^70)

# ── W17. Plots ────────────────────────────────────────────────────────────────
wp1 = plot(
    [bar(x    = wind_comparison[!, "State"],
         y    = wind_comparison[!, "eia2pf_total_mw"],
         name = "EIA2PF Reference (MW)",
         marker_color = "steelblue"),
     bar(x    = wind_comparison[!, "State"],
         y    = wind_comparison[!, "update_total_mw"],
         name = "EI Update Total (MW)",
         marker_color = "darkorange")],
    Layout(
        title       = "Wind: EI Update vs EIA2PF Reference (MW)",
        barmode     = "group",
        xaxis_title = "State",
        yaxis_title = "Capacity (MW)",
        legend      = attr(orientation="h", y=-0.2),
    )
)

wp2 = plot(
    [bar(x    = filter(r -> r.source == src, update_by_source)[!, "state"],
         y    = filter(r -> r.source == src, update_by_source)[!, "total_mw"],
         name = src,
         marker_color = get(WIND_SOURCE_COLORS, src, "gray"))
     for src in ["EI_EIA2PF", "EI_MMWG", "EIA_ONLY",
                 "EIA_MMWG_EXACT", "EIA_MMWG_FUZZY"]
     if src in unique(update_by_source[!, "source"])],
    Layout(
        title       = "Wind EI Update — Capacity by Match Source (MW)",
        barmode     = "stack",
        xaxis_title = "State",
        yaxis_title = "Capacity (MW)",
        legend      = attr(orientation="h", y=-0.2),
    )
)

# Percent captured bar
wp3 = plot(
    bar(
        x    = wind_comparison[!, "State"],
        y    = wind_comparison[!, "pct_captured"],
        marker_color = [
            pct >= 95 ? "#2ecc71" : pct >= 80 ? "#e67e22" : "#e74c3c"
            for pct in coalesce.(wind_comparison[!, "pct_captured"], 0.0)
        ],
        text  = [string(p, "%") for p in
                 coalesce.(wind_comparison[!, "pct_captured"], 0.0)],
        textposition = "outside",
    ),
    Layout(
        title       = "Wind EI Update — % of EIA2PF Capacity Captured",
        xaxis_title = "State",
        yaxis_title = "% Captured",
        yaxis       = attr(range=[0, 115]),
        shapes      = [attr(type="line", x0=-0.5, x1=2.5,
                            y0=100, y1=100,
                            line=attr(color="black", dash="dash", width=1))],
    )
)

display(wp1)
display(wp2)
display(wp3)
