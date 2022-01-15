#=
acquire: Julia port of NYCbuswatcher acquire.py
- Julia version: 1.7.1
- Author: anthonytownsend
- Date: 2022-01-06
=#

using DotEnv
using Dates
using HTTP
using JSON
using TimeZones
using DataFrames
using Feather

function load_dotenv(filename = ".env")
    isfile(filename) || return
    for line in eachline(filename)
        var, val = strip.(split(line, "="))
        ENV[var] = val
    end
end

function get_routelist()
    url = "http://bustime.mta.info/api/where/routes-for-agency/MTA%20NYCT.json?key=$(ENV["API_KEY"])"
    println("fetching $url")
    routelist = JSON.parse(String(HTTP.request("GET",url).body))
    [ route["id"] for route in routelist["data"]["list"] ]
end


function get_urls(routelist)
    urls=Dict()
    for route in routelist
        route_clean = replace(route, " "  => "%20")
        url = "http://bustime.mta.info/api/siri/vehicle-monitoring.json?key=$(ENV["API_KEY"])&VehicleMonitoringDetailLevel=minimum&LineRef=$route_clean"
        push!(urls, (route => url ))
        # print("inserted /api/siri/vehicle-monitoring.json?key=$API_KEY&VehicleMonitoringDetailLevel=calls&LineRef=$route")
    end
    urls
end


# async -- after https://discourse.julialang.org/t/is-there-a-limit-on-the-number-of-async-tasks-that-can-be-scheduled/58992
function fetch_feeds(urls)
    feeds=Dict()
    @sync begin
        for (route, url) in urls
            @async begin
                try
                    data = JSON.parse(String(HTTP.request("GET",url).body))
                    push!(feeds, (route => data ))
                    print(".")
                catch e
                    println("Error fetching $(route) from $(url)")
                end
            end 
        end
    end
    println(" ")
    println("Feeds is $(length(feeds)) records long")
    feeds
end

## DEBUG unwarap them from Any?
# https://discourse.julialang.org/t/i-made-a-recursive-function-to-extract-a-value-from-a-json-given-the-key-for-that/56278
function extract_json(json, key)
    req = []
    function extract(json, key)
        for (k, v) in pairs(json)
            if k == key
                push!(req, v)
            elseif typeof(v) <: AbstractDict
                extract(v, key)
            end
        end
    end
    extract(json, key)
    req
end


function parse_feeds(feeds)

    lookup = Dict(
    "direction" => "DirectionRef",
    "service_date" =>  "DataFrameRef",
    "trip_id" =>  "DatedVehicleJourneyRef",
    "gtfs_shape_id" =>  "JourneyPatternRef",
    "route_short" =>  "PublishedLineName",
    "agency" =>  "OperatorRef",
    "origin_id" => "OriginRef",
    "destination_id" => "DestinationRef",
    "destination_name" => "DestinationName",
    "next_stop_id" =>  "StopPointRef",
    "next_stop_eta" =>  "ExpectedArrivalTime",
    "next_stop_d_along_route" =>  "CallDistanceAlongRoute",
    "next_stop_d" =>  "DistanceFromCall",
    "alert" =>  "SituationSimpleRef",
    "lat" => "Latitude",
    "lon" => "Longitude",
    "bearing" =>  "Bearing",
    "progress_rate" =>  "ProgressRate",
    "progress_status" =>  "ProgressStatus",
    "occupancy" =>  "Occupancy",
    "vehicle_id" => "VehicleRef",
    "gtfs_block_id" => "BlockRef",
    "passenger_count" =>  "EstimatedPassengerCount"
    )


    buses = DataFrame(
        recorded_at_time = ZonedDateTime[],
        direction = String[],
        service_date = Date[],
        trip_id = String[],
        gtfs_shape_id = String[],
        route_short = String[],
        agency = String[],
        origin_id = String[],
        destination_id = String[],
        destination_name = String[],
        next_stop_id = String[],
        next_stop_eta = DateTime[],
        next_stop_d_along_route = Float16[],
        next_stop_d = Float16[],
        alert = String[],
        lat = Float64[],
        lon = Float64[],
        bearing = Float16[],
        progress_rate = String[],
        progress_status = String[],
        occupancy = String[],
        vehicle_id = String[],
        gtfs_block_id = String[],
        passenger_count = Int8[]
    )

    for (route,feed) in feeds    
        try
            vehicleactivity = feed["Siri"]["ServiceDelivery"]["VehicleMonitoringDelivery"][1]["VehicleActivity"]
            for vehicle in vehicleactivity
                try
                    bus = Dict()
                    bus["recorded_at_time"] = ZonedDateTime(vehicle["RecordedAtTime"],"yyyy-mm-ddTHH:MM:SSS.ssszzzzz")
                    monitoredvehiclejourney=vehicle["MonitoredVehicleJourney"]
    
                    for (k,v) in lookup
                        bus[k] = extract_json(monitoredvehiclejourney, v)
                    end
                    
                    push!(buses, bus, cols=:subset)
                    print('b')
                catch e
                    println("Error parsing RecordedAtTime or MonitoredVehicleJourney")
                end
            
            end
            println()
        catch e
            println("No VehicleActivity for $route")
        
        end
    end
end


function dump_parsed_feeds(feeds)
    println("This function dumps to something.") 
end


function main()
    cfg = DotEnv.config()
    routelist = get_routelist()
    urls = get_urls(routelist)
    feeds = fetch_feeds(urls)
    buses = parse_feeds(feeds)
    dump_parsed_feeds(buses)
end


main()
