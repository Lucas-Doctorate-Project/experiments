// platform.cpp
#include <simgrid/s4u.hpp>

#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <iomanip>

#include <cstdlib>   // getenv
#include <stdexcept> // runtime_error

#include "third_party/nlohmann/json.hpp" // nlohmann/json third-party library

using json = nlohmann::json;
namespace sg4 = simgrid::s4u;


std::pair<std::string, std::string> format_map(const std::map<std::string, std::pair<double, double>>& m)
{
    std::ostringstream carbon_s, water_s;
    carbon_s << std::fixed << std::setprecision(2);
    water_s << std::fixed << std::setprecision(2);

    bool first = true;
    for (const auto& [key, value] : m) {
        if (!first)
            carbon_s << ";";
        carbon_s << key << ":" << value.first;
        first = false;
    }

    first = true;
    for (const auto& [key, value] : m) {
        if (!first)
            water_s << ";";
        water_s << key << ":" << value.second * (3.78541 / 1000.0); // gal/Mwh to L/KWh
        first = false;
    }

    return {carbon_s.str(), water_s.str()};
}


struct HostSpec {
  std::string name;

  std::vector<std::string> speed;

  // properties
  std::unordered_map<std::string, std::string> props;
};

static void add_hosts(sg4::NetZone* zone, const std::vector<HostSpec>& hosts)
{
  for (const auto& h : hosts) {
    auto* host = zone->add_host(h.name, h.speed);

    if (!h.props.empty())
      host->set_properties(h.props);
      

    host->seal();
  }
}

extern "C" void load_platform(sg4::Engine& e);
void load_platform(sg4::Engine& e)
{
  std::string energy_mix = "";
  std::map<std::string, std::pair<double, double>> intensities; // pair<carbon, water>

  const char* json_path_c = std::getenv("PLAT_JSON");
  std::string json_path = json_path_c ? json_path_c : "platform.json";

  // read json intensities file
  std::ifstream f(json_path);
  if (!f)
    throw std::runtime_error("Error when opening: " + json_path);
  json j;
  try {
    f >> j; // parse
  } catch (const json::parse_error& e) {
    throw std::runtime_error(std::string("Invalid JSON: ") + e.what());
  }

  // get only the useful info from json file

  // biomass
  double biomass_ipcc_2014 = j.at("carbon").at("biomass-ipcc-2014").get<double>();
  double biomass_steam_tower = j.at("water").at("biopower-steam-tower").get<double>();
  double biomass_biogas_tower = j.at("water").at("biopower-biogas-tower").get<double>();
  double biomass_once_through = j.at("water").at("biopower-steam-once-through").get<double>();
  double biomass_steam_pond = j.at("water").at("biopower-steam-pond").get<double>();

  // fossil gas
  double gas_ngcc_unece_2020 = j.at("carbon").at("gas-ngcc-unece-2020").get<double>();
  double gas_ngcc_tower = j.at("water").at("gas-ngcc-tower").get<double>();
  double gas_ngcc_once_through = j.at("water").at("gas-ngcc-once-through").get<double>();
  double gas_ngcc_pond = j.at("water").at("gas-ngcc-pond").get<double>(); 

  // fossil hard coal, lignite
  double coal_pc_unece_2020 = j.at("carbon").at("coal-pc-unece-2020").get<double>();
  double coal_sc_unece_2020 = j.at("carbon").at("coal-sc-unece-2020").get<double>();
  double coal_pc_subc_tower = j.at("water").at("coal-pc-subc-tower").get<double>();
  double coal_pc_sc_tower = j.at("water").at("coal-pc-sc-tower").get<double>();
  double coal_pc_subc_once_through = j.at("water").at("coal-pc-subc-once-through").get<double>();
  double coal_pc_sc_once_through = j.at("water").at("coal-pc-sc-once-through").get<double>();
  double coal_pc_subc_pond = j.at("water").at("coal-pc-subc-pond").get<double>();
  double coal_pc_sc_pond = j.at("water").at("coal-pc-sc-pond").get<double>();

  // fossil coal-derived gas
  double coal_igcc_unece_2020 = j.at("carbon").at("coal-igcc-unece-2020").get<double>();
  double coal_igcc_tower = j.at("water").at("coal-igcc-tower").get<double>();

  // Hydro Run-of-river and pondage, Hydro Water Reservoir
  double hydro_large_unece_2020 = j.at("carbon").at("hydro-large-unece-2020").get<double>();
  double hydro_medium_unece_2020 = j.at("carbon").at("hydro-medium-unece-2020").get<double>();
  double hydro = j.at("water").at("hydro").get<double>();

  // solar
  double solar_pv_poly_si_roof_unece_2020 = j.at("carbon").at("solar-pv-poly-si-roof-unece-2020").get<double>();
  double solar_pv_poly_si_ground_unece_2020 = j.at("carbon").at("solar-pv-poly-si-ground-unece-2020").get<double>();
  double solar_pv_cdte_roof_unece_2020 = j.at("carbon").at("solar-pv-cdte-roof-unece-2020").get<double>();
  double solar_pv_cdte_ground_unece_2020 = j.at("carbon").at("solar-pv-cdte-ground-unece-2020").get<double>();
  double solar_pv_cigs_roof_unece_2020 = j.at("carbon").at("solar-pv-cigs-roof-unece-2020").get<double>();
  double solar_pv_cigs_ground_unece_2020 = j.at("carbon").at("solar-pv-cigs-ground-unece-2020").get<double>();
  double solar_pv = j.at("water").at("solar-pv").get<double>();

  // wind offshore
  double wind_offshore_concrete_unece_2020 = j.at("carbon").at("wind-offshore-concrete-unece-2020").get<double>();
  double wind_offshore_steel_unece_2020 = j.at("carbon").at("wind-offshore-steel-unece-2020").get<double>();
  double wind = j.at("water").at("wind").get<double>();

  // wind onshore
  double wind_onshore_unece_2020 = j.at("carbon").at("wind-onshore-unece-2020").get<double>();

  // nuclear
  double nuclear_unece_2020 = j.at("carbon").at("nuclear-unece-2020").get<double>();
  double nuclear_tower = j.at("water").at("nuclear-tower").get<double>();
  double nuclear_once_through = j.at("water").at("nuclear-once-through").get<double>();
  double nuclear_pond = j.at("water").at("nuclear-pond").get<double>();

  // geothermal
  double geothermal_ipcc_2014 = j.at("carbon").at("geothermal-ipcc-2014").get<double>();
  double geothermal_flash_tower = j.at("water").at("geothermal-flash-tower").get<double>();
  double geothermal_flash_dry = j.at("water").at("geothermal-flash-dry").get<double>();
  double geothermal_binary_dry = j.at("water").at("geothermal-binary-dry").get<double>();
  double geothermal_egs_dry = j.at("water").at("geothermal-egs-dry").get<double>();
  double geothermal_binary_hybrid = j.at("water").at("geothermal-binary-hybrid").get<double>();

  intensities["Biomass"] = {biomass_ipcc_2014, 0.25*(biomass_biogas_tower+biomass_once_through+biomass_steam_pond+biomass_steam_tower)};
  intensities["Fossil Gas"] = {gas_ngcc_unece_2020, 0.33*(gas_ngcc_tower+gas_ngcc_once_through+gas_ngcc_pond)};
  intensities["Fossil Hard coal"] = {0.95*coal_pc_unece_2020 + 0.05*coal_sc_unece_2020, 0.3*(coal_pc_subc_tower+coal_pc_subc_once_through+coal_pc_subc_pond) + 0.03*(coal_pc_sc_once_through+coal_pc_sc_pond+coal_pc_sc_tower)};
  intensities["Fossil Brown coal/Lignite"] = intensities["Fossil Hard coal"];
  intensities["Fossil Coal-derived gas"] = {coal_igcc_unece_2020, coal_igcc_tower};
  intensities["Hydro Run-of-river and pondage"] = {0.95*hydro_medium_unece_2020 + 0.05*hydro_large_unece_2020, hydro};
  intensities["Hydro Water Reservoir"] = intensities["Hydro Run-of-river and pondage"];
  intensities["Solar"] = {0.45*solar_pv_poly_si_roof_unece_2020 + 0.45*solar_pv_poly_si_ground_unece_2020 + 0.025 * (solar_pv_cdte_ground_unece_2020 + solar_pv_cdte_roof_unece_2020 + solar_pv_cigs_ground_unece_2020 + solar_pv_poly_si_roof_unece_2020), solar_pv};
  intensities["Wind Offshore"] = {0.5*wind_offshore_concrete_unece_2020 + 0.5*wind_offshore_steel_unece_2020, wind};
  intensities["Wind Onshore"] = {wind_onshore_unece_2020, wind};
  intensities["Nuclear"] = {nuclear_unece_2020, 0.33*(nuclear_tower + nuclear_once_through + nuclear_pond)};
  intensities["Geothermal"] = {geothermal_ipcc_2014, 0.2*(geothermal_flash_tower+geothermal_flash_dry+geothermal_binary_dry+geothermal_binary_hybrid+geothermal_egs_dry)};

  intensities["Waste"] = {0.0, 0.0};
  intensities["Fossil Oil"] = {0.0, 0.0};
  intensities["Hydro Pumped Storage"] = {0.0, 0.0};

  const char* plat_type_c = std::getenv("PLAT_TYPE");
  std::string plat_type = plat_type_c ? plat_type_c : "";

  if (plat_type == "clean") {
    energy_mix = "Biomass:0.36;Fossil Gas:9.09;Fossil Hard coal:0.00;Fossil Oil:0.09;Hydro Pumped Storage:1.61;Hydro Run-of-river and pondage:7.24;Hydro Water Reservoir:5.19;Nuclear:72.21;Solar:0.00;Waste:0.50;Wind Offshore:0.13;Wind Onshore:3.32;Energy storage:0.25";
  } else if (plat_type == "mixed") {
    energy_mix = "Biomass:9.02;Fossil Brown coal/Lignite:19.07;Fossil Coal-derived gas:1.43;Fossil Gas:29.09;Fossil Hard coal:7.12;Fossil Oil:0.25;Geothermal:0.05;Hydro Pumped Storage:0.21;Hydro Run-of-river and pondage:2.04;Hydro Water Reservoir:0.03;Other renewable:0.17;Solar:0.00;Waste:1.67;Wind Offshore:4.05;Wind Onshore:25.10;Other:0.70";
  } else if (plat_type == "fossil") {
    energy_mix = "Biomass:2.06;Fossil Brown coal/Lignite:10.83;Fossil Coal-derived gas:0.78;Fossil Gas:17.85;Fossil Hard coal:32.85;Fossil Oil:1.18;Hydro Pumped Storage:0.00;Hydro Run-of-river and pondage:0.27;Hydro Water Reservoir:0.11;Other renewable:0.34;Solar:0.00;Wind Onshore:30.36;Other:3.39";
  } else {
    throw std::runtime_error("Invalid PLAT_TYPE. Possible values: 'clean', 'mixed' or 'fossil'");
  }

  std::pair<std::string, std::string> formatted = format_map(intensities);


  // create zone
  auto* zone = e.get_netzone_root()->add_netzone_full("AS0");

  // create hosts
  std::vector<HostSpec> hosts = {
      HostSpec{
          "master_host",
          {"100Mf"},
          std::unordered_map<std::string, std::string>{
              {"wattage_per_state", "100:200"},
              {"wattage_off", "10"},
              {"energy_mix", energy_mix},
              {"carbon_intensity", formatted.first},
              {"water_intensity", formatted.second},
          },
      }
  };
  
  for (int i = 0; i<1600 ; i++) {
    hosts.push_back(
      HostSpec {
        "host"+std::to_string(i),
        {"100.0Mf", "1e-9Mf", "0.5f", "0.05f"},
        std::unordered_map<std::string, std::string>{
            {"wattage_per_state", "30.0:30.0:100.0, 9.75:9.75:9.75, 200.996721311:200.996721311:200.996721311, 425.1743849:425.1743849:425.1743849"},
            {"wattage_off", "9.75"},
            {"sleep_pstates", "1:2:3"},
            {"energy_mix", energy_mix},
            {"carbon_intensity", formatted.first},
            {"water_intensity", formatted.second},
        },
      }
    );
  }

  // add hosts
  add_hosts(zone, hosts);

  zone->seal();
}
