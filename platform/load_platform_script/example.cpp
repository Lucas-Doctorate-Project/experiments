#include <simgrid/s4u.hpp>
#include <simgrid/plugins/environmental_footprint.h>
#include <iostream>

XBT_LOG_NEW_DEFAULT_CATEGORY(s4u_app_test, "Messages specific for this example");

namespace sg4 = simgrid::s4u;


static void print_host_properties(sg4::Host* host)
{
  XBT_INFO("Host %s: speed=%.3g flops/s; pstate=%ld", host->get_cname(), host->get_speed(), host->get_pstate());

  const auto* props = host->get_properties(); 
  if (!props || props->empty()) {
    XBT_INFO("  (no properties)");
    return;
  }

  for (const auto& [k, v] : *props) {
    XBT_INFO("  %s = %s", k.c_str(), v.c_str());
  }
}


static void worker()
{
  auto* h = sg4::this_actor::get_host();
  XBT_INFO("Host %s is running", h->get_cname());

  sg4::this_actor::execute(1e9);

  XBT_INFO("Host %s finished", h->get_cname());
}

int main(int argc, char** argv)
{
  sg4::Engine e(&argc, argv);
  sg_host_environmental_footprint_plugin_init();

  // load .so
  e.load_platform("./libplatform.so");

  for (auto* h : sg4::Engine::get_instance()->get_all_hosts()) {
    print_host_properties(h);
    sg4::Actor::create(std::string("actor-") + h->get_name(), h, worker);
  }

  e.run();

  return 0;
}
