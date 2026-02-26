#!/usr/bin/env python3

import xml.etree.ElementTree as ET


def create_prop(parent, prop_id, value):
    """Helper function to create a prop element."""
    prop = ET.SubElement(parent, 'prop')
    prop.set('id', prop_id)
    prop.set('value', value)


def generate_platform_xml(output_file, num_nodes=1600):
    """
    Generate a SimGrid platform XML file with a master host and homogeneous nodes.
    Environmental data (energy_mix, carbon/water intensities) is intentionally omitted
    here; the dynamic trace broadcasts the initial values to all hosts at t=0.

    Args:
        output_file: Path to the output XML file
        num_nodes: Number of homogeneous nodes to generate (default: 1600)
    """

    # Create root element
    platform = ET.Element('platform')
    platform.set('version', '4.1')

    # Create zone
    zone = ET.SubElement(platform, 'zone')
    zone.set('id', 'AS0')
    zone.set('routing', 'Full')

    # Create master host (management/login node, not a compute node)
    # Note: Batsim automatically assigns the 'master' role to the master host,
    # so we don't need to set it explicitly as a property
    master = ET.SubElement(zone, 'host')
    master.set('id', 'master_host')
    master.set('speed', '100Mf')
    create_prop(master, 'wattage_per_state', '10:200')

    # Create homogeneous compute nodes modelled after Mustang:
    #   1600 nodes, each with 24 AMD Opteron 6176 cores at 2.3 GHz,
    #   Educated guess: 10 W idle (machines are turned off when not in use). 320 W full load (dual-socket 115 W TDP + memory/chassis).
    # The AMD Opteron 6176 is based on the Magny-Cours (K10) architecture and supports SSE2 (128-bit).
    # This means each core can execute at most 2 double-precision FP operations per cycle (via 128-bit SSE).
    for i in range(num_nodes):
        node = ET.SubElement(zone, 'host')
        node.set('id', f'node-{i}')
        node.set('speed', '4.6Gf')
        node.set('core', '24')

        create_prop(node, 'role', 'compute_node')
        create_prop(node, 'wattage_per_state', '10:320')

    # Create tree and write to file
    tree = ET.ElementTree(platform)
    ET.indent(tree, space='    ')

    with open(output_file, 'w') as f:
        f.write("<?xml version='1.0'?>\n")
        f.write('<!DOCTYPE platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd">\n')
        tree.write(f, encoding='unicode', xml_declaration=False)

    print(f"Generated platform XML with {num_nodes} nodes at: {output_file}")


if __name__ == "__main__":
    generate_platform_xml('mustang_platform.xml', num_nodes=1600)