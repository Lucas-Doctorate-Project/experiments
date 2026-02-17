#!/usr/bin/env python3

import xml.etree.ElementTree as ET
import csv
import json
import os


def create_prop(parent, prop_id, value):
    """Helper function to create a prop element."""
    prop = ET.SubElement(parent, 'prop')
    prop.set('id', prop_id)
    prop.set('value', value)


def read_energy_mix_from_trace(trace_file):
    """Read the energy_mix value from the first data row of a trace CSV file."""
    with open(trace_file, 'r') as f:
        reader = csv.DictReader(f)
        row = next(reader)
        return row['new_value']


def map_source_to_default_key(source_name):
    """Map energy source name from CSV to the default key in intensities.json."""
    source_lower = source_name.lower()

    if 'coal' in source_lower or 'lignite' in source_lower:
        return 'coal-default'
    elif 'gas' in source_lower:
        return 'gas-default'
    elif 'oil' in source_lower:
        # Map oil to gas as proxy (similar peaking characteristics)
        return 'gas-default'
    elif 'nuclear' in source_lower:
        return 'nuclear-default'
    elif 'hydro' in source_lower:
        return 'hydro-default'
    elif 'solar' in source_lower:
        return 'solar-pv-default'
    elif 'wind' in source_lower:
        return 'wind-default'
    elif 'csp' in source_lower:
        return 'csp-default'
    elif 'bio' in source_lower:
        return 'biopower-default'
    elif 'geothermal' in source_lower:
        return 'geothermal-default'
    elif 'waste' in source_lower:
        # Map waste-to-energy to biopower (similar combustion process)
        return 'biopower-default'
    elif 'storage' in source_lower:
        # Energy storage: use minimal values (storage doesn't produce, just stores)
        # We'll use solar-pv as a low-emission proxy
        return 'solar-pv-default'
    else:
        return None


def normalize_energy_mix(energy_mix):
    """
    Normalize energy mix percentages to sum exactly to 100%.

    Args:
        energy_mix: Energy mix string (e.g., "Source1:10.5;Source2:89.49")

    Returns:
        Normalized energy mix string with percentages summing to exactly 100%
    """
    parts = []
    for part in energy_mix.split(';'):
        if ':' in part:
            source_name, percentage_str = part.split(':', 1)
            parts.append((source_name.strip(), float(percentage_str.strip())))

    # Calculate current sum
    current_sum = sum(pct for _, pct in parts)

    # Normalize to 100%
    if current_sum > 0 and abs(current_sum - 100.0) > 0.001:
        normalized_parts = [(source, (pct / current_sum) * 100.0) for source, pct in parts]
    else:
        normalized_parts = parts

    # Format back to string with 2 decimal places
    return ';'.join(f"{source}:{pct:.2f}" for source, pct in normalized_parts)


def generate_intensities_from_mix(energy_mix, intensities_data):
    """
    Generate carbon and water intensity strings based on energy mix sources.

    Args:
        energy_mix: Energy mix string (e.g., "Source1:10;Source2:20")
        intensities_data: Dictionary loaded from intensities.json

    Returns:
        Tuple of (carbon_intensity_string, water_intensity_string)
    """
    sources = []
    for part in energy_mix.split(';'):
        if ':' in part:
            source_name = part.split(':')[0].strip()
            sources.append(source_name)

    carbon_parts = []
    water_parts = []

    for source in sources:
        default_key = map_source_to_default_key(source)

        if default_key:
            # Use IPCC 2014 for defaults
            carbon_key = f"{default_key}-ipcc-2014"
            if carbon_key in intensities_data['carbon']:
                carbon_value = intensities_data['carbon'][carbon_key]
                carbon_parts.append(f"{source}: {carbon_value}")

            if default_key in intensities_data['water']:
                water_value = intensities_data['water'][default_key]
                water_parts.append(f"{source}: {water_value}")

    carbon_intensity = ';'.join(carbon_parts)
    water_intensity = ';'.join(water_parts)

    return carbon_intensity, water_intensity


def generate_platform_xml(output_file, energy_mix, carbon_intensity, water_intensity, num_nodes=1600):
    """
    Generate a SimGrid platform XML file with a master host and homogeneous nodes.

    Args:
        output_file: Path to the output XML file
        energy_mix: Energy mix string to use for all hosts
        carbon_intensity: Carbon intensity string for all hosts
        water_intensity: Water intensity string for all hosts
        num_nodes: Number of homogeneous nodes to generate (default: 1600)
    """

    # Create root element
    platform = ET.Element('platform')
    platform.set('version', '4.1')

    # Create zone
    zone = ET.SubElement(platform, 'zone')
    zone.set('id', 'AS0')
    zone.set('routing', 'Full')

    # Create master host
    # Note: Batsim automatically assigns the 'master' role to the master host,
    # so we don't need to set it explicitly as a property
    master = ET.SubElement(zone, 'host')
    master.set('id', 'master_host')
    master.set('speed', '100Mf')
    create_prop(master, 'wattage_per_state', '100:200')
    create_prop(master, 'wattage_off', '10')
    create_prop(master, 'energy_mix', energy_mix)
    create_prop(master, 'carbon_intensity', carbon_intensity)
    create_prop(master, 'water_intensity', water_intensity)

    # Create homogeneous nodes
    for i in range(num_nodes):
        node = ET.SubElement(zone, 'host')
        node.set('id', f'node-{i}')
        node.set('speed', '100.0Mf, 1e-9Mf, 0.5f, 0.05f')
        node.set('pstate', '0')

        create_prop(node, 'role', 'compute_node')
        create_prop(node, 'wattage_per_state', '30.0:30.0:100.0, 9.75:9.75:9.75, 200.996721311:200.996721311:200.996721311, 425.1743849:425.1743849:425.1743849')
        create_prop(node, 'wattage_off', '9.75')
        create_prop(node, 'sleep_pstates', '1:2:3')
        create_prop(node, 'energy_mix', energy_mix)
        create_prop(node, 'carbon_intensity', carbon_intensity)
        create_prop(node, 'water_intensity', water_intensity)

    # Create tree and write to file
    tree = ET.ElementTree(platform)
    ET.indent(tree, space='    ')

    with open(output_file, 'w') as f:
        f.write("<?xml version='1.0'?>\n")
        f.write('<!DOCTYPE platform SYSTEM "http://simgrid.gforge.inria.fr/simgrid/simgrid.dtd">\n')
        tree.write(f, encoding='unicode', xml_declaration=False)

    print(f"Generated platform XML with {num_nodes} nodes at: {output_file}")


if __name__ == "__main__":
    with open('intensity-factors/intensities.json', 'r') as f:
        intensities_data = json.load(f)

    traces = [
        {
            'name': 'clean_energy',
            'trace_file': 'energy-mix/clean_energy_trace.csv',
            'output_file': 'platform/clean_energy_platform.xml'
        },
        {
            'name': 'fossil_heavy',
            'trace_file': 'energy-mix/fossil_heavy_trace.csv',
            'output_file': 'platform/fossil_heavy_platform.xml'
        },
        {
            'name': 'mixed',
            'trace_file': 'energy-mix/mixed_trace.csv',
            'output_file': 'platform/mixed_platform.xml'
        }
    ]

    for trace in traces:
        energy_mix = read_energy_mix_from_trace(trace['trace_file'])
        energy_mix = normalize_energy_mix(energy_mix)  # Normalize to exactly 100%
        carbon_intensity, water_intensity = generate_intensities_from_mix(energy_mix, intensities_data)
        generate_platform_xml(trace['output_file'], energy_mix, carbon_intensity, water_intensity, num_nodes=1600)