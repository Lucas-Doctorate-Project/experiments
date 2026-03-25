#!/usr/bin/env python3

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import re
import sys
import tomllib
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import requests


ENTSOE_API_URL = "https://web-api.tp.entsoe.eu/api"
ENTSOE_EIC_CODES_XML_URL = "https://eepublicdownloads.blob.core.windows.net/cio-lio/xml/allocated-eic-codes.xml"
REQUEST_TIMEOUT_SECONDS = 30
API_TOKEN_ENV_VARS = ("ENTSOE_API_TOKEN", "API_TOKEN")
PREFERRED_COUNTRY_FUNCTIONS = ("Member State", "Bidding Zone", "Control Area")
DOCUMENT_TYPE = "A75"
PROCESS_TYPE = "A16"
DEFAULT_HOST_ID = "AS0"
DEFAULT_INTENSITIES_JSON = "intensities.json"
DEFAULT_ENTSOE_DATA_DIR = "entsoe-data"
DEFAULT_TRACE_DATA_DIR = "traces"
LOG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
MIX_SOURCE_NAME = "Mix"
PSR_TYPE_NAMES = {
    "B01": "Biomass",
    "B02": "Fossil Brown coal/Lignite",
    "B03": "Fossil Coal-derived gas",
    "B04": "Fossil Gas",
    "B05": "Fossil Hard coal",
    "B06": "Fossil Oil",
    "B07": "Fossil Oil shale",
    "B08": "Fossil Peat",
    "B09": "Geothermal",
    "B10": "Hydro Pumped Storage",
    "B11": "Hydro Run-of-river and pondage",
    "B12": "Hydro Water Reservoir",
    "B13": "Marine",
    "B14": "Nuclear",
    "B15": "Other renewable",
    "B16": "Solar",
    "B17": "Waste",
    "B18": "Wind Offshore",
    "B19": "Wind Onshore",
    "B20": "Other",
    "B25": "Energy storage",
}

COUNTRY_TIMEZONES = {
    "AL": "Europe/Tirane",
    "AT": "Europe/Vienna",
    "BA": "Europe/Sarajevo",
    "BE": "Europe/Brussels",
    "BG": "Europe/Sofia",
    "CH": "Europe/Zurich",
    "CY": "Asia/Nicosia",
    "CZ": "Europe/Prague",
    "DE": "Europe/Berlin",
    "DK": "Europe/Copenhagen",
    "EE": "Europe/Tallinn",
    "ES": "Europe/Madrid",
    "FI": "Europe/Helsinki",
    "FR": "Europe/Paris",
    "GB": "Europe/London",
    "GR": "Europe/Athens",
    "HR": "Europe/Zagreb",
    "HU": "Europe/Budapest",
    "IE": "Europe/Dublin",
    "IS": "Atlantic/Reykjavik",
    "IT": "Europe/Rome",
    "LT": "Europe/Vilnius",
    "LU": "Europe/Luxembourg",
    "LV": "Europe/Riga",
    "ME": "Europe/Podgorica",
    "MK": "Europe/Skopje",
    "MT": "Europe/Malta",
    "NL": "Europe/Amsterdam",
    "NO": "Europe/Oslo",
    "PL": "Europe/Warsaw",
    "PT": "Europe/Lisbon",
    "RO": "Europe/Bucharest",
    "RS": "Europe/Belgrade",
    "SE": "Europe/Stockholm",
    "SI": "Europe/Ljubljana",
    "SK": "Europe/Bratislava",
    "TR": "Europe/Istanbul",
    "UA": "Europe/Kyiv",
}

LOGGER = logging.getLogger(__name__)

# ENTSO-E production types are fixed. These defaults point to the generic
# intensity families and are resolved to actual keys in intensities.json.
DEFAULT_SOURCE_LABELS: dict[str, dict[str, str]] = {
    "Biomass": {"carbon": "biopower", "water": "biopower"},
    "Energy storage": {"carbon": "solar-pv", "water": "solar-pv"},
    "Fossil Brown coal/Lignite": {"carbon": "coal", "water": "coal"},
    "Fossil Coal-derived gas": {"carbon": "gas", "water": "gas"},
    "Fossil Gas": {"carbon": "gas", "water": "gas"},
    "Fossil Hard coal": {"carbon": "coal", "water": "coal"},
    "Fossil Oil": {"carbon": "gas", "water": "gas"},
    "Fossil Oil shale": {"carbon": "coal", "water": "coal"},
    "Fossil Peat": {"carbon": "coal", "water": "coal"},
    "Geothermal": {"carbon": "geothermal", "water": "geothermal"},
    "Hydro Pumped Storage": {"carbon": "hydro", "water": "hydro"},
    "Hydro Run-of-river and pondage": {"carbon": "hydro", "water": "hydro"},
    "Hydro Water Reservoir": {"carbon": "hydro", "water": "hydro"},
    "Marine": {"carbon": "ocean", "water": "other"},
    "Nuclear": {"carbon": "nuclear", "water": "nuclear"},
    "Other": {"carbon": "other", "water": "other"},
    "Other renewable": {"carbon": "solar-pv", "water": "solar-pv"},
    "Solar": {"carbon": "solar-pv", "water": "solar-pv"},
    "Waste": {"carbon": "biopower", "water": "biopower"},
    "Wind Offshore": {"carbon": "wind", "water": "wind"},
    "Wind Onshore": {"carbon": "wind", "water": "wind"},
}


@dataclass(frozen=True)
class ExtractConfig:
    dataset_name: str
    country_code: str
    timezone_name: str


@dataclass(frozen=True)
class OutputConfig:
    dataset_name: str
    output_csv: Path
    host_id: str


@dataclass(frozen=True)
class SourceMapping:
    carbon_key: str
    water_key: str
    carbon_value: str
    water_value: str


@dataclass(frozen=True)
class PipelineConfig:
    config_path: Path
    run_fetch: bool
    run_export: bool
    normalized_csv: Path
    year: int
    week: int
    extracts: list[ExtractConfig]
    intensities_json: Path
    outputs: list[OutputConfig]
    source_mappings: dict[str, SourceMapping]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch ENTSO-E generation-by-source data and optionally export Batsim-compatible traces."
    )
    parser.add_argument("--config", required=True, help="Path to the TOML pipeline configuration.")
    return parser.parse_args()


def configure_logging() -> None:
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, stream=sys.stdout)


def load_config(config_path: Path) -> PipelineConfig:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file does not exist: {config_path}")

    with config_path.open("rb") as handle:
        data = tomllib.load(handle)

    year = parse_positive_int(data.get("year"), "year")
    week = parse_positive_int(data.get("week"), "week")
    validate_iso_week(year, week)

    run_fetch = parse_bool(data.get("fetch", True), "fetch")
    run_export = parse_bool(data.get("export", True), "export")
    if not run_fetch and not run_export:
        raise ValueError("At least one stage must be enabled. Set 'fetch = true' or 'export = true'.")

    normalized_csv_raw = data.get("output_csv")
    if normalized_csv_raw is None:
        normalized_csv = resolve_path(config_path.parent, default_entsoe_csv_name(year, week))
    elif isinstance(normalized_csv_raw, str) and normalized_csv_raw.strip():
        normalized_csv = resolve_path(config_path.parent, normalized_csv_raw)
    else:
        raise ValueError("Config field 'output_csv' must be a non-empty string when provided.")

    extracts_raw = data.get("extracts")
    if not isinstance(extracts_raw, list) or not extracts_raw:
        raise ValueError("Config field 'extracts' is required and must contain at least one entry.")

    extracts: list[ExtractConfig] = []
    seen_names: set[str] = set()
    for index, entry in enumerate(extracts_raw, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"Extract #{index} must be a TOML table.")

        country_code = entry.get("country")
        if not isinstance(country_code, str) or not country_code.strip():
            raise ValueError(f"Extract #{index} is missing a valid 'country'.")
        country_code = country_code.strip().upper()
        timezone_name = resolve_country_timezone_name(country_code)

        dataset_name = entry.get("name", country_code)
        if not isinstance(dataset_name, str) or not dataset_name.strip():
            raise ValueError(f"Extract #{index} has an invalid 'name'.")
        if dataset_name in seen_names:
            raise ValueError(f"Duplicate extract name '{dataset_name}' in pipeline config.")
        seen_names.add(dataset_name)
        extracts.append(ExtractConfig(dataset_name=dataset_name, country_code=country_code, timezone_name=timezone_name))

    intensities_json_raw = data.get("intensities_json", DEFAULT_INTENSITIES_JSON)
    if not isinstance(intensities_json_raw, str) or not intensities_json_raw.strip():
        raise ValueError("Config field 'intensities_json' must be a non-empty string when provided.")
    intensities_path = resolve_path(config_path.parent, intensities_json_raw)
    if not intensities_path.exists():
        raise FileNotFoundError(f"Intensities file does not exist: {intensities_path}")

    with intensities_path.open("r", encoding="utf-8") as handle:
        intensities_data = json.load(handle)

    carbon_data = intensities_data.get("carbon")
    water_data = intensities_data.get("water")
    if not isinstance(carbon_data, dict) or not isinstance(water_data, dict):
        raise ValueError(f"Intensities file {intensities_path} must contain 'carbon' and 'water' objects.")

    source_mappings = build_default_source_mappings(carbon_data=carbon_data, water_data=water_data)
    source_mappings_raw = data.get("source_mappings", {})
    if not isinstance(source_mappings_raw, dict):
        raise ValueError("Config field 'source_mappings' must be a TOML table when provided.")
    source_mappings.update(build_explicit_source_mappings(source_mappings_raw, carbon_data, water_data))

    outputs_raw = data.get("outputs", [])
    if not isinstance(outputs_raw, list):
        raise ValueError("Config field 'outputs' must be an array of TOML tables when provided.")

    outputs: list[OutputConfig] = []
    seen_output_names: set[str] = set()
    for index, entry in enumerate(outputs_raw, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"Output #{index} must be a TOML table.")

        dataset_name = entry.get("dataset")
        if not isinstance(dataset_name, str) or not dataset_name.strip():
            raise ValueError(f"Output #{index} is missing a valid 'dataset'.")
        if dataset_name in seen_output_names:
            raise ValueError(f"Duplicate output dataset '{dataset_name}' in pipeline config.")
        seen_output_names.add(dataset_name)

        output_csv_raw = entry.get("path", f"{dataset_name}_trace.csv")
        if not isinstance(output_csv_raw, str) or not output_csv_raw.strip():
            raise ValueError(f"Output #{index} has an invalid 'path'.")

        host_id = entry.get("host_id", DEFAULT_HOST_ID)
        if not isinstance(host_id, str) or not host_id.strip():
            raise ValueError(f"Output #{index} has an invalid 'host_id'.")

        outputs.append(
            OutputConfig(
                dataset_name=dataset_name,
                output_csv=resolve_path(config_path.parent, output_csv_raw),
                host_id=host_id,
            )
        )

    return PipelineConfig(
        config_path=config_path,
        run_fetch=run_fetch,
        run_export=run_export,
        normalized_csv=normalized_csv,
        year=year,
        week=week,
        extracts=extracts,
        intensities_json=intensities_path,
        outputs=outputs,
        source_mappings=source_mappings,
    )


def parse_positive_int(raw_value: object, field_name: str) -> int:
    if not isinstance(raw_value, int) or raw_value <= 0:
        raise ValueError(f"Config field '{field_name}' must be a positive integer.")
    return raw_value


def parse_bool(raw_value: object, field_name: str) -> bool:
    if not isinstance(raw_value, bool):
        raise ValueError(f"Config field '{field_name}' must be a boolean.")
    return raw_value


def validate_iso_week(year: int, week: int) -> None:
    try:
        datetime.fromisocalendar(year, week, 1)
    except ValueError as exc:
        raise ValueError(f"Invalid ISO week/year combination: year={year}, week={week}.") from exc


def resolve_country_timezone_name(country_code: str) -> str:
    timezone_name = COUNTRY_TIMEZONES.get(country_code)
    if timezone_name is None:
        raise ValueError(
            f"Could not resolve a local timezone for country '{country_code}'. Add it to COUNTRY_TIMEZONES before collecting data."
        )
    return timezone_name


def compute_extract_week_window(extract: ExtractConfig, year: int, week: int) -> tuple[datetime, datetime, datetime, datetime]:
    local_timezone = ZoneInfo(extract.timezone_name)
    start_local = datetime.fromisocalendar(year, week, 1).replace(tzinfo=local_timezone)
    end_local = start_local + timedelta(days=7)
    return start_local, end_local, start_local.astimezone(timezone.utc), end_local.astimezone(timezone.utc)


def resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path)
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def default_entsoe_csv_name(year: int, week: int) -> str:
    return f"{DEFAULT_ENTSOE_DATA_DIR}/entsoe_{year}_W{week:02d}.csv"


def format_utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def format_api_period(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y%m%d%H%M")


def parse_api_datetime(raw_value: object, field_name: str) -> datetime:
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ValueError(f"{field_name} must be a non-empty datetime string.")

    value = raw_value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must include a timezone offset.")
    return parsed.astimezone(timezone.utc)


def parse_resolution(raw_value: object, field_name: str) -> timedelta:
    if not isinstance(raw_value, str) or not raw_value.strip():
        raise ValueError(f"{field_name} must be a non-empty ISO-8601 duration.")

    match = re.fullmatch(
        r"P(?:(?P<days>\d+)D)?(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+)S)?)?",
        raw_value,
    )
    if match is None:
        raise ValueError(f"Unsupported ISO-8601 duration '{raw_value}' in {field_name}.")

    parts = {key: int(value) if value is not None else 0 for key, value in match.groupdict().items()}
    delta = timedelta(
        days=parts["days"],
        hours=parts["hours"],
        minutes=parts["minutes"],
        seconds=parts["seconds"],
    )
    if delta <= timedelta(0):
        raise ValueError(f"{field_name} must be greater than zero.")
    return delta


def local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def child_elements(parent: ET.Element, name: str) -> list[ET.Element]:
    return [child for child in list(parent) if local_name(child.tag) == name]


def find_child(parent: ET.Element, name: str) -> ET.Element | None:
    for child in list(parent):
        if local_name(child.tag) == name:
            return child
    return None


def find_text(parent: ET.Element, path: tuple[str, ...]) -> str | None:
    current = parent
    for name in path:
        child = find_child(current, name)
        if child is None:
            return None
        current = child
    return current.text.strip() if current.text else None


def require_api_token() -> str:
    for env_var in API_TOKEN_ENV_VARS:
        token = os.getenv(env_var)
        if token:
            return token
    joined = ", ".join(API_TOKEN_ENV_VARS)
    raise RuntimeError(f"Set one of the API token environment variables before running the fetch stage: {joined}")


def fetch_country_eic_map() -> dict[str, str]:
    LOGGER.info("Fetching ENTSO-E country EIC codes")
    response = requests.get(ENTSOE_EIC_CODES_XML_URL, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()
    root = ET.fromstring(response.content.lstrip(b"\xef\xbb\xbf"))

    candidates: dict[str, tuple[int, str]] = {}
    for document in child_elements(root, "EICCode_MarketDocument"):
        display_name = find_text(document, ("display_Names.name",))
        function_name = find_text(document, ("Function_Names", "name"))
        eic_code = find_text(document, ("mRID",))
        if display_name is None or function_name is None or eic_code is None:
            continue

        country_code = display_name.strip().upper()
        if len(country_code) != 2 or function_name not in PREFERRED_COUNTRY_FUNCTIONS:
            continue

        rank = PREFERRED_COUNTRY_FUNCTIONS.index(function_name)
        previous = candidates.get(country_code)
        if previous is None or rank < previous[0]:
            candidates[country_code] = (rank, eic_code)

    if not candidates:
        raise RuntimeError("Could not resolve any country EIC codes from the official ENTSO-E XML list.")

    result = {country_code: eic_code for country_code, (_, eic_code) in candidates.items()}
    LOGGER.info("Resolved %d ENTSO-E country EIC codes", len(result))
    return result


def fetch_generation_rows(
    extract: ExtractConfig,
    config: PipelineConfig,
    country_eic: str,
    api_token: str,
) -> list[dict[str, object]]:
    _, _, request_start_utc, request_end_utc = compute_extract_week_window(extract, config.year, config.week)
    LOGGER.info(
        "Fetching dataset '%s' for country %s (%s) from %s to %s",
        extract.dataset_name,
        extract.country_code,
        extract.timezone_name,
        format_utc_iso(request_start_utc),
        format_utc_iso(request_end_utc),
    )
    params = {
        "securityToken": api_token,
        "documentType": DOCUMENT_TYPE,
        "processType": PROCESS_TYPE,
        "in_Domain": country_eic,
        "periodStart": format_api_period(request_start_utc),
        "periodEnd": format_api_period(request_end_utc),
    }
    response = requests.get(ENTSOE_API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
    response.raise_for_status()

    root = ET.fromstring(response.content)
    root_name = local_name(root.tag)
    if root_name == "Acknowledgement_MarketDocument":
        reason_text = find_text(root, ("Reason", "text")) or "No reason provided by ENTSO-E."
        raise RuntimeError(f"ENTSO-E API returned an acknowledgement for dataset '{extract.dataset_name}': {reason_text}")
    if root_name != "GL_MarketDocument":
        raise RuntimeError(f"Unexpected ENTSO-E API document type '{root_name}' for dataset '{extract.dataset_name}'.")

    rows: list[dict[str, object]] = []
    for time_series in child_elements(root, "TimeSeries"):
        psr_type_code = find_text(time_series, ("MktPSRType", "psrType"))
        if psr_type_code is None:
            raise RuntimeError(f"TimeSeries without psrType in dataset '{extract.dataset_name}'.")
        source_name = PSR_TYPE_NAMES.get(psr_type_code)
        if source_name is None:
            raise RuntimeError(f"Unknown ENTSO-E psrType '{psr_type_code}' in dataset '{extract.dataset_name}'.")

        for period in child_elements(time_series, "Period"):
            period_start_utc = parse_api_datetime(
                find_text(period, ("timeInterval", "start")),
                f"{extract.dataset_name}/{source_name} period start",
            )
            period_end_utc = parse_api_datetime(
                find_text(period, ("timeInterval", "end")),
                f"{extract.dataset_name}/{source_name} period end",
            )
            resolution = parse_resolution(
                find_text(period, ("resolution",)),
                f"{extract.dataset_name}/{source_name} resolution",
            )

            for point in child_elements(period, "Point"):
                position_raw = find_text(point, ("position",))
                quantity_raw = find_text(point, ("quantity",))
                if position_raw is None:
                    raise RuntimeError(f"Point without position in dataset '{extract.dataset_name}/{source_name}'.")
                if quantity_raw is None:
                    raise RuntimeError(f"Point without quantity in dataset '{extract.dataset_name}/{source_name}'.")

                point_position = int(position_raw)
                interval_start_utc = period_start_utc + (point_position - 1) * resolution
                interval_end_utc = interval_start_utc + resolution
                if interval_end_utc > period_end_utc:
                    raise RuntimeError(
                        f"Dataset '{extract.dataset_name}' has point {point_position} past the declared interval for '{source_name}'."
                    )

                rows.append(
                    {
                        "dataset_name": extract.dataset_name,
                        "country_code": extract.country_code,
                        "country_eic": country_eic,
                        "year": config.year,
                        "iso_week": config.week,
                        "request_start_utc": format_utc_iso(request_start_utc),
                        "request_end_utc": format_utc_iso(request_end_utc),
                        "source_code": psr_type_code,
                        "source_name": source_name,
                        "period_start_utc": format_utc_iso(period_start_utc),
                        "period_end_utc": format_utc_iso(period_end_utc),
                        "resolution_seconds": int(resolution.total_seconds()),
                        "point_position": point_position,
                        "interval_start_utc": format_utc_iso(interval_start_utc),
                        "interval_end_utc": format_utc_iso(interval_end_utc),
                        "generation_mw": float(quantity_raw),
                    }
                )

    if not rows:
        raise RuntimeError(f"Dataset '{extract.dataset_name}' produced no rows.")

    LOGGER.info(
        "Fetched %d rows for dataset '%s' (%s)",
        len(rows),
        extract.dataset_name,
        extract.country_code,
    )
    return rows


def write_normalized_rows(output_csv: Path, rows: list[dict[str, object]]) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "dataset_name",
        "country_code",
        "country_eic",
        "year",
        "iso_week",
        "request_start_utc",
        "request_end_utc",
        "source_code",
        "source_name",
        "period_start_utc",
        "period_end_utc",
        "resolution_seconds",
        "point_position",
        "interval_start_utc",
        "interval_end_utc",
        "generation_mw",
    ]
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(
            sorted(
                rows,
                key=lambda row: (
                    str(row["dataset_name"]),
                    str(row["source_code"]),
                    str(row["interval_start_utc"]),
                    int(row["point_position"]),
                ),
            )
        )


def resolve_default_carbon_key(label: str, carbon_data: dict[str, object]) -> str:
    candidates = [f"{label}-default-unece-2020", f"{label}-default-ipcc-2014", label]
    for candidate in candidates:
        if candidate in carbon_data:
            return candidate
    raise ValueError(f"Could not resolve a default carbon key for label '{label}'.")


def resolve_default_water_key(label: str, water_data: dict[str, object]) -> str:
    candidates = [f"{label}-default", label]
    for candidate in candidates:
        if candidate in water_data:
            return candidate
    raise ValueError(f"Could not resolve a default water key for label '{label}'.")


def build_default_source_mappings(
    carbon_data: dict[str, object],
    water_data: dict[str, object],
) -> dict[str, SourceMapping]:
    mappings: dict[str, SourceMapping] = {}
    for source_name, labels in DEFAULT_SOURCE_LABELS.items():
        carbon_key = resolve_default_carbon_key(labels["carbon"], carbon_data)
        water_key = resolve_default_water_key(labels["water"], water_data)
        mappings[source_name] = SourceMapping(
            carbon_key=carbon_key,
            water_key=water_key,
            carbon_value=str(carbon_data[carbon_key]),
            water_value=str(water_data[water_key]),
        )
    return mappings


def build_explicit_source_mappings(
    source_mappings_raw: dict[str, object],
    carbon_data: dict[str, object],
    water_data: dict[str, object],
) -> dict[str, SourceMapping]:
    source_mappings: dict[str, SourceMapping] = {}
    for source_name, mapping_raw in source_mappings_raw.items():
        if not isinstance(mapping_raw, dict):
            raise ValueError(f"Mapping for source '{source_name}' must be a TOML table.")

        carbon_key = mapping_raw.get("carbon")
        water_key = mapping_raw.get("water")
        if not isinstance(carbon_key, str) or not isinstance(water_key, str):
            raise ValueError(f"Source '{source_name}' must define string 'carbon' and 'water' keys.")
        if carbon_key not in carbon_data:
            raise ValueError(f"Carbon intensity key '{carbon_key}' for source '{source_name}' does not exist.")
        if water_key not in water_data:
            raise ValueError(f"Water intensity key '{water_key}' for source '{source_name}' does not exist.")

        source_mappings[source_name] = SourceMapping(
            carbon_key=carbon_key,
            water_key=water_key,
            carbon_value=str(carbon_data[carbon_key]),
            water_value=str(water_data[water_key]),
        )
    return source_mappings


def load_input_dataframe(input_csv: Path) -> pd.DataFrame:
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV does not exist: {input_csv}")

    LOGGER.info("Loading normalized CSV from %s", input_csv)
    df = pd.read_csv(input_csv)
    required_columns = {
        "dataset_name",
        "country_code",
        "country_eic",
        "source_code",
        "source_name",
        "resolution_seconds",
        "interval_start_utc",
        "interval_end_utc",
        "generation_mw",
    }
    missing_columns = sorted(required_columns - set(df.columns))
    if missing_columns:
        raise ValueError(f"Input CSV is missing required columns: {', '.join(missing_columns)}")

    df["interval_start_utc"] = pd.to_datetime(df["interval_start_utc"], utc=True)
    df["interval_end_utc"] = pd.to_datetime(df["interval_end_utc"], utc=True)
    df["resolution_seconds"] = pd.to_numeric(df["resolution_seconds"], errors="raise").astype(int)
    df["generation_mw"] = pd.to_numeric(df["generation_mw"], errors="raise")
    LOGGER.info("Loaded %d normalized rows from %s", len(df), input_csv)
    return df


def derive_outputs(config: PipelineConfig, input_df: pd.DataFrame) -> list[OutputConfig]:
    if config.outputs:
        return config.outputs

    dataset_names = sorted(input_df["dataset_name"].dropna().astype(str).unique().tolist())
    if not dataset_names:
        raise ValueError("Input CSV contains no dataset names.")

    return [
        OutputConfig(
            dataset_name=dataset_name,
            output_csv=(config.config_path.parent / DEFAULT_TRACE_DATA_DIR / f"{dataset_name}_trace.csv").resolve(),
            host_id=DEFAULT_HOST_ID,
        )
        for dataset_name in dataset_names
    ]


def format_mix_scalar(value: float) -> str:
    return f"{MIX_SOURCE_NAME}:{value:.2f}"


def build_trace_rows(
    config: PipelineConfig,
    dataset_df: pd.DataFrame,
    output: OutputConfig,
    source_mappings: dict[str, SourceMapping],
) -> list[dict[str, object]]:
    if dataset_df.empty:
        raise ValueError(f"Dataset '{output.dataset_name}' contains no rows in the input CSV.")

    if dataset_df["country_code"].nunique(dropna=False) != 1:
        raise ValueError(f"Dataset '{output.dataset_name}' contains more than one country.")

    country_code = str(dataset_df["country_code"].iloc[0])
    timezone_name = resolve_country_timezone_name(country_code)
    dataset_timezone = ZoneInfo(timezone_name)
    week_start_local = datetime.fromisocalendar(config.year, config.week, 1).replace(tzinfo=dataset_timezone)
    dataset_df = dataset_df.copy()
    dataset_df["local_interval_start"] = dataset_df["interval_start_utc"].dt.tz_convert(dataset_timezone)
    dataset_df["local_interval_end"] = dataset_df["interval_end_utc"].dt.tz_convert(dataset_timezone)

    negative_rows = dataset_df["generation_mw"] < 0
    if negative_rows.any():
        for row in dataset_df.loc[negative_rows].itertuples():
            LOGGER.warning(
                "Clamping negative generation to zero for dataset '%s', source '%s', interval '%s', value=%s",
                output.dataset_name,
                row.source_name,
                row.local_interval_start.isoformat(),
                row.generation_mw,
            )
        dataset_df.loc[negative_rows, "generation_mw"] = 0.0

    base_resolution_seconds = int(dataset_df["resolution_seconds"].min())
    if base_resolution_seconds <= 0:
        raise ValueError(f"Dataset '{output.dataset_name}' has a non-positive resolution.")
    base_resolution = timedelta(seconds=base_resolution_seconds)

    expanded_rows: list[dict[str, object]] = []
    for row in dataset_df.itertuples():
        row_resolution = timedelta(seconds=int(row.resolution_seconds))
        repeated_steps_float = row_resolution.total_seconds() / base_resolution.total_seconds()
        repeated_steps = max(1, int(math.ceil(repeated_steps_float)))
        if not repeated_steps_float.is_integer():
            LOGGER.warning(
                "Dataset '%s' source '%s' interval '%s' has resolution %ss that is not a multiple of the base resolution %ss. Repeating with ceil semantics.",
                output.dataset_name,
                row.source_name,
                row.local_interval_start.isoformat(),
                row.resolution_seconds,
                base_resolution_seconds,
            )
        for step in range(repeated_steps):
            expanded_rows.append(
                {
                    "source_name": row.source_name,
                    "source_code": row.source_code,
                    "local_interval_start": row.local_interval_start + step * base_resolution,
                    "generation_mw": float(row.generation_mw),
                }
            )

    if not expanded_rows:
        raise ValueError(f"Dataset '{output.dataset_name}' contains no expanded rows.")

    expanded_df = pd.DataFrame(expanded_rows)
    expanded_df = (
        expanded_df.groupby(["source_name", "source_code", "local_interval_start"], dropna=False, as_index=False)["generation_mw"]
        .sum()
    )

    source_order_frame = (
        expanded_df[["source_name", "source_code"]]
        .drop_duplicates()
        .sort_values(["source_code", "source_name"], kind="stable")
    )

    energy_df = expanded_df.pivot(index="local_interval_start", columns="source_name", values="generation_mw")
    energy_df = energy_df.sort_index().fillna(0.0)

    active_sources = [source for source in source_order_frame["source_name"].tolist() if energy_df[source].sum() > 0]
    if not active_sources:
        raise ValueError(f"Dataset '{output.dataset_name}' contains no active sources after filtering zero-only columns.")

    missing_mappings = [source for source in active_sources if source not in source_mappings]
    if missing_mappings:
        missing = ", ".join(sorted(missing_mappings))
        raise ValueError(f"Dataset '{output.dataset_name}' has unmapped active sources: {missing}")

    final_index = pd.date_range(
        start=week_start_local,
        end=energy_df.index.max() + base_resolution,
        freq=pd.Timedelta(seconds=base_resolution_seconds),
        tz=dataset_timezone,
        inclusive="left",
    )
    energy_df = energy_df.reindex(final_index, fill_value=0.0)
    energy_df = energy_df[active_sources]

    rows: list[dict[str, object]] = []
    rows.append(
        {
            "timestamp": 0,
            "host_id": output.host_id,
            "property_name": "energy_mix",
            "new_value": format_mix_scalar(100.0),
        }
    )

    for local_timestamp, row in energy_df.iterrows():
        timestamp = int((local_timestamp.to_pydatetime() - week_start_local).total_seconds())
        total_generation = float(row.sum())
        if total_generation <= 0:
            LOGGER.warning(
                "Dataset '%s' has non-positive total generation at local interval '%s'. Exporting zero effective intensities.",
                output.dataset_name,
                local_timestamp.isoformat(),
            )
            carbon_intensity = 0.0
            water_intensity = 0.0
        else:
            carbon_intensity = 0.0
            water_intensity = 0.0
            for source in active_sources:
                generation = float(row[source])
                if generation <= 0:
                    continue
                weight = generation / total_generation
                carbon_intensity += weight * float(source_mappings[source].carbon_value)
                water_intensity += weight * float(source_mappings[source].water_value)

        rows.append(
            {
                "timestamp": int(timestamp),
                "host_id": output.host_id,
                "property_name": "carbon_intensity",
                "new_value": format_mix_scalar(round(carbon_intensity, 2)),
            }
        )
        rows.append(
            {
                "timestamp": int(timestamp),
                "host_id": output.host_id,
                "property_name": "water_intensity",
                "new_value": format_mix_scalar(round(water_intensity, 2)),
            }
        )
    return rows


def write_trace(output_csv: Path, rows: list[dict[str, object]]) -> None:
    output_csv.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(output_csv, index=False, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')


def run_fetch_stage(config: PipelineConfig) -> None:
    LOGGER.info(
        "Starting fetch stage for ISO week %d-W%02d with %d dataset(s)",
        config.year,
        config.week,
        len(config.extracts),
    )
    api_token = require_api_token()
    country_eic_map = fetch_country_eic_map()

    all_rows: list[dict[str, object]] = []
    for extract in config.extracts:
        country_eic = country_eic_map.get(extract.country_code)
        if country_eic is None:
            raise RuntimeError(f"Could not resolve an ENTSO-E country EIC for ISO code '{extract.country_code}'.")
        all_rows.extend(fetch_generation_rows(extract=extract, config=config, country_eic=country_eic, api_token=api_token))

    write_normalized_rows(config.normalized_csv, all_rows)
    LOGGER.info("Wrote %d rows to %s", len(all_rows), config.normalized_csv)


def run_export_stage(config: PipelineConfig) -> None:
    LOGGER.info("Starting export stage from %s", config.normalized_csv)
    input_df = load_input_dataframe(config.normalized_csv)
    outputs = derive_outputs(config, input_df)
    LOGGER.info("Exporting %d dataset trace(s)", len(outputs))

    for output in outputs:
        dataset_df = input_df[input_df["dataset_name"] == output.dataset_name].copy()
        if dataset_df.empty:
            raise ValueError(f"Configured dataset '{output.dataset_name}' does not exist in {config.normalized_csv}.")
        LOGGER.info("Building trace for dataset '%s' -> %s", output.dataset_name, output.output_csv)
        rows = build_trace_rows(config, dataset_df, output, config.source_mappings)
        write_trace(output.output_csv, rows)
        LOGGER.info("Wrote %d rows to %s", len(rows), output.output_csv)


def main() -> None:
    configure_logging()
    args = parse_args()
    config = load_config(Path(args.config).resolve())
    LOGGER.info(
        "Loaded config %s, fetch=%s, export=%s, week=%d-W%02d",
        config.config_path,
        config.run_fetch,
        config.run_export,
        config.year,
        config.week,
    )

    if config.run_fetch:
        run_fetch_stage(config)
    if config.run_export:
        run_export_stage(config)


if __name__ == "__main__":
    main()
