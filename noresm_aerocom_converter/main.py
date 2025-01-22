import typer
from typing_extensions import Annotated
from typing import List, Optional
from enum import Enum
from xarray import open_mfdataset, Dataset
import yaml
from pathlib import Path
from datetime import datetime
import importlib.resources
from os.path import dirname, join as joinpath

from rich.console import Console
from rich.table import Table

# from conversion_instructions import get_conversion_intstructions
console = Console()
app = typer.Typer(
    help="Small tool for converting NorESM modeldata to Aerocom3 modeldata"
)


AVAILABLEMONTHS = [
    "01",
    "02",
    "03",
    "04",
    "05",
    "06",
    "07",
    "08",
    "09",
    "10",
    "11",
    "12",
]

# PERIOD=9999
# PERIOD=1850
CONSTANTS = dict(
    #LL=31,
    # converts from sulfuric acid (H2SO4) to SO4 (96/98 MW)
    SF1="0.9796",
    # converts from ammonium sulfate (NH4_2SO4) to SO4 (96/134 MW)
    SF2="0.7273",
    # mass fraction of DST_A3 for d>10 um (from AeroTab, assuming no growth))
    F10DSTA3="0.23",
    # mass fraction of SS_A3 for d>10 um (from AeroTab, assuming no growth))
    F10SSA3="0.008",
    # Rair
    RAIR="287.0",
    # yaml file used for conversion commands
)
FREQUENCY = "monthly"


data_dir = joinpath(dirname(__file__), "data")
YAML_FILE = Path(data_dir, "conversions.yaml")
YAML_FILE_RAW = Path(data_dir, "conversions_raw.yaml")


class Level(str, Enum):
    M = ("ModelLevel",)
    S = ("Surface",)
    C = ("Column",)
    SS = ("SurfaceAtStations",)
    MS = ("ModelLevelAtStations",)
    default = ("INVALIDCOORDINATETYPE",)


def _get_file_list(
    inputdir: str, experiment: str, years: List[str], raw: bool
) -> dict[str, list[str]]:
    files = {}
    for year in years:
        file_year = []
        for month in range(1, 13):
            folder = Path(inputdir)
            if folder.is_dir():
                filenames = folder.glob(f"{experiment}.cam.h0a.{year}-{month:02}.nc") if raw else folder.glob(f"{experiment}.cam.h0.{year}-{month:02}.nc")
                #filenames = folder.glob(f"{experiment}.cam.h0*.{year}-{month:02}.nc")
                for full_name in filenames:
                    # full_name = f"{inputdir}/{experiment}.cam.h0*.{year}-{month:02}.nc"
                    if Path(full_name).exists():
                        file_year.append(
                            # f"{inputdir}/{experiment}.cam.h0*.{year}-{month:02}.nc"
                            full_name
                        )
                    else:
                        continue
            else:
                raise ValueError(f"Folder {inputdir} does not exist")

        if not file_year: #TEMPORARY FIX AS SOME RAW CASES HAVE DIFFERENT FILECODES
            for month in range(1,13):
                if folder.is_dir():
                    filenames = folder.glob(f"{experiment}.cam.h0*.{year}-{month:02}.nc")
                    for full_name in filenames:
                        if Path(full_name).exists():
                            file_year.append(
                                # f"{inputdir}/{experiment}.cam.h0*.{year}-{month:02}.nc"
                                full_name
                            )
                        else:
                            continue

                else:
                    raise ValueError(f"Folder {inputdir} does not exist")

        files[year] = file_year

    return files


def _fill_in_constants(formula: str, ll: int) -> str:
    to_be_filled = {}
    if "LL" in formula:
        to_be_filled["LL"] = ll
    for key in CONSTANTS:
        if key in formula:
            to_be_filled[key] = CONSTANTS[key]
    formula = formula.format(**to_be_filled)
    return formula


def _open_year_dataset(files: list[str]) -> Dataset:
    data = open_mfdataset(paths=files, decode_times=False)
    return data


def _make_aerocom_dataset(
    data: Dataset,
    variable: str,
    instruction: dict[str, str],
    year: str,
    ll: int,
) -> Dataset | None:
    filled_formula = _fill_in_constants(instruction["formula"], ll)
    command = f"data.assign({variable} = lambda x: {filled_formula})"
    try:
        new_data = eval(command)
    except Exception as e:
        print(f"Could not due conversion for {variable} due to {str(e)}")
        return
    try:
        new_data = new_data[[variable, "time", "time_bnds", "lat", "lon"]]
    except:
        new_data = new_data[[variable, "time", "time_bounds", "lat", "lon"]]
    new_data[variable].attrs["units"] = instruction["units"]
    try:
        new_data.time.attrs["units"] = data.time.attrs["units"]
    except:
        print(f"Warning: Could not find time units in NorESM file, used baseyear {year} instead")
        new_data.time.attrs["units"] = f"days since {year}-01-01 00:00:00"

    new_data.attrs["converted at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return new_data


def save_aerocom_data(
    data: Dataset, outdir: str, fullname: str, aerocomname: str, level: str, year: str
):
    out_file = (
        f"{outdir}/aerocom3_{fullname}_{aerocomname}_{level}_{year}_{FREQUENCY}.nc"
    )
    console.print(f"Saving file to {out_file}")
    data.to_netcdf(out_file)


def get_conversion_yaml(raw: bool) -> dict[str, dict[str, str]]:
    filename = YAML_FILE_RAW if raw else YAML_FILE
    with open(filename, "r") as f:
        instructions = yaml.safe_load(f)

    return instructions


def _convert(
    inputdir: str,
    outputdir: str,
    experiment: str,
    fullname: str,
    baseyear: int,
    years: List[str],
    ll: int,
    variables: Optional[List[str]] = None,
    raw: bool = False,
    dry_run: bool = False,
) -> None:
    for i, year in enumerate(years):

        years[i] = f"{int(year):04}"
    instructions = get_conversion_yaml(raw=raw)  # get_conversion_intstructions(LL)
    if variables is None:
        variables = list(instructions.keys())
    files = _get_file_list(inputdir, experiment, years, raw)
    for year in files:
        console.print(f"Converting for year {year}, with reference year {baseyear}")
        data = _open_year_dataset(files[year])
        for var in instructions:
            if var in variables:
                new_var = instructions[var]["new_name"]
                new_data = _make_aerocom_dataset(
                    data, new_var, instructions[var], f"{baseyear:04}", ll
                )
                if new_data is None:
                    print(f"Failed to make {var}. Continuing")
                    continue

                if dry_run:
                    console.print(f"Successfully made {var}. Won't save!")
                    continue

                save_aerocom_data(
                    new_data,
                    outputdir,
                    fullname,
                    new_var,
                    instructions[var]["coordinates"],
                    f"{baseyear + int(year):04}",
                )


@app.command(help="Converts modeldata according to arguments and options given in file")
def from_file(path: Annotated[str, typer.Argument(rich_help_panel="Path to ")]):
    if Path(path).exists():
        with open(path, "r") as f:
            arguments = yaml.safe_load(f)

        _convert(**arguments)


@app.command(help="Converts modeldata according to given arguments and options")
def convert(
    inputdir: Annotated[str, typer.Argument(rich_help_panel="Input Directory")],
    outputdir: Annotated[str, typer.Argument(rich_help_panel="Output directory")],
    experiment: Annotated[str, typer.Argument(rich_help_panel="Experiment Name")],
    fullname: Annotated[str, typer.Argument(rich_help_panel="Full Name")],
    baseyear: Annotated[int, typer.Argument(rich_help_panel="Reference Year")],
    years: Annotated[List[str], typer.Argument(rich_help_panel="Years")],
    ll: Annotated[
        int,
        typer.Argument(rich_help_panel="Vertical Level (used for some conversions)"),
    ],
    variables: Annotated[
        Optional[List[str]],
        typer.Option(
            rich_help_panel="Which variables to convert. If non is given, then everything is converted"
        ),
    ],
    raw: Annotated[bool, typer.Option(rich_help_panel="If true NAC assumes raw noresm files, and uses conversion_raw to convert. If false, CMORE is assumed, and conversions is used.")] = False,
    dry_run: Annotated[
        bool,
        typer.Option(rich_help_panel="Does all the conversions, but doesn't save."),
    ] = False,
) -> None:
    _convert(
        inputdir,
        outputdir,
        experiment,
        fullname,
        baseyear,
        years,
        ll,
        variables,
        raw,
        dry_run,
    )


@app.command(
    help="List all possible chemical species that are defined in conversion.yaml, and thus can be converted"
)
def list_species(
    species: Annotated[
        Optional[List[str]],
        typer.Argument(
            rich_help_panel="Print information for single species. If non given, all possible species are listed"
        ),
    ] = None,
    raw: Annotated[bool, typer.Option(rich_help_panel="If true NAC assumes raw noresm files, and uses conversion_raw to convert. If false, CMORE is assumed, and conversions is used.")] = False,
):
    instruction = get_conversion_yaml(raw=raw)
    species_list = sorted(instruction.keys())

    if len(species) > 0:
        species_used = []
        for s in species:
            if s in species_list:
                species_used.append(s)
        species_list = species_used[:]

    table = Table("Species", "Unit", "Coordinates", "Formula")
    for s in species_list:
        table.add_row(
            s,
            instruction[s]["units"],
            instruction[s]["coordinates"],
            instruction[s]["formula"],
        )

    console.print(table)


if __name__ == "__main__":
    app()
