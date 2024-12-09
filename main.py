from datetime import datetime
import json
from pathlib import Path
import shutil
import typing

import pandas as pd


POC: Path
"""Pre-pended to all of the configs ending with `_PATH`
    to point to the correct path for inputs/outputs for each of the pocs."""


INPUT_BANK_ACCOUNTS_DATA_PATH: typing.Final = Path("./inputs/bankaccounts.json")
INPUT_EMPLOYEE_DATA_PATH: typing.Final = Path("./inputs/ee_data.json")
INPUT_PAYRUN_DATA_PATH: typing.Final = Path("./inputs/payrun_data.json")
INPUT_PAYRUN_MAPPING_PATH: typing.Final = Path("./mapping.json")
INPUT_PAYRUN_XLSX_PATH: typing.Final = Path("./inputs/payrun_file.xlsx")


OUT_DATE_FORMAT: typing.Final = "%d.%m.%Y"  # being just '%y' breaks work start dates at year 1900
OUT_TEMPLATE_PATH: typing.Final = Path("./template/TEST PAYROLL INSTRUCTIONS 12 2022 NEW Employee Form.xlsx")
OUT_RESULT_PATH: typing.Final = Path("./Employee Form.xlsx")

class OutLocation(typing.TypedDict):
    """The `sheet` and top-left cell coordinates, where a given `DataFrame` would be written in some Excel file."""
    sheet: str
    row: int
    col: int
OUT_EMPLOYEES_INFO_LOCATION: typing.Final[OutLocation] = {"sheet": "Employees", "row": 0, "col": 1}
OUT_EMPLOYEES_GREEN_LOCATION: typing.Final[OutLocation] = {"sheet": "Employees", "row": 5, "col": 0}
OUT_EMPLOYEES_RED_LOCATION: typing.Final[OutLocation] = {"sheet": "Employees", "row": 5, "col": 14}
OUT_EMPLOYEE_FORM_LOCATION: typing.Final[OutLocation] = {"sheet": "Employee form", "row": 2, "col": 0}


def read_bank_accounts(*, employee_ids: pd.Index, payrun_meta: dict[str, typing.Any]) -> pd.DataFrame:
    """Read the `bankaccounts.json` file."""
    def select_row_from_duplicates(rows: pd.DataFrame) -> pd.DataFrame:
        if len(rows) == 1:
            return rows
        # NOTE: would also include other filters / bussiness logic, like e.g.:
        #   - rows["bankCountry"] == payrun_meta["countryAbbr"]
        #   - rows["primaryBankAccount"] == True
        # but no clue what the actual requirements are and don't want to spend time just imagining something
        rows = rows[~rows["iban"].isna()]
        return rows

    df_ba = pd.read_json(POC.joinpath(INPUT_BANK_ACCOUNTS_DATA_PATH)).set_index("employee")
    df_ba = df_ba[(df_ba.index.isin(employee_ids)) & (df_ba["company"] == payrun_meta["companyId"])]
    df_ba = df_ba.groupby(df_ba.index.name, sort=False).apply(select_row_from_duplicates).droplevel(level=0)
    return df_ba.reindex(employee_ids, axis="index").sort_index()


def read_employees(*, employee_ids: pd.Index, payrun_meta: dict[str, typing.Any]) -> pd.DataFrame:
    """Read the `ee_data.json` file."""
    # NOTE: from what I can see, filters (including thiese two) are not needed
    # with the provided test-data, in practice - possibly more filters?
    def in_company(companies: pd.Series) -> pd.Series:
        return pd.Series(
            index = companies.index,
            data = [
                ref['id'] == payrun_meta["companyId"]
                for ref in companies
            ],
        )

    def in_country(countries: pd.Series) -> pd.Series:
        return pd.Series(
            index = countries.index,
            data=[
                # because I didn't see 'country_code' in 'payrun_data.json':
                ref['name'] == payrun_meta["country"] and ref["abbreviature"] == payrun_meta["countryAbbr"]
                for ref in countries
            ]
        )

    df_ee = pd.read_json(POC.joinpath(INPUT_EMPLOYEE_DATA_PATH)).set_index("id").filter(employee_ids, axis="index")
    df_ee = df_ee[
        (in_company(df_ee['companyRef'])) & (in_country(df_ee['countryRef']))
    ]
    df_ee.index.set_names("id", inplace=True)
    return df_ee.sort_index()


def read_payrun_data() -> tuple[dict[str, typing.Any], pd.DataFrame, pd.DataFrame]:
    """Read the `payrun_data.json` file.

    :return: a `tuple` of three elements:
        - the payrun's metadata as a `dict`
        - the payrun's data as a `pd.DataFrame`
        - the payrun's global mappings as a `pd.DataFrame`
    """
    # TODO: this seems a non-optimal approach:
    with open(POC.joinpath(INPUT_PAYRUN_DATA_PATH), 'r') as f:
        payrun_data: dict[str, typing.Any] = json.load(f)
    df_pr_data = pd.DataFrame(payrun_data.pop("payrun_data"))
    df_pr_map = pd.DataFrame(payrun_data.pop("mappings"))
    return payrun_data, df_pr_data, df_pr_map


def read_payrun_data_mapping() -> pd.DataFrame:
    """Read the `mapping.json` file.

    Create an empty scaffold if the file doesn't already exist and
    proceed with implicitly empty `globalName` (e.g. `null`, `""`) for all of the `templateName`s.
    Filling some/all of the mappings in the generated file and re-running this script will fill the output columns.

    Mappings for `templateName`s that don't exist as columns in the current template are completely ignorred.

    Duplicate mappings, each defining a different `globalName` for the same (existing) `templateName` are allowed,
    but only the `globalName` of the last mapping parsed from the file is used.

    Missing mappings for one or more `templateName`s implicitly specify an empty `globalName` (e.g. `null`, `""`)
    for that `templateName` and will just show up in the result as `None` values. Mappings explicitly specifying
    an empty `globalName` (e.g. `null`, `""`) show up in the result the same way.
    """
    mapping_path = POC.joinpath(INPUT_PAYRUN_MAPPING_PATH)

    col_names = pd.read_excel(
        POC.joinpath(OUT_TEMPLATE_PATH),
        sheet_name=OUT_EMPLOYEES_INFO_LOCATION["sheet"],
        # TODO: I suppose following three args can be more dynamic:
        usecols='O:Y',
        header=4,
        nrows=1,
    ).columns

    if mapping_path.exists():
        df_pr_map = (pd
            .read_json(mapping_path)
            .drop_duplicates(subset="templateName", keep="last")
            .set_index("templateName")
            .filter(col_names, axis="index")
        )
    else:
        template = [{"globalName": None, "templateName": col} for col in col_names]
        with open(mapping_path, 'w') as f:
            json.dump(template, f, indent=4)
        df_pr_map = pd.DataFrame(template)

    # TODO: calc for this can be more dynamic:
    df_pr_map["colPos"] = range(ord("O") - ord("A"), ord("Y") - ord("A") + 1)

    return df_pr_map


def read_payrun_excel() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Read the 4 tabs/pages of the `payrun_file.xlsx` file, each in its own `DataFrame`."""
    pr_xls_file = pd.ExcelFile(POC.joinpath(INPUT_PAYRUN_XLSX_PATH))
    main = pd.read_excel(pr_xls_file, "Payrun file",  header=1)
    main = main.loc[~main['System Employee ID'].isna()].set_index('System Employee ID')
    changes = pd.read_excel(pr_xls_file, "P&T Changes",  header=1)
    notes = pd.read_excel(pr_xls_file, "Notes",  header=0)
    updates = pd.read_excel(pr_xls_file, "General updates",  header=0)
    return main.sort_index(), changes, notes, updates


def create_out_employees_info(payrun_meta: dict[str, typing.Any]) -> pd.DataFrame:
    """Data for the top-left cells (column 'B') in the `Employees` sheet in the Excel output."""
    keys = ("company", "payDate", "month")
    values = []
    for k in keys:
        v = payrun_meta.get(k)
        if v is None:
            raise KeyError(f"Could not find required key '{k}' in file '{POC.joinpath(INPUT_PAYRUN_DATA_PATH)}'")
        values.append(v)
    values[1] = datetime.strptime(payrun_meta["payDate"], "%d/%b/%Y").strftime(OUT_DATE_FORMAT)
    return pd.DataFrame(values)


def create_out_employees_green(df_pr_xls_main: pd.DataFrame, df_ee: pd.DataFrame, df_ba: pd.DataFrame) -> pd.DataFrame:
    """Data for the green columns in the `Employees` sheet in the Excel output."""
    df = pd.DataFrame([
        df_pr_xls_main.index,
        df_ee["fullname"],
        df_ee["position"],
        df_pr_xls_main["Department"],  # or from df_ee["department"], or df_ee["department"] as a fallback?
        df_ba["accountNumber"],
        df_ba["iban"],
        df_ee["workType"],  # 'Percentage of employment'?
        df_pr_xls_main["Annual Salary"],
        pd.to_datetime(df_pr_xls_main["Effective Date"]).dt.strftime(OUT_DATE_FORMAT),  # 'Start date for new yearly salary'?
        df_pr_xls_main.filter(like="Car Allowance", axis=1).iloc[:, 0],
        df_pr_xls_main.filter(like="Car Value", axis=1).iloc[:, 0],
        df_ee["rgNumber"],  # 'Registration Number'? - couldn't find anything else
        df_ee["startDate"].apply(lambda x:
            None if x["date"] is None
            else datetime.fromisoformat(x["date"]).date().strftime(OUT_DATE_FORMAT)  # validate 'not after current payrun'?
        ),
        pd.Series([None] * len(df_pr_xls_main.index)),  # no clue what the 'Comments' col is supposed to be
    ]).T.set_axis(df_pr_xls_main.index, axis="index")
    return df.sort_index()


def create_out_employees_red(
    df_pr_xls_main: pd.DataFrame,
    df_pr_data: pd.DataFrame,
    df_pr_map: pd.DataFrame,
    df_pr_map_local: pd.DataFrame,
) -> pd.DataFrame:
    """Data for the red columns in the `Employees` sheet in the Excel output."""
    # NOTE: will probably need to parse the 'pr_data["elements"]' as a separate DataFrame to read the fields for each employee
    # NOTE: do we actually need the global mapping "df_pr_map"? what for?
    return pd.DataFrame()


def create_out_employee_form() -> pd.DataFrame:
    """Data for the `Employee form` sheet in the Excel output."""
    # NOTE: Absolutely no idea what the logic for this sheet should be.
    # instructions.txt says 'payrun_data.xlsx' - if that's supposed to be:
    #   - 'payrun_data.json' -> where are changes of any kind specified in it?
    #   - 'payrun_file.xlsx' -> assuming the 'P&T Changes' sheet should be used,
    #     how exaclty are the 'Pay elements' changes in it supposed to be the core of what will be written here?
    return pd.DataFrame()


def get_excel_writer() -> pd.ExcelWriter:
    """A WriteHandle for the file spcified by `OUT_RESULT_PATH`.

    Creates the initial scaffold using the file specified by `OUT_TEMPLATE_PATH`.
    """
    dst = POC.joinpath(OUT_RESULT_PATH)
    shutil.copyfile(POC.joinpath(OUT_TEMPLATE_PATH), dst)
    return pd.ExcelWriter(
        dst,
        engine="openpyxl",
        mode="a",
        if_sheet_exists="overlay",
        engine_kwargs={
            # "keep_vba": True,
            # "rich_text": True,
        },
    )


def write_to_excel(w: pd.ExcelWriter, df: pd.DataFrame, loc: OutLocation) -> None:
    """Write `df` to `loc`, using `w`."""
    df.to_excel(
        w,
        index=False,
        header=False,
        sheet_name=loc["sheet"],
        startrow=loc["row"],
        startcol=loc["col"],
    )


def do_poc():
    df_pr_xls_main, _, _, _ = read_payrun_excel()

    payrun_meta, df_pr_data, df_pr_map = read_payrun_data()
    df_pr_map_local = read_payrun_data_mapping()

    df_ba = read_bank_accounts(
        employee_ids=df_pr_xls_main.index,
        payrun_meta=payrun_meta,
    )
    df_ee = read_employees(
        employee_ids=df_pr_xls_main.index,
        payrun_meta=payrun_meta,
    )

    out_employees_info = create_out_employees_info(payrun_meta)
    out_employees_green = create_out_employees_green(df_pr_xls_main, df_ee, df_ba)
    out_employees_red = create_out_employees_red(df_pr_xls_main, df_pr_data, df_pr_map, df_pr_map_local)
    out_employee_form = create_out_employee_form()

    with get_excel_writer() as w:
        write_to_excel(w, out_employees_info, OUT_EMPLOYEES_INFO_LOCATION)
        write_to_excel(w, out_employees_green, OUT_EMPLOYEES_GREEN_LOCATION)
        write_to_excel(w, out_employees_red, OUT_EMPLOYEES_RED_LOCATION)
        write_to_excel(w, out_employee_form, OUT_EMPLOYEE_FORM_LOCATION)


if __name__ == "__main__":
    # NOTE: the "special logic for the column Position" for "poc_2" is not done.
    # for p in ("./poc_1", "./poc_2"):
    for p in ("./poc_1",):
        POC = Path(p)
        do_poc()
