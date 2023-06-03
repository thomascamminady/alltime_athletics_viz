import glob

import polars as pl

from alltime_athletics_viz.pipes import (
    pipe_assign_has_hurdles_or_not,
    pipe_assign_sprint_middle_long_distance,
    pipe_assign_track_event_or_not,
    pipe_convert_time_to_seconds,
    pipe_fix_dtype,
    pipe_fix_issue_with_half_marathon_distance,
    pipe_get_event_distance,
    pipe_get_percentage_wrt_world_record,
    pipe_remove_all_null_columns,
    pipe_remove_invalid,
    pipe_rename_columns_mean,
)


def get_running_only(files: list[str]) -> list[str]:
    return [
        f
        for f in files
        if (
            "metres" in f
            or "00m" in f
            or "Mile" in f
            or "One hour" in f
            or "km" in f
            or "marathon" in f
            or "110m" in f
        )
        and ("4x" not in f)
    ]


def get_df_list(files) -> list[pl.DataFrame]:
    return [
        pl.read_csv(file, infer_schema_length=10000).with_columns(
            [
                pl.lit(file.split("/")[4]).alias("event"),
                pl.lit(file).alias("file"),
            ]
        )
        for file in files
    ]


def rename_df_list_elements(df_list: list[pl.DataFrame]) -> list[pl.DataFrame]:
    return [df.pipe(pipe_rename_columns_mean) for df in df_list]


def parse_running_events() -> pl.DataFrame:
    """So far only works with men's legal standard marks."""
    return (
        pl.concat(
            rename_df_list_elements(  # Rename columns in data frames.
                get_df_list(  # Turn list of csv files into list of data frames.
                    get_running_only(  # Remove all non running csv files.
                        glob.glob(  # Get all csv files recursively.
                            "../data/men/standard/*/legal/0*.csv",
                            recursive=True,
                        )
                    )
                )
            ),
            how="diagonal",
        )
        .pipe(pipe_fix_dtype)
        .pipe(pipe_convert_time_to_seconds)
        .pipe(pipe_remove_all_null_columns)
        .pipe(pipe_remove_invalid)
        .pipe(pipe_get_event_distance)
        .pipe(pipe_get_percentage_wrt_world_record)
        .pipe(pipe_assign_sprint_middle_long_distance)
        .pipe(pipe_assign_has_hurdles_or_not)
        .pipe(pipe_assign_track_event_or_not)
        .pipe(pipe_fix_issue_with_half_marathon_distance)
        .drop(columns=["3", "valid"])
    )
