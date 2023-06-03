import glob

import polars as pl

from alltime_athletics_viz.pipes import (
    pipe_assign_file_name,
    pipe_assign_has_hurdles_or_not,
    pipe_assign_sprint_middle_long_distance,
    pipe_assign_track_event_or_not,
    pipe_convert_time_to_seconds,
    pipe_drop_unwanted_columns,
    pipe_fix_dtype,
    pipe_fix_issue_with_half_marathon_distance,
    pipe_get_event_distance,
    pipe_get_percentage_wrt_world_record,
    pipe_get_wr_strength_by_comparing_with_tenth,
    pipe_remove_all_null_columns,
    pipe_remove_invalid,
    pipe_rename_columns_names,
)


def import_men_running_only_events() -> pl.DataFrame:
    """So far only works with men's legal standard marks."""
    return pl.concat(
        [
            pl.read_csv(event, infer_schema_length=10000)
            .pipe(pipe_assign_file_name, event)
            .pipe(pipe_rename_columns_names)
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
            .pipe(pipe_drop_unwanted_columns)
            .pipe(pipe_get_wr_strength_by_comparing_with_tenth)
            for event in get_men_running_only_files()
        ],
        how="diagonal",
    )


def get_men_running_only_files() -> list[str]:
    return [
        f
        for f in glob.glob(  # Get all csv files recursively.
            "../data/men/standard/*/legal/0*.csv", recursive=True
        )
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
