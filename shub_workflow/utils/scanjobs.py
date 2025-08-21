"""
Utility for scan, extract and prints logs, spider arguments, stats and items on target spiders/scripts using regex
patterns. It also has the capability to post process the extracted data (see --post-process option) and generate plots
from them (see --plot option). Plotting requires pandas, seaborn and matplotlib libraries.

It can generate regex pattern groups, post process them via simple post-script like language,
save in order to generate data tables and generate plots.

If you use --spider-argument-pattern, matches from provided logs, stats and items patterns are limited to those jobs
that matches any of the provided spider argument patterns.

By default, the scan period is the las 1 day. See --period option.

By default, each time a new match is found, it is printed in the console and the search pauses waiting for
pressing Enter. This mode is useful for visual inspection. This behavior can be modified via the --write
option or the --plot option.

--write is useful for generating big amount of data for further analysis or generating data tables (in
combination with regex groups and stat values). With this option, data is written into a json list file, each line
being the data extracted from a single match.

As usual in shub-workflows when you run them outside SC, you need to include the --project-id in order
to set the correct target project where to find the jobs.

Examples
========

1. Searches for log pattern 'youtube.+?always_retriable_rate' in jobs for script "py:deliver.py":

       > python scanjobs.py --project-id=production py:deliver.py -l 'youtube.+?always_retriable_rate'

2. Searches for log pattern 'youtube.+?always_retriable_rate": (\\d+\\.\\d+)' in jobs for script "py:deliver.py",
   and additionally prints the data extracted from regex groups defined in the pattern.

       > python scanjobs.py --project-id=production py:deliver.py -l 'youtube.+?always_retriable_rate": (\\d+\\.\\d+)'

3. Searches for the stats 'ipType'. 'records_read', 'unable_to_get_url/retries' in jobs of the spider "downloader"
   for which the spider argument "source" matches the pattern "douyin". The data extracted will be the regex group
   in 'ipType/(.+)', plus the value of the matching stats.

       > python scanjobs.py --project-id=production downloader -a source:douyin -s 'ipType/(.+)' -s records_read \\
       -s unable_to_get_url/retries

   Lets suppose that the data extracted on each match is like:

       ('datacenter', '11558', '2500', '9059')

   the first element corresponds to the matchin group of the 'ipType/(.+)' applied on the stat name. The second one is
   the value of that stat, and the third and fourth one are the value of the stats "records_read" and
   "unable_to_get_url/retries" respectively.

4. The same as example 3, but with post processing instructions:

       > python scanjobs.py --project-id=798547 downloader -a source:douyin -s 'ipType/(.+)' -s records_read \\
       -s unable_to_get_url/retries -p "3 -1 roll pop exch div"

   "3 -1 roll pop" discards the second element.
   "exch div" divides the last number over the second-last, consume boths and appends the result.

   The final effect of the instructions "3 -1 roll pop exch div" is to discard the second element, and divide
   the last by the second last. So a data line like this one:

       ('datacenter', '11558', '2500', '9059')

   will be converted into:

       ('datacenter', 3.6236)

5. Another more complex example:

       > python scanjobs.py --project-id=798547 downloader -a source:douyin -s 'ipType/(.+)' -s unable_to_get_url \\
        -s records_read -p "4 -1 roll pop dup 4 -1 roll exch div 3 1 roll div 1 add"

   Lets suppose that it matches these stats:

       {'ipType/datacenter': 5943, 'unable_to_get_url': 278, 'unable_to_get_url/retries': 5063, 'records_read': 880}

   So, the initial data generated is:

       ('datacenter', '5943', '278', '5063', '880')

   "4 -1 roll pop" discards the second element:

       ('datacenter', '278', '5063', '880')

   "dup" duplicates the last one:

       ('datacenter', '278', '5063', '880', '880')

   "4 -1 roll" rotates the last 4 elements 1 place left:

       ('datacenter', '5063', '880', '880', '278')

   "exch div" dives the last over the second last:

      ('datacenter', '5063', '880', 0.3159090909090909)

   "3 1 roll" rotates right the three last elements:

      ('datacenter', 0.3159090909090909, '5063', '880')

   And f"div 1 add" divides 5063 over 880 and adds 1, thus yielding the final result:

      ('datacenter', 0.3159090909090909, 6.7534090909090905))

postscript instructions supported:
----------------------------------

1. Binary operations. In all cases, pops both numbers and push the result to stack
    add - sum last two numbers in stack
    sub - substract the last two numbers in stack
    mul - multiply the last two numbers in stack
    div - divide the last two numbers in stack

2. stack manipulation and counting:

    dup - pushes the last element of the stack, so it becomes duplicated on the stack.
    pop - pops out the last element in stack
    roll - pops out last two elements in the stack (x, y), which are the parameters, and rolls the last x
           elements of the remaining stack, in the direction indicated by y: to the right if y = 1, to the left
           if y = -1
    exch - exchange possitions of the last two elements in the stack
    count - computes the size of the stack, and push the result into the stack

3. flow manipulation:

    repeat -

4. conversion:

    cvi - pops out the last element of the stack, converts to integer, and pushes it back to stack.

5. special (non postcript origin):

    hold - It is used to conserve the stack between extractions for the same job. So if for example you are
           extracting logs and stats, without hold the same post processing string will be applied (or tried to)
           on each extracted set separately. The hold instruction is consumed within the scan of a single job,
           and pushes the result of an extraction into the stack with no further post processing, so it will be
           processed along the next extraction in the same job, or until not another hold found. In short,
           it skips the post processing until next extraction, and conserves the stack.

    prune - conserves only the top n elements from the stack. Receives an integer as parameter.

======================================================================
"""

import re
import sys
import time
import json
import logging
import argparse
import datetime
import zoneinfo
import math
from uuid import uuid4
from typing import Iterator, Tuple, TypedDict, List, Iterable, Dict, Union, Optional
from itertools import chain

import dateparser
import jmespath
from timelength import TimeLength
from typing_extensions import NotRequired
from scrapinghub.client.jobs import Job
from shub_workflow.script import BaseScript, JobDict


LOG = logging.getLogger(__name__)


def json_serializer(obj):
    if isinstance(obj, datetime.datetime):
        return str(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


def post_process(instructions: Iterable[Union[str, int, float]]) -> List[Union[str, int, float]]:
    """
    >>> post_process(["stringA", 3, 4, "dup"])
    ['stringA', 3, 4, 4]
    >>> post_process(["stringA", 3, 4, "div"])
    ['stringA', 0.75]
    >>> post_process(["stringA", 3, 4, "pop", "pop"])
    ['stringA']
    >>> post_process(["stringA", 3, 4, "add"])
    ['stringA', 7.0]
    >>> post_process(["stringA", 3, 4, 3, 1, "roll"])
    [4, 'stringA', 3]
    >>> post_process(["stringA", 3, 4, 5, 3, 2, "roll"])
    ['stringA', 4, 5, 3]
    >>> post_process(["stringA", 3, 4, 5, 3, -2, "roll"])
    ['stringA', 5, 3, 4]
    >>> post_process(["stringA", 3, 4, 5, "exch"])
    ['stringA', 3, 5, 4]
    >>> post_process([4, 3, "sub"])
    [1.0]
    >>> post_process([4, 3, "mul"])
    [12.0]
    >>> post_process(["2025-04-08", "residential", "100", "30", "189", "3", "-1", "roll",
    ... "dup", "3", "1", "roll", "div", "3", "1", "roll", "div", "2", "1", "roll"])
    ['2025-04-08', 'residential', 0.3, 1.89]

    >>> post_process(["123", "cvi"])
    [123]

    >>> post_process(["1", "2", "3", "4", "div", "2", "prune"])
    ['2', 0.75]

    >>> post_process(["3", "4", "5", "2", "{", "add", "}", "repeat"])
    [12.0]

    Lets suppose we have the following series: ['431', '2138', '412', '216', '829', '195']
    lets divide 3 by sum of 0 and 3, 4 by sum of 1 and 4, 5 by sum of 2 and 5:
    >>> [216 / (431 + 216), 829 / (2138 + 829), 195 / (412 + 195)]
    [0.33384853168469864, 0.2794068082237951, 0.3212520593080725]

    How to achieve same result with postprocess commands?
    >>> post_process(['431', '2138', '412', '216', '829', '195', 3, 1, "roll", 4, 1, "roll",
    ... 5, 1, "roll", "dup", 3, 1, "roll", "add", "div", "count", 1, "roll",
    ... "dup", 3, 1, "roll", "add", "div", "count", 1, "roll",
    ... "dup", 3, 1, "roll", "add", "div", "count", 1, "roll"])
    [0.33384853168469864, 0.2794068082237951, 0.3212520593080725]

    Notice the 3 times repetition of ["dup", 3, 1, "roll", "add", "div", "count", 1, "roll"]
    The above can be simplified as:
    >>> post_process(['431', '2138', '412', '216', '829', '195', 3, 1, "roll", 4, 1, "roll",
    ... 5, 1, "roll", 3, "{", "dup", 3, 1, "roll", "add", "div", "count", 1, "roll", "}", "repeat"])
    [0.33384853168469864, 0.2794068082237951, 0.3212520593080725]
    """

    stack: List[Union[str, int, float]] = []
    repeat_level = 0

    for ins in instructions:
        if ins == "repeat":
            assert stack.pop() == "}", "invalid syntax for repeat"
            repeat_list: List[Union[str, int, float]] = []
            try:
                while (e := stack.pop()) != "{":
                    repeat_list.insert(0, e)
            except IndexError:
                raise SyntaxError("Unclosed }")
            num_repeats = int(stack.pop())
            for _ in range(num_repeats):
                stack = post_process(stack + repeat_list)
            continue
        if ins == "{":
            repeat_level += 1
        elif ins == "}":
            repeat_level -= 1
        if repeat_level > 0:
            stack.append(ins)
        elif ins == "dup":
            stack.append(stack[-1])
        elif ins == "pop":
            stack.pop()
        elif ins == "add":
            stack.append(float(stack.pop()) + float(stack.pop()))
        elif ins == "mul":
            stack.append(float(stack.pop()) * float(stack.pop()))
        elif ins == "div":
            denom = float(stack.pop())
            num = float(stack.pop())
            stack.append(num / denom)
        elif ins == "roll":
            places = int(stack.pop())
            length = int(stack.pop())
            head, tail = stack[:-length], stack[-length:]
            stack = head + tail[-places:] + tail[:-places]
        elif ins == "exch":
            a = stack.pop()
            b = stack.pop()
            stack.extend([a, b])
        elif ins == "sub":
            a = float(stack.pop())
            b = float(stack.pop())
            stack.append(b - a)
        elif ins == "count":
            stack.append(len(stack))
        elif ins == "cvi":
            stack.append(int(stack.pop()))
        elif ins == "prune":
            stack = stack[-int(stack.pop()):]
        else:
            stack.append(ins)
    return stack


def _apply_binning_to_df(df, x_key, y_keys_to_agg, num_bins, agg_func, hue_key, x_is_datetime, xlabel, pd):
    """
    Applies binning and aggregation to the DataFrame based on x_key.

    Args:
        df (pd.DataFrame): The input DataFrame.
        x_key (str): The column name for the x-axis.
        y_keys_to_agg (list): List of column names (y-keys) to aggregate.
        num_bins (int): Number of bins for x-axis aggregation.
        agg_func (str or function): Aggregation function.
        hue_key (str or None): The key used for grouping (optional).
        x_is_datetime (bool): True if x_key is datetime.
        xlabel (str): The default xlabel value to use if not provided by user.

    Returns:
        tuple: (pd.DataFrame, str, str)
               - The DataFrame after binning and aggregation.
               - The new x_key for plotting (bin_midpoint).
               - The new xlabel for plotting.
               - Raises Exception if binning fails.
    """
    # Create bins using pd.cut
    # Use include_lowest=True to ensure min value is included
    df["bin"], bin_edges = pd.cut(df[x_key], bins=num_bins, labels=False, retbins=True, include_lowest=True)

    # Calculate bin midpoints for plotting
    bin_midpoints = []
    for i in range(num_bins):
        start_edge = bin_edges[i]
        end_edge = bin_edges[i + 1]
        if x_is_datetime:
            # Calculate midpoint for Timestamps using Timedelta
            midpoint = start_edge + (end_edge - start_edge) / 2
        else:  # Assume numeric
            midpoint = (start_edge + end_edge) / 2
        bin_midpoints.append(midpoint)

    # Map bin index to midpoint
    midpoint_map = {i: bin_midpoints[i] for i in range(num_bins)}
    df["bin_midpoint"] = df["bin"].map(midpoint_map)

    # Define grouping keys
    group_keys = ["bin_midpoint"]
    if hue_key:
        group_keys.insert(0, hue_key)

    # Define aggregation dictionary
    agg_dict = {yk: agg_func for yk in y_keys_to_agg}

    # Perform aggregation
    print(f"Aggregating by: {group_keys}")
    agg_df = df.groupby(group_keys, observed=False).agg(agg_dict).reset_index()

    # Replace df with aggregated data
    if xlabel is None:  # Update xlabel only if user didn't provide one
        xlabel = f"{x_key} (Binned)"
    x_key = "bin_midpoint"
    return agg_df, x_key, xlabel


def _apply_smoothing_to_df(df, y_keys_to_smooth, smoothing_window, hue_key, pd):
    """
    Applies rolling average smoothing to specified y_keys in the DataFrame.

    Args:
        df (pd.DataFrame): The input DataFrame (should be sorted by hue_key then x_key).
        y_keys_to_smooth (list): List of column names (y-keys) to smooth.
        smoothing_window (int): The window size for the rolling average.
        hue_key (str or None): The key used for grouping before smoothing.

    Returns:
        tuple: (pd.DataFrame, dict)
               - The DataFrame with added smoothed columns.
               - A plot_cols_map dictionary mapping original y_keys to the
                 names of the columns that should be plotted (original or smoothed).
    """
    plot_cols_map = {}
    df_smoothed = df.copy()  # Work on a copy to avoid modifying original df in unexpected ways if passed around

    for yk in y_keys_to_smooth:
        if yk not in df_smoothed.columns:
            print(f"Warning: y_key '{yk}' not found in DataFrame during smoothing. Skipping.")
            plot_cols_map[yk] = yk  # Map to original if not found, though this shouldn't happen if validated before
            continue

        smoothed_col_name = f"{yk}_smoothed_{smoothing_window}"
        try:
            if hue_key and hue_key in df_smoothed.columns:
                # Ensure grouping column is suitable type
                if not pd.api.types.is_string_dtype(df_smoothed[hue_key]) and not pd.api.types.is_categorical_dtype(
                    df_smoothed[hue_key]
                ):
                    df_smoothed[hue_key] = df_smoothed[hue_key].astype(str)

                df_smoothed[smoothed_col_name] = df_smoothed.groupby(hue_key, group_keys=False, observed=True)[
                    yk
                ].transform(lambda x: x.rolling(window=smoothing_window, min_periods=1, center=True).mean())
            else:  # Apply rolling average to the whole column if no hue_key
                df_smoothed[smoothed_col_name] = (
                    df_smoothed[yk].rolling(window=smoothing_window, min_periods=1, center=True).mean()
                )
            plot_cols_map[yk] = smoothed_col_name
        except Exception as e:
            print(f"Warning: Could not apply smoothing to '{yk}'. Using original data. Error: {e}")
            plot_cols_map[yk] = yk  # Fallback to original if smoothing fails
            if smoothed_col_name in df_smoothed.columns:  # Clean up partially created column on error
                df_smoothed.drop(columns=[smoothed_col_name], inplace=True)

    return df_smoothed, plot_cols_map


def _format_xticks(ax, x_values, max_xticks, is_binned, x_is_datetime, np, pd, plt):
    """
    Internal helper function to set and format x-axis ticks.
    Ensures uniform tick distribution if not binned and many data points.
    """
    from matplotlib.dates import DateFormatter

    if len(x_values) == 0:  # Handle empty case
        ax.set_xticks([])
        return

    selected_ticks = []

    if is_binned:
        # For binned data, x_values are the bin midpoints.
        # These are already representative of uniform intervals.
        selected_ticks = x_values
        # Optional: could still thin out if num_bins > max_xticks, but for now show all.
    else:  # Not binned
        # Create uniform ticks across the full data range.
        min_val = x_values[0]
        max_val = x_values[-1]

        if x_is_datetime:
            # Convert min/max datetime to numeric for linspace
            min_num = plt.matplotlib.dates.date2num(min_val)
            max_num = plt.matplotlib.dates.date2num(max_val)
            # Generate evenly spaced numeric ticks
            tick_nums = np.linspace(min_num, max_num, num=max_xticks)
            # Convert numeric ticks back to datetime objects
            selected_ticks = plt.matplotlib.dates.num2date(tick_nums)
        else:
            # Generate evenly spaced numeric ticks
            selected_ticks = np.linspace(min_val, max_val, num=max_xticks)

    ax.set_xticks(selected_ticks)

    # Format labels for the selected_ticks
    if x_is_datetime:  # Check type of selected_ticks
        try:
            # Use DateFormatter for specific format (up to seconds)
            date_format = DateFormatter("%Y-%m-%d %H:%M:%S")
            ax.xaxis.set_major_formatter(date_format)
            # Apply rotation and alignment AFTER setting formatter
            plt.setp(ax.get_xticklabels(), rotation=45, ha="right", size="small")
        except Exception as e_fmt:
            print(f"Warning: Could not apply DateFormatter for labels: {e_fmt}")
            # Fallback: Simple string conversion with rotation
            ax.set_xticklabels(
                [
                    str(
                        pd.Timestamp(label).strftime("%Y-%m-%d %H:%M:%S")
                        if isinstance(label, (pd.Timestamp, np.datetime64))
                        else str(label)
                    )
                    for label in selected_ticks
                ],
                rotation=45,
                ha="right",
                size="small",
            )
    else:  # Numeric or other types
        # Simple string conversion with rotation
        ax.set_xticklabels(
            [f"{label:.2f}" if isinstance(label, float) else str(label) for label in selected_ticks],
            rotation=45,
            ha="right",
            size="small",
        )


def _apply_difference_to_df(df, y_keys_original, hue_key, current_plot_cols_map, pd):
    """
    Applies the difference transformation (value - previous value) to specified y_keys.

    Args:
        df (pd.DataFrame): The input DataFrame (should be sorted by hue_key then x_key).
        y_keys_original (list): List of original y-key names.
        hue_key (str or None): The key used for grouping before differencing.
        current_plot_cols_map (dict): Current mapping of original y_keys to their column names in df.

    Returns:
        tuple: (pd.DataFrame, dict)
               - The DataFrame with added differenced columns.
               - An updated plot_cols_map dictionary.
    """
    df_diff = df.copy()
    new_plot_cols_map = current_plot_cols_map.copy()

    for yk_original in y_keys_original:
        col_to_diff = new_plot_cols_map.get(yk_original, yk_original)  # Get current column name
        if col_to_diff not in df_diff.columns:
            print(
                f"Warning: Column '{col_to_diff}' for original y_key '{yk_original}' not found "
                "for differencing. Skipping."
            )
            continue

        diff_col_name = f"{col_to_diff}_diff"
        try:
            if hue_key and hue_key in df_diff.columns:
                # Apply diff within each group
                df_diff[diff_col_name] = df_diff.groupby(hue_key, group_keys=False, observed=True)[col_to_diff].diff()
            else:
                # Apply diff to the whole column
                df_diff[diff_col_name] = df_diff[col_to_diff].diff()
            new_plot_cols_map[yk_original] = diff_col_name  # Update map to point to diff column
        except Exception as e:
            print(f"Warning: Could not apply differencing to '{yk_original}'. Using original data. Error: {e}")
            new_plot_cols_map[yk_original] = col_to_diff
            if diff_col_name in df_diff.columns:
                df_diff.drop(columns=[diff_col_name], inplace=True)

    return df_diff, new_plot_cols_map


class PlotOptions(TypedDict):
    x_key: NotRequired[str]
    y_keys: List[str]
    hue_key: NotRequired[str]
    title: NotRequired[str]
    save: NotRequired[bool]
    max_xticks: NotRequired[int]
    smoothing_window: NotRequired[int]
    tile_plots: NotRequired[bool]
    num_bins: NotRequired[int]
    agg_func: NotRequired[str]
    timezone: NotRequired[str]
    ylabel: NotRequired[str]
    apply_diff: NotRequired[bool]
    tile_key: NotRequired[str]


def plot(
    data_list: List[Dict[str, Union[str, int, float]]],
    x_key: str,
    y_keys: List[str],
    hue_key: Optional[str] = None,
    title: str = "Line Plot",
    xlabel: Optional[str] = None,
    ylabel: Optional[str] = None,
    save: bool = False,
    max_xticks: int = 20,
    smoothing_window: int = 0,
    tile_plots: bool = True,
    num_bins: int = 0,
    agg_func: str = "mean",
    apply_diff: bool = False,
    theme: str = "darkgrid",
    timezone: Optional[str] = None,
    tile_key: Optional[str] = None,
):
    """
    Generates a line plot with potentially multiple lines based on a hue category
    from a list of dictionaries using Seaborn. Assumes valid inputs.

    Args:
        data_list (list): A list of dictionaries.
        x_key (str): The key in the dictionaries for the x-axis.
        y_keys (list): A list of keys in the dictionaries for the y-axis variables..
        hue_key (str, optional): The key to differentiate lines by color. Defaults to None.
        title (str, optional): The title for the plot. Defaults to "Line Plot".
        xlabel (str, optional): The label for the x-axis. Defaults to x_key.
        ylabel (str, optional): The label for the y-axis. Defaults to y_key.
        save (bool, optional): Save plot image.
        max_xticks (int, optional): The approximate maximum number of x-ticks to display. Defaults to 20.
        smoothing_window (int, optional): The window size for the rolling average.
                                          Smoothing is applied if window > 1.
                                          Defaults to 0 (no smoothing).
        tile_plots (bool, optional): If True (default), creates separate subplots for each y_key.
                                     If False, plots all y_keys on the same graph.
        num_bins (int, optional): If > 0, divides the x_key range into this many bins
                                  and aggregates y_keys within each bin. Requires numeric/datetime x_key.
                                  Defaults to 0 (no binning).
        agg_func (str or function, optional): Aggregation function to use when binning.
                                              Examples: 'mean', 'median', 'sum', 'count', 'std' (see pandas
                                              agg() method). Defaults to 'mean'.
        apply_diff (bool, optional): If True, calculates the difference between consecutive y_key values.
                                     Defaults to False. Applied after binning and smoothing.
        theme (str, optional): Seaborn theme. See seaborn documentation for options. Default: 'darkgrid'.
        timezone (str, optional): A timezone spec. It is added on the x label if x label are timestamps.
        tile_key (str, optional): If provided, creates separate tiled plots for each unique value
                                  in this column. This overrides `tile_plots=True` for y_key-based tiling.
    Returns:
        None: Displays the plot using matplotlib.pyplot.show() or saves it.
    """
    try:
        import numpy as np
        import pandas as pd
        import seaborn as sns
        import matplotlib.pyplot as plt
    except ImportError as e:
        print(f"Plotting requires library {e.name}")
        return

    # Convert the list of dictionaries to a Pandas DataFrame
    df = pd.DataFrame(data_list)
    if tile_key:
        df = df.dropna(subset=[tile_key])
    if hue_key:
        df = df.dropna(subset=[hue_key])
    df = df.dropna(axis=1, how='all')

    y_keys_in_df = [yk for yk in y_keys if yk in df.columns and yk != tile_key]  # Use this for processing

    sort_keys = []
    if hue_key and hue_key in df.columns:
        sort_keys.append(hue_key)
    if tile_key:
        sort_keys.append(tile_key)

    x_is_datetime = False
    # Attempt to convert x_key to datetime if possible for proper sorting
    try:
        # Use errors='coerce' to handle non-convertible values gracefully (they become NaT)
        df[x_key] = pd.to_datetime(df[x_key], errors="coerce")
        df = df.dropna(subset=[x_key])  # Drop rows where conversion failed
        if not df.empty:
            x_is_datetime = True
    except (ValueError, TypeError, OverflowError):
        # If not datetime or conversion fails, treat as categorical or numeric
        print(f"Warning: x_key '{x_key}' could not be reliably converted to datetime. Sorting based on original type.")
        # Pass # No conversion needed, sort will use existing type

    sort_keys.append(x_key)

    try:
        print(f"Sorting DataFrame by: {sort_keys}")
        # Drop rows where x_key became NaT during conversion attempt
        if pd.api.types.is_datetime64_any_dtype(df[x_key]):
            df = df.dropna(subset=[x_key])
        df = df.sort_values(by=sort_keys).reset_index(drop=True)  # Reset index after sort
    except KeyError as e:
        print(f"Error sorting DataFrame: Could not find key {e}. Check x_key and hue_key.")
        return  # Cannot proceed without sorting
    except Exception as e:
        print(f"An unexpected error occurred during sorting: {e}")
        return  # Cannot proceed without sorting

    apply_smoothing = smoothing_window > 1
    plot_cols_map = {}  # Map original y_key to the column name to plot (original or smoothed)

    y_keys = [k for k in y_keys if k != x_key and k != hue_key]
    if not y_keys:
        print("Error: y_keys must be a non-empty list of strings.")
        return

    if num_bins > 0:
        print(f"Applying binning: num_bins={num_bins}, agg_func='{agg_func}'")
        df, x_key, xlabel = _apply_binning_to_df(
            df, x_key, y_keys_in_df, num_bins, agg_func, hue_key, x_is_datetime, xlabel, pd
        )

    plot_cols_map = {yk: yk for yk in y_keys_in_df}  # Initialize map with original y-keys (or aggregated ones)
    if apply_smoothing:
        print(f"Applying smoothing with window {smoothing_window}.")
        df, plot_cols_map = _apply_smoothing_to_df(df, y_keys_in_df, smoothing_window, hue_key, pd)

    if apply_diff:
        print("Applying difference transformation.")
        df, plot_cols_map = _apply_difference_to_df(df, y_keys_in_df, hue_key, plot_cols_map, pd)

    # Filter y_keys to only those successfully processed (found and optionally smoothed)
    valid_y_keys = list(plot_cols_map.keys())
    if not valid_y_keys:
        print("Error: No valid y_keys found or processed.")
        return

    # Set the plot style (optional)
    sns.set_theme(style=theme)
    # Select columns needed: x_key, y_cols, and hue_key (if it exists)
    cols_to_keep = [x_key] + list(plot_cols_map.values())
    if hue_key and hue_key in df.columns:  # Check if hue_key exists and is valid
        cols_to_keep.append(hue_key)
    elif hue_key:
        print(f"Warning: Provided hue_key '{hue_key}' not found in data. Ignoring hue.")
        hue_key = None
    if tile_key and tile_key in df.columns:
        cols_to_keep.append(tile_key)
    df = df.dropna(axis=1, how='all')
    plot_df = df[cols_to_keep].copy()

    if apply_smoothing:
        title += f" (Smoothed, Window={smoothing_window})"
    if num_bins > 0:
        title += f" (Binned, n={num_bins})"
    if apply_diff:
        title += " (Differenced)"

    num_plots = len(valid_y_keys)

    xlabel = xlabel or x_key
    if x_is_datetime and timezone:
        xlabel = f"{xlabel} ({timezone})"

    # --- Tiling by tile_key Logic ---
    if tile_key and tile_key in plot_df.columns:
        unique_tile_values = np.sort(plot_df[tile_key].unique())  # Get unique values for tiling

        num_plots = len(unique_tile_values)
        print(f"Tiling by '{tile_key}'. Found {num_plots} unique tiles.")

        ncols = min(math.ceil(math.sqrt(num_plots)), 4)
        nrows = math.ceil(num_plots / ncols)

        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(ncols * 10, nrows * 7), squeeze=False)
        fig.suptitle(title, fontsize=16, y=0.98)
        axes_flat = axes.flatten()

        # Collect legend handles and labels across all subplots
        all_handles = []
        all_labels = []

        for i, yk in enumerate(unique_tile_values):
            ax = axes_flat[i]

            tile_data = plot_df[plot_df[tile_key] == yk].copy()

            # Melt data for multiple y_keys on one tile
            id_vars_tile = [x_key]
            if hue_key:
                id_vars_tile.append(hue_key)

            value_vars_tile_actual = [plot_cols_map[yk] for yk in valid_y_keys]
            rename_map_for_metric_tile = {plot_cols_map[yk]: yk for yk in valid_y_keys}

            melted_tile_df = pd.melt(
                tile_data,
                id_vars=id_vars_tile,
                value_vars=value_vars_tile_actual,
                var_name="Metric_actual",
                value_name="Value",
            )
            melted_tile_df["Metric"] = melted_tile_df["Metric_actual"].map(rename_map_for_metric_tile)

            sns.lineplot(
                data=melted_tile_df,
                x=x_key,
                y="Value",
                hue=hue_key if hue_key else None,
                style="Metric" if len(valid_y_keys) > 1 else None,
                marker="o",
                ax=ax,
            )
            ax.set_title(yk)  # Subplot title
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel or "Value" if not apply_diff else "Difference in Value")

            # Collect handles/labels for figure-level legend
            h, lab = ax.get_legend_handles_labels()
            # Modify labels to include both hue and metric if both are used
            if hue_key and "Metric" in melted_tile_df.columns:
                # Reconstruct labels like "Hue_Val / Metric_Val"
                for label_idx, original_label in enumerate(lab):
                    if original_label == hue_key:  # This is the legend title, skip
                        continue
                    # Find the corresponding hue and style values for this legend entry
                    # This is a bit tricky; often it's better to manually create proxy artists
                    # or ensure seaborn's legend creation matches your desired combined labels.
                    # For simplicity, let's assume labels are ordered as hue1-metric1, hue1-metric2, hue2-metric1...
                    # or just hue1, hue2, metric1, metric2 etc.
                    # For now, we'll just append them as is, which might not be perfectly combined.
                    # pass  # Will collect later from ax.get_legend_handles_labels()

            if h and lab:
                # Append only unique handles/labels to avoid duplicates in figure legend
                for handle, label in zip(h, lab):
                    if label not in all_labels:
                        all_handles.append(handle)
                        all_labels.append(label)

            _format_xticks(
                ax=ax,
                x_values=np.sort(plot_df[x_key].unique()),
                max_xticks=max_xticks,
                is_binned=num_bins > 0,
                x_is_datetime=x_is_datetime,
                np=np,
                pd=pd,
                plt=plt,
            )

        # Hide unused subplots
        for i in range(num_plots, len(axes_flat)):
            axes_flat[i].set_visible(False)

        fig.subplots_adjust(
            left=0.08,
            right=0.95,
            bottom=0.15,
            top=0.85 if (all_handles and all_labels) else 0.95,
            hspace=0.5,
            wspace=0.25,
        )
    # --- Tiling by y_key Logic ---
    elif tile_plots and num_plots > 1:
        # Calculate grid size (prefer wider than tall)
        ncols = min(math.ceil(math.sqrt(num_plots)), 4)
        nrows = math.ceil(num_plots / ncols)

        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(ncols * 10, nrows * 7), squeeze=False)
        fig.suptitle(title, fontsize=16, y=0.98)  # Main title for the figure
        axes_flat = axes.flatten()
        handles, labels = None, None  # To store legend handles/labels

        for i, yk in enumerate(valid_y_keys):
            ax = axes_flat[i]
            plot_col_name = plot_cols_map[yk]  # Get the actual column name (original or smoothed)

            sns.lineplot(
                data=plot_df,
                x=x_key,
                y=plot_col_name,
                hue=hue_key,
                marker="o",
                ax=ax,
            )

            ax.set_title(yk)  # Subplot title
            ax.set_xlabel(xlabel)
            ax.set_ylabel(ylabel or plot_col_name)

            if hue_key and handles is None and labels is None:
                current_handles, current_labels = ax.get_legend_handles_labels()
                if current_handles:
                    handles, labels = current_handles, current_labels

            # Explicitly hide the legend on the subplot *after* getting handles/labels
            if ax.get_legend() is not None:
                ax.get_legend().set_visible(False)

            _format_xticks(
                ax=ax,
                x_values=np.sort(plot_df[x_key].unique()),
                max_xticks=max_xticks,
                is_binned=num_bins > 0,
                x_is_datetime=x_is_datetime,
                np=np,
                pd=pd,
                plt=plt,
            )

        # Hide unused subplots
        for i in range(num_plots, len(axes_flat)):
            axes_flat[i].set_visible(False)

        # Add a single figure-level legend below the plots
        if hue_key is not None and handles and labels:
            # Place legend below the subplots, centered horizontally
            # Adjust ncol based on number of labels for better layout
            fig.legend(
                handles,
                labels,
                title=hue_key,
                loc="upper center",
                bbox_to_anchor=(0.5, 0.95),
                ncol=min(len(labels), 6),
                fontsize="medium",
                title_fontsize="medium",
            )

        # Adjust layout to prevent overlap and make space for legend/title
        fig.subplots_adjust(
            left=0.08, right=0.95, bottom=0.25, top=0.85 if hue_key is not None else 0.95, hspace=0.5, wspace=0.25
        )

    # --- Single Plot Logic, multiple y keys ---
    elif num_plots > 1:
        # Melt the DataFrame to long format for plotting multiple y-vars on one graph
        id_vars = [x_key]
        if hue_key is not None:
            id_vars.append(hue_key)

        value_vars = [plot_cols_map[yk] for yk in valid_y_keys]  # Columns to melt (original or smoothed)
        # Map smoothed names back to original for the 'variable' column in melted data
        rename_map = {v: k for k, v in plot_cols_map.items()}

        try:
            melted_df = pd.melt(plot_df, id_vars=id_vars, value_vars=value_vars, var_name="Metric", value_name="Value")
            # Rename the 'Metric' column values back to original y_key names
            melted_df["Metric"] = melted_df["Metric"].map(rename_map)
        except Exception as e:
            print(f"Error during data melting: {e}. Cannot create single plot.")
            return

        plt.figure(figsize=(12, 6))
        # Use 'style' to differentiate the original y_keys, 'hue' for the original hue_key
        ax = sns.lineplot(
            data=melted_df, x=x_key, y="Value", hue=hue_key or "Metric", style="Metric", marker="o", sort=False
        )

        # --- Customization ---
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel("Value")  # Generic Y label as it represents multiple metrics
        if hue_key is not None:
            plt.legend(title=f"{hue_key} / Metric")  # Combined legend title

        _format_xticks(
            ax=ax,
            x_values=np.sort(plot_df[x_key].unique()),
            max_xticks=max_xticks,
            is_binned=num_bins > 0,
            x_is_datetime=x_is_datetime,
            np=np,
            pd=pd,
            plt=plt,
        )

        plt.tight_layout()  # Adjust layout

    # --- Single Plot Logic, single y key ---
    else:

        y_key = valid_y_keys[0]
        y_col_to_plot = plot_cols_map[y_key]

        plt.figure(figsize=(10, 6))  # Set figure size
        ax = sns.lineplot(data=df, x=x_key, y=y_col_to_plot, hue=hue_key, marker="o")

        # --- Customization ---
        plt.title(title)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel or y_key)
        if hue_key:
            plt.legend(title=hue_key)  # Add a legend based on the hue key

        _format_xticks(
            ax=ax,
            x_values=np.sort(plot_df[x_key].unique()),
            max_xticks=max_xticks,
            is_binned=num_bins > 0,
            x_is_datetime=x_is_datetime,
            np=np,
            pd=pd,
            plt=plt,
        )

        # Adjust layout to prevent labels from overlapping plot elements
        plt.tight_layout()  # Call this *after* setting labels and titles

    # --- Display / Save ---
    if hasattr(sys, "ps1") or "ipykernel" in sys.modules or "spyder" in sys.modules:
        plt.show()
    else:
        print("Cannot display plot. Will try to save on filesystem...")
        save = True

    if save:
        try:
            save_path = f"{uuid4()}.png"
            plt.savefig(save_path)
            print(f"Plot saved to {save_path}.")
        except Exception as save_err:
            print(f"Could not save plot: {save_err}")
        plt.close()  # Close the plot figure


class FilterResult(TypedDict):
    tstamp: datetime.datetime
    message: NotRequired[str]
    groups: Tuple[Union[str, int, float], ...]
    dict_groups: NotRequired[Dict["str", Union[str, int, float]]]
    field: NotRequired[str]
    itemno: NotRequired[int]
    stats: NotRequired[Dict[str, Union[str, int, float]]]
    value: NotRequired[str]


class ScanJobs(BaseScript):

    description = __doc__

    def __init__(self):
        super().__init__()
        self.captured_stats_keys: List[str] = []

    def add_argparser_options(self):
        super().add_argparser_options()
        self.argparser.add_argument("spider", help="Target spider or script. If '*' is used, match everything.")
        self.argparser.add_argument(
            "--log-pattern", "-l", help="Log pattern. Can be multiple.", action="append", default=[]
        )
        self.argparser.add_argument(
            "--spider-argument-pattern",
            "-a",
            help="argument pattern (in format arg:value). Can be multiple.",
            action="append",
            default=[],
        )
        self.argparser.add_argument(
            "--item-field-pattern",
            "-f",
            help=(
                "Item field pattern (in format jmespath_search_string:value). Can be multiple. If value is empty "
                "string, just matches existence of the searched field."
            ),
            action="append",
            default=[],
        )
        self.argparser.add_argument(
            "--stat-pattern",
            "-s",
            help="Stat key pattern. Can be multiple.",
            action="append",
            default=[],
        )
        self.argparser.add_argument(
            "--tag-pattern",
            help="Only match given tag pattern. Can be multiple.",
            action="append",
            default=[],
        )
        self.argparser.add_argument(
            "--period",
            "-p",
            default=86400,
            help="Time window period in seconds or whatever string parsed by timelength library. Default: %(default)s",
        )
        self.argparser.add_argument(
            "--end-time",
            "-e",
            help=(
                "End side of the time window. By default it is just now. "
                "In any format that dateparser can recognize."
            ),
        )
        self.argparser.add_argument(
            "--first-match-only",
            help="Print only first match and continue with next job.",
            action="store_true",
        )
        self.argparser.add_argument(
            "--max-items-per-job", type=int, help="Don't scan more than the given number of items or logs per job."
        )
        self.argparser.add_argument(
            "--print-progress-each",
            type=int,
            default=100,
            help="Print scan progress each given number of jobs. Default: %(default)s",
        )
        self.argparser.add_argument(
            "--write",
            "-w",
            type=argparse.FileType("w"),
            help="If given, write the captured patterns into the provided json list file, along with dates.",
        )
        self.argparser.add_argument(
            "--post-process-code",
            "-c",
            help="postscript like instructions to process groups.",
        )
        self.argparser.add_argument(
            "--data-headers",
            help=(
                "If provided, instead of generating a list per datapoint, it generates a dict. Comma separated list."
                "If 'auto' is provided, try to generate headers automatically. This requires alternating text and"
                "value being extracted."
            ),
        )
        self.argparser.add_argument(
            "--plot",
            help=(
                "If provided, generate a plot with the provided parameters. Format: "
                "X=<x key>,Y=<y keys>,hue=<hue key>,title=<title>,save,xticks=<num>,smooth=<num>,no_tiles,ylabel"
                "bins=<n/func>\n"
                "title is required. "
                "Y can be a single y key, or multiple separated by /. If not provided, will use all extracted headers "
                "except the ones defined in X and/or hue."
                "X defaults to time stamp. save and no_tiles are flags, True if included, False "
                "otherwise. If save is provided, save plot image. If no_tiles is provided, plot all y_keys"
                "in same graph. "
                "bins get two parameters: number of bins and aggregate function (like sum, mean, std, median, etc. "
                "See pandas agg() method)"
                "Requires --data-headers in order to name extracted data points."
            ),
        )
        self.argparser.add_argument(
            "--has-tag", action="append", help="Only select jobs with the given tag. Can be multiple."
        )
        self.argparser.add_argument(
            "--include-running-jobs",
            action="store_true",
            help="Also scan running jobs. By default only scans finished jobs",
        )
        self.argparser.add_argument(
            "--count",
            action="store_true",
            help=(
                "If provided, log matches will generate 1 so they can be aggregated as a counter, for example "
                "in order to use with plot option 'bin=<n>/sum'. (see --plot)"
            ),
        )
        self.argparser.add_argument(
            "--read",
            "-r",
            type=argparse.FileType("r"),
            help=(
                "If provided, read plot data from given file (previously generated with -w), "
                "instead of scanning jobs."
            ),
        )
        self.argparser.add_argument(
            "--safe-default-stat",
            "-d",
            action="append",
            default=[],
            help=(
                "The provided stat can be safely added as default on every stats where it is missing. "
                "Can be given multiple times."
            ),
        )
        self.argparser.add_argument("--tstamp-format", default="%Y-%m-%d %H:%M:%S", help="Default: %(default)s")
        self.argparser.add_argument(
            "--zone-info",
            "-z",
            help=(
                "Ensure output timestamps are on the provided zone info. Must be a string supported by zoneinfo "
                "library"
            ),
        )
        self.argparser.add_argument(
            "--capture-spiderargs",
            action="store_true",
            help="When using --data-headers, also include job spiderargs in the captured result.",
        )
        self.argparser.add_argument(
            "--capture-joblink",
            action="store_true",
            help="When using --data-headers, also include job link in the captured result.",
        )
        self.argparser.add_argument(
            "--separate-matches-per-target",
            action="store_true",
            help="Yield matches for same target (either log line or stats) separately",
        )
        self.argparser.add_argument(
            "--separate-patterns-per-target",
            action="store_true",
            help="Yield matches for same target and pattern (either log line or stats) separately",
        )
        self.argparser.add_argument(
            "--scripts-only",
            action="store_true",
            help="When used with wildcard '*' passed as argument, only match scripts.",
        )
        self.argparser.add_argument(
            "--spiders-only",
            action="store_true",
            help="When used with wildcard '*' passed as argument, only match spiders.",
        )
        self.argparser.add_argument(
            "--no-user-enter",
            action="store_true",
            help=(
                "Don't wait for user to press enter to continue on each match. This is the default "
                "when using --write or --plot."
            ),
        )

    def parse_args(self) -> argparse.Namespace:
        args = super().parse_args()
        if args.plot and not args.data_headers:
            self.argparser.error("You must use --data-headers along with --plot")
        if (args.spiders_only or args.scripts_only) and args.spider != "*":
            self.argparser.error("--spiders-only and --scripts-only can only be used when argument is '*'")
        return args

    def filter_log_pattern(self, jdict: JobDict, job: Job, limit: int) -> Iterator[FilterResult]:
        if not self.args.log_pattern:
            return
        job_has_match = False
        for idx, logline in enumerate(job.logs.iter()):
            if self.args.max_items_per_job and idx == self.args.max_items_per_job:
                break
            if logline["time"] < limit:
                continue
            log_time = logline["time"] / 1000
            if msg := logline["message"]:
                for pattern in self.args.log_pattern:
                    pattern_has_match = False
                    groups: Tuple[str, ...] = ()
                    for m in re.finditer(pattern, msg, flags=re.S):
                        job_has_match = True
                        pattern_has_match = True
                        groups += m.groups()
                    if groups or pattern_has_match:
                        yield {
                            "tstamp": datetime.datetime.fromtimestamp(log_time),
                            "message": msg,
                            "groups": (1,) if self.args.count else groups,
                        }

                    if self.args.first_match_only and job_has_match:
                        break

    def filter_item_field_pattern(self, jdict: JobDict, job: Job, limit: int) -> Iterator[FilterResult]:
        if not self.args.item_field_pattern:
            return
        has_match = False
        for idx, item in enumerate(job.items.iter(meta=["_ts"], startts=int(limit))):
            if self.args.max_items_per_job and idx == self.args.max_items_per_job:
                break

            item_time = item["_ts"] / 1000
            for item_field_pattern in self.args.item_field_pattern:
                jpath, pattern = item_field_pattern.split(":", 1)
                value = jmespath.search(jpath, item)
                if value is None:
                    continue
                m = None
                if not pattern and value or (m := re.search(pattern, value)) is not None:
                    yield {
                        "tstamp": datetime.datetime.fromtimestamp(item_time),
                        "itemno": idx,
                        "field": jpath,
                        "value": value,
                        "groups": m.groups() if m is not None else (value,),
                    }
                    has_match = True

            if self.args.first_match_only and has_match:
                break

    def filter_stats_pattern(self, jdict: JobDict, job: Job, tstamp: datetime.datetime) -> Iterator[FilterResult]:
        if not self.args.stat_pattern:
            return
        groups: List[str] = []
        if not self.captured_stats_keys:
            self.captured_stats_keys = self.args.safe_default_stat.copy()
        scrapystats = {k: 0 for k in self.captured_stats_keys if k in self.args.safe_default_stat}
        # this ensures correct defaults when for example post processing expects a specific amount of data and
        # stats miss some of them
        for key, val in jdict.get("scrapystats", {}).items():
            scrapystats[key] = val
        ordered_scrapy_stats = {}
        for key in sorted(scrapystats.keys()):
            ordered_scrapy_stats[key] = scrapystats[key]

        collected_stats: Dict[str, Union[str, int, float]] = {}
        for stat_pattern in self.args.stat_pattern:
            pattern_groups: List[str] = []
            for key, val in ordered_scrapy_stats.items():
                if m := re.search(stat_pattern, key):
                    collected_stats[key] = val
                    if self.args.separate_matches_per_target:
                        yield {
                            "tstamp": tstamp,
                            "stats": collected_stats,
                            "groups": m.groups() + (str(val),),
                        }
                    elif self.args.separate_patterns_per_target:
                        pattern_groups.extend(m.groups() + (str(val),))
                    else:
                        groups.extend(m.groups() + (str(val),))
            if pattern_groups:
                yield {
                    "tstamp": tstamp,
                    "stats": collected_stats,
                    "groups": tuple(pattern_groups),
                }
        if collected_stats:
            self.captured_stats_keys = list(collected_stats.keys())

        if groups:
            yield {
                "tstamp": tstamp,
                "stats": collected_stats,
                "groups": tuple(groups),
            }

    def filter_spider_argument(
        self, jdict: JobDict, tstamp: datetime.datetime, jobcount: int
    ) -> Optional[Dict[str, str]]:
        for spider_arg_pattern in self.args.spider_argument_pattern:
            key, pattern = spider_arg_pattern.split(":", 1)
            if re.search(pattern, jdict.get("spider_args", {}).get(key, "")):
                print(f"Jobs scanned: {jobcount}")
                print(f"Timestamp reached: {tstamp}")
                print(f"https://app.zyte.com/p/{jdict['key']}/stats")
                print(jdict["spider_args"])
                return jdict["spider_args"]
        return None

    def run(self):

        end_limit = time.time()
        if self.args.end_time is not None and (dt := dateparser.parse(self.args.end_time)) is not None:
            end_limit = dt.timestamp()

        plot_data_points: List[Dict[str, Union[str, int, float]]] = []
        plot_options: PlotOptions = {"y_keys": []}
        if self.args.plot:
            for option in self.args.plot.split(","):
                if option == "save":
                    plot_options["save"] = True
                elif option == "no_tiles":
                    plot_options["tile_plots"] = False
                elif option == "tdiff":
                    plot_options["apply_diff"] = True
                else:
                    key, val = option.split("=")
                    if key == "X":
                        plot_options["x_key"] = val
                    elif key == "Y":
                        plot_options["y_keys"] = val.split("/")
                    elif key == "hue":
                        plot_options["hue_key"] = val
                    elif key == "title":
                        plot_options["title"] = val
                    elif key == "xticks":
                        plot_options["max_xticks"] = int(val)
                    elif key == "smooth":
                        plot_options["smoothing_window"] = int(val)
                    elif key == "bins":
                        num_bins, plot_options["agg_func"] = val.split("/")
                        plot_options["num_bins"] = int(num_bins)
                    elif key == "ylabel":
                        plot_options["ylabel"] = val
                    elif key == "tile_key":
                        plot_options["tile_key"] = val
                    else:
                        self.argparser.error(f"Wrong plot parameter '{key}'")
            assert "title" in plot_options, "title is required for plot."
            plot_options.setdefault("x_key", "tstamp")

        all_headers = set()
        if self.args.read:
            for line in self.args.read:
                record = json.loads(line)
                all_headers.update(record.keys())
                plot_data_points.append(record)
            all_headers.discard("tstamp")
        else:
            all_headers = self.scan_jobs(end_limit, plot_data_points)

        if self.args.plot:
            if not plot_options["y_keys"]:
                plot_options["y_keys"] = sorted(all_headers)
            if not plot_data_points:
                print("No data to plot.")
            else:
                plot_options["timezone"] = self.args.zone_info
                print("Generating plots...")
                plot(plot_data_points, **plot_options)

    def _convert_timestamp(self, timestamp: datetime.datetime) -> str:
        if self.args.zone_info:
            timestamp = timestamp.astimezone(zoneinfo.ZoneInfo(self.args.zone_info))
        return timestamp.strftime(self.args.tstamp_format)

    def scan_jobs(self, end_limit, plot_data_points: List[Dict[str, Union[str, int, float]]]):
        period = TimeLength(self.args.period).result.seconds
        limit = (end_limit - period) * 1000
        jobcount = 0
        all_headers = set()
        meta = ["finished_time", "running_time"]
        if self.args.spider_argument_pattern:
            meta.append("spider_args")
        if self.args.stat_pattern:
            meta.append("scrapystats")
        if self.args.spiders_only or self.args.scripts_only:
            meta.append("spider")
        tag_patterns = []
        if self.args.tag_pattern:
            meta.append("tags")
            tag_patterns = [re.compile(tag_re) for tag_re in self.args.tag_pattern]
        for jdict in self.get_jobs(
            spider=None if self.args.spider == "*" else self.args.spider,
            meta=meta,
            state=["finished", "running"] if self.args.include_running_jobs else ["finished"],
            has_tag=self.args.has_tag,
        ):
            jobcount += 1
            if self.args.spiders_only and jdict["spider"].startswith("py:"):
                continue
            if self.args.scripts_only and not jdict["spider"].startswith("py:"):
                continue
            if tag_patterns:
                tag_match = False
                for tag in jdict["tags"]:
                    if any(re.search(treg, tag) for treg in tag_patterns):
                        tag_match = True
                        break
                if not tag_match:
                    continue

            if "finished_time" in jdict and jdict["finished_time"] / 1000 > end_limit:
                continue

            if "finished_time" in jdict and jdict["finished_time"] < limit:
                print(f"Reached limit of {period} seconds.")
                print("Total jobs scanned:", jobcount)
                break

            keyprinted = False
            job = self.get_job(jdict["key"])
            if "finished_time" in jdict:
                job_tstamp = jdict["finished_time"]
            elif "running_time" in jdict:
                job_tstamp = int((jdict["running_time"] + int(end_limit * 1000)) / 2)
            else:
                continue
            tstamp = datetime.datetime.fromtimestamp(job_tstamp / 1000)
            has_match = False

            spiderargs = self.filter_spider_argument(jdict, tstamp, jobcount)
            if spiderargs:
                has_match = True
                keyprinted = True
                if not self.args.write and not self.args.plot and not self.args.no_user_enter:
                    input("Press Enter to continue...\n")
            elif self.args.spider_argument_pattern:
                continue

            post_process_instructions: Optional[List[str]] = (
                self.args.post_process_code.split() if self.args.post_process_code is not None else None
            )
            post_process_stack: List[Union[str, int, float]] = []
            for result in chain(
                self.filter_log_pattern(jdict, job, limit),
                self.filter_item_field_pattern(jdict, job, limit),
                self.filter_stats_pattern(jdict, job, tstamp),
                # ensure to print matching job even if no patterns provided
                [FilterResult(groups=(), tstamp=tstamp)],
            ):
                job_link = f"https://app.zyte.com/p/{jdict['key']}/stats"
                if not keyprinted:
                    print(f"Jobs scanned: {jobcount}")
                    print(f"Timestamp reached: {result['tstamp']}")
                    print(job_link)
                    keyprinted = True
                if "message" in result:
                    print(result["message"])
                    has_match = True
                if "itemno" in result:
                    print(f"Item #{result['itemno']}. {result['field']}:{result['value']}")
                    has_match = True
                if "stats" in result:
                    print("Matching stats:", result["stats"])
                    has_match = True
                if result["groups"]:
                    if post_process_instructions is not None:
                        hold = False
                        if post_process_instructions and post_process_instructions[0] == "hold":
                            post_process_instructions.pop(0)
                            hold = True
                        print("Data points extracted:", result["groups"], "Holded" if hold else "")
                        post_process_stack.extend(result["groups"])
                        if hold:
                            continue
                        post_process_stack.extend(post_process_instructions)
                        try:
                            result["groups"] = tuple(post_process(post_process_stack))
                        except ZeroDivisionError:
                            LOG.warning(f"Ignoring data {result['groups']}: post processing raised ZeroDivisionError.")
                            if not self.args.plot and not self.args.write and not self.args.no_user_enter:
                                input("Press Enter to continue...\n")
                            continue
                        except Exception as e:
                            LOG.warning(f"Ignoring data {result['groups']}: post processing raised {e!r}.")
                            if not self.args.plot and not self.args.write and not self.args.no_user_enter:
                                input("Press Enter to continue...\n")
                            continue
                        post_process_stack = []

                    if self.args.data_headers:
                        if self.args.data_headers == "auto":
                            list_iterator = iter(result["groups"])
                            result["dict_groups"] = dict(zip(*[list_iterator] * 2))
                            headers = sorted(result["dict_groups"].keys())
                        else:
                            headers = self.args.data_headers.split(",")
                            result["dict_groups"] = dict(zip(headers, result["groups"]))
                        for k, v in list(result["dict_groups"].items()):
                            if isinstance(v, str):
                                try:
                                    result["dict_groups"][k] = float(v)
                                except ValueError:
                                    pass
                        result["dict_groups"]["tstamp"] = self._convert_timestamp(result["tstamp"])
                        if self.args.plot:
                            plot_data_points.insert(0, result["dict_groups"])
                            all_headers.update(headers)
                        if spiderargs and self.args.capture_spiderargs:
                            result["dict_groups"].update(spiderargs)
                        if self.args.capture_joblink:
                            result["dict_groups"]["job_link"] = job_link
                    print("Data points generated:", result.get("dict_groups") or result["groups"])
                any_pattern = self.args.log_pattern or self.args.item_field_pattern or self.args.stat_pattern
                if self.args.write:
                    if result.get("dict_groups"):
                        print(json.dumps(result["dict_groups"]), file=self.args.write)
                    elif result["groups"]:
                        groups = (self._convert_timestamp(result["tstamp"]),) + result["groups"]
                        print(json.dumps(groups), file=self.args.write)
                    elif not any_pattern:
                        print(job_link, file=self.args.write)
                elif (
                    not self.args.plot
                    and (result["groups"] or has_match or not any_pattern)
                    and not self.args.no_user_enter
                ):
                    input("Press Enter to continue...\n")

                if self.args.first_match_only and has_match:
                    break

            if jobcount % self.args.print_progress_each == 0:
                print(f"Jobs scanned: {jobcount}")
                tstamp = datetime.datetime.fromtimestamp(job_tstamp / 1000)
                print(f"Timestamp reached: {tstamp}")

        return all_headers


if __name__ == "__main__":
    ScanJobs().run()
