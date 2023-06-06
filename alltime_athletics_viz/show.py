import polars as pl
from IPython.core.display import HTML
from IPython.display import display


def show_df(df: pl.DataFrame):
    display(HTML(df.to_pandas().to_html()))
