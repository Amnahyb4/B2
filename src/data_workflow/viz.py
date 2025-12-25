import pandas as pd
from pathlib import Path
import plotly.express as px

def save_fig(fig, path:Path, *, scale:int=2)-> None: ##Plotly scale multiplies the resolution
    """Save plotly figure as an image"""
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.write_image(str(path), scale=scale)


def bar_sorted(df: pd.DataFrame, x:str, y:str, title:str):
    """Created bar chart"""
    d=df.sort_values(y, ascending=False)
    fig=px.bar(d, x=x, y=y, title=title)
    fig.update_layout(
        title={"x":0.02},##align title
        margin={"l":60, "r":20, "t":60, "b":60 }, ##adds padding so labels are not cut off
    )
    fig.update_xaxes(title_text=x)
    fig.update_yaxes(title_text=y)
    return fig 

def time_line(df:pd.DataFrame, x:str, y:str, color=None, title:str=""):
    """ Create a line chart for time series data"""
    fig=px.line(df, x=x, y=y, color=color,title=title)
    fig.update_layout(title={"x":0.02})
    fig.update_xaxes(title_text=x)
    fig.update_yaxes(title_text=y)
    return fig 

def histogram_chart(df:pd.DataFrame, x:str, nbins:int=30, title:str=""):
    """create histogram for distribution visualization"""
    fig=px.histogram(df, x=x, nbins=nbins, title=title)
    fig.update_layout(title={"x": 0.02})
    fig.update_xaxes(title_text=x)
    fig.update_yaxes(title_text="Number of orders")
    return fig

##bar chart: category-numeric , discrte , seperated by spaces indicating category distinct
##histogram: continous data, no spacing, grouping values into bins ex(5,6,7,8..)