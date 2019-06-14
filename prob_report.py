#!/usr/bin/env python

""" Script to generate Problem Report Email """

import datetime
import time

import pandas as pd
import plotly
import plotly.graph_objs as go

from jinja2 import Environment, PackageLoader
from pyzabbix import ZabbixAPI


def retrieve_data():
    """ Function to retrieve Problem data from Zabbix API """

    # Generate Today's date (MM/DD/YYYY) into Unix time (##########)
    todays_date = int(time.mktime(datetime.date.today().timetuple()))

    # Access Zabbix API
    zapi = ZabbixAPI(zabbix url) # URL to your Zabbix installation
    zapi.login(username, password) # Zabbix Username/Password for report user

    events = zapi.event.get(
        time_from=todays_date - 604800,
        time_till=todays_date,
        selectHosts=["host"],
        output="extend",
        sortfield=["clock", "eventid"],
        sortorder="ASC",
    )

    return pd.DataFrame(events)


def clean_data(args):
    """ Function to clean the data within the dataframe """
    dataframe = pd.DataFrame(args)

    # Drop unnecessary columns from the dataframe
    dropped_frame = dataframe.drop(
        [
            "acknowledged",
            "c_eventid",
            "correlationid",
            "ns",
            "object",
            "objectid",
            "source",
            "suppressed",
            "userid",
            "value",
        ],
        axis=1,
    )

    # Move resolved problems to a new column associated with original problem
    def create_resolution_time_dataframe(args):
        resolution_time = args[["r_eventid"]].copy()
        resolution_time["eventid"] = resolution_time["r_eventid"]
        resolution_time.drop("r_eventid", axis=1, inplace=True)
        event_time = args[["eventid", "clock"]].copy()
        merged_frame = resolution_time.merge(event_time, on="eventid")
        return merged_frame

    def restructure_dataframe():
        merged_frame = create_resolution_time_dataframe(dropped_frame)
        merged_frame["r_eventid"] = merged_frame["eventid"]
        merged_frame["r_clock"] = merged_frame["clock"]
        merged_frame.drop(["eventid", "clock"], axis=1, inplace=True)
        return merged_frame

    adjusted_frame = dropped_frame.merge(restructure_dataframe(), on="r_eventid")

    # Create new column with Resolved Time in seconds rts_clock
    def create_resolved_time(args):
        dataframe = pd.DataFrame(args)
        dataframe["clock"] = pd.Series(dataframe["clock"]).astype(int)
        dataframe["r_clock"] = pd.Series(dataframe["r_clock"]).astype(int)
        dataframe = dataframe.assign(rts_clock=lambda x: x.r_clock - x.clock)
        return dataframe

    resolved_frame = create_resolved_time(adjusted_frame)

    # Drop the empty 'Hosts' and 'r_eventid' rows, reformat dataframe
    def host_series_adjustment(args):
        dataframe = pd.DataFrame(args)
        host_frame = pd.DataFrame(dataframe["hosts"])
        host_frame["hosts"] = pd.Series(host_frame["hosts"]).astype(str).str.upper()
        host_frame["hosts"] = host_frame["hosts"].str.strip("[]").str.strip("{}")
        host_frame = host_frame["hosts"].str.split(" ", n=3, expand=True)
        host_frame[3] = host_frame[3].str.strip("''")
        dataframe["hosts"] = host_frame[3]
        dataframe = (
            dataframe.dropna()
            .drop(["r_eventid", "r_clock"], axis=1)
            .reset_index(drop=True)
        )
        return dataframe

    adjusted_dataframe = host_series_adjustment(resolved_frame)

    # Correct column datatypes
    def correct_datatypes(args):
        dataframe = pd.DataFrame(args)
        dataframe["clock"] = pd.to_datetime(dataframe["clock"], unit="s").dt.strftime(
            "%m/%d/%Y %H:%M:%S"
        )
        dataframe["eventid"] = pd.Series(dataframe["eventid"]).astype(int)
        dataframe["severity"] = pd.Series(dataframe["severity"]).map(
            {
                "0": "Not Classified",
                "1": "Information",
                "2": "Warning",
                "3": "Average",
                "4": "High",
                "5": "Disaster",
            }
        )
        dataframe["rts_clock"] = pd.to_datetime(
            dataframe["rts_clock"], unit="s"
        ).dt.strftime("%H:%M:%S")

        return dataframe

    cleaned_data = correct_datatypes(adjusted_dataframe)

    return cleaned_data


def problems_by_severity(args):
    """ Generate a Pie Chart representing percentage of all problems """
    dataframe = pd.DataFrame(args["severity"])
    dataframe["colors"] = dataframe["severity"].copy()

    # Map color associations
    dataframe["colors"] = pd.Series(dataframe["colors"]).map(
        {
            "Not Classified": "#97AAB3",
            "Information": "#7499FF",
            "Warning": "#FFC859",
            "Average": "#FFA059",
            "High": "#E97659",
            "Disaster": "#E45959",
        }
    )

    labels = list(dataframe["severity"].value_counts().keys().tolist())
    values = list(dataframe["severity"].value_counts().tolist())
    colors = list(dataframe["colors"].value_counts().keys().tolist())

    trace = go.Pie(
        labels=labels,
        values=values,
        showlegend=False,
        hoverinfo="label+value",
        textinfo="label+percent",
        textfont=dict(size=14),
        hole=0.4,
        marker=dict(colors=colors, line=dict(color="#000000", width=2)),
    )

    layout = go.Layout(
        title=dict(text="Problems by Severity:", font=dict(size=24), xanchor="center")
    )

    fig = go.Figure(data=[trace], layout=layout)

    # plotly.offline.plot(fig, filename="problems_by_severity.html")
    return plotly.offline.plot(fig, output_type="div", include_plotlyjs=False)


def time_and_frequency(args):
    """ Generate Line chart showing total problems throughout the day """
    dataframe = pd.DataFrame(args["clock"])
    dataframe["clock"] = pd.to_datetime(dataframe["clock"]).dt.strftime("%H:%M")
    dataframe = dataframe["clock"].value_counts().sort_index()

    data = go.Scatter(
        x=dataframe.keys().tolist(),
        y=dataframe.tolist(),
        mode="lines",
        connectgaps=True,
    )

    layout = go.Layout(
        title=dict(text="Time of Frequency:", font=dict(size=24), xanchor="center")
    )

    fig = go.Figure(data=[data], layout=layout)

    # plotly.offline.plot(fig, filename='time_and_frequency.html')
    return plotly.offline.plot(fig, include_plotlyjs=False, output_type="div")


def problems_per_day(args):
    """ Generate Bar Graph representing issues per day """
    dataframe = pd.DataFrame(args["clock"])
    series_data = pd.to_datetime(dataframe["clock"]).dt.normalize()
    series_data = series_data.value_counts().sort_index()

    dates = series_data.keys().tolist()
    counts = series_data.tolist()

    data = [
        go.Bar(
            x=dates,
            y=counts,
            marker=dict(color="#7499FF", line=dict(color="#000000", width=1)),
            name="Problems",
        )
    ]
    layout = go.Layout(title="Problems per Day")
    fig = go.Figure(data=data, layout=layout)

    # plotly.offline.plot(fig, filename='problems_per_day.html')
    return plotly.offline.plot(fig, include_plotlyjs=False, output_type="div")


def generate_table(args):
    """ Generate Visual table for report """
    dataframe = pd.DataFrame(
        args[["eventid", "clock", "rts_clock", "severity", "hosts", "name"]]
    )

    trace = go.Table(
        name="Problems caught by Zabbix",
        columnwidth=[6, 11, 9, 8, 26, 40],
        header=dict(
            values=[
                "Event ID",
                "Timestamp",
                "Resolution Time",
                "Severity",
                "Host",
                "Event",
            ],
            line=dict(width=2),
            fill=dict(color="#C2D4FF"),
            align=["center"],
            font=dict(size=14),
        ),
        cells=dict(
            values=[
                dataframe.eventid,
                dataframe.clock,
                dataframe.rts_clock,
                dataframe.severity,
                dataframe.hosts,
                dataframe.name,
            ],
            fill=dict(color="#F5F8FF"),
            align=["left"] * 5,
        ),
    )

    layout = go.Layout(
        title=dict(
            text="Should there be a title here?", font=dict(size=24), xanchor="center"
        ),
        height=1800,
    )

    fig = go.Figure(data=[trace], layout=layout)

    # plotly.offline.plot(data, filename="generate_table.html")
    return plotly.offline.plot(fig, output_type="div", include_plotlyjs=False)


def generate_report(args):
    """ Generate report to be emailed out """
    env = Environment(loader=PackageLoader("prob_report", "templates"))
    comprise_template = env.get_template("problem_report.html")

    data = {
        "percentage_pie": problems_by_severity(args),
        "frequency_line": time_and_frequency(args),
        "per_day_bar": problems_per_day(args),
        "generated_table": generate_table(args),
    }

    compiled_report = comprise_template.render(page=data)

    with open("problem_report.html", "w") as file:
        file.write(compiled_report)


def main():
    """ Applicaton Logic """
    dataframe = clean_data(retrieve_data())
    generate_report(dataframe)


if __name__ == "__main__":
    main()
