import logging

from db_connector import RemoteDB
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import kaleido
import numpy as np

logging.getLogger("matplotlib").setLevel(logging.WARNING)


# Summary charts ======================================================================================================
def plt_acc_by_year(db):
    acc_year_sql = """
    SELECT COUNT(*), accidentyear FROM accidents
    GROUP BY accidentyear
    ORDER BY accidentyear;
    """
    result = db.execute_query(acc_year_sql)
    result_df = pd.DataFrame(result)

    fig = px.bar(result_df, y='year', x='count', orientation='h', title='No. of Accidents per Year')
    fig.write_image("fig/acc_by_year.png")
    fig.write_html("html/acc_by_year.png")


def plt_acc_by_weekday(db):
    acc_weekday_sql = f"""
    SELECT COUNT(*) AS count, accidentweekday_en AS weekday
    FROM accidents
    GROUP BY accidentweekday_en
    ORDER BY COUNT(*);
    """

    result = db.execute_query(acc_weekday_sql)
    result_df = pd.DataFrame(result)

    fig = px.bar(result_df, y='weekday', x='Count', orientation='h', title='No. of Accidents per Weekday')
    fig.write_image("fig/acc_by_weekday.png")
    fig.write_html("html/acc_by_weekday.html")


def plt_acc_by_day_year(db):
    acc_year_day_sql = """
    SELECT accidentyear AS year, accidentweekday_en AS weekday, COUNT(*) AS count
    FROM accidents
    GROUP BY weekday, year
    ORDER BY year, COUNT(*);
    """


def plt_acc_by_daytime(db):
    acc_weekday_sql = f"""
    SELECT COUNT(*) AS count, accidenthour AS hour
    FROM accidents
    GROUP BY accidenthour
    ORDER BY COUNT(*);
    """

    result = db.execute_query(acc_weekday_sql)
    result_df = pd.DataFrame(result)

    fig = px.bar(result_df, y='hour', x='count', orientation='h')
    fig.write_image("fig/acc_by_day.png")
    fig.write_html("html/acc_by_day.html")

# Time Series charts ==================================================================================================
def acc_by_type():
    pass

def severity_by_year():
    pass

def ped_by_month():
    pass

def bike_by_month():
    pass


def severity_by_month():
    pass




# Utilities ===========================================================================================================
def save_as_barplot(df, xname, yname, orientation, file_name):
    pass


def save_as_html():
    pass


if __name__ == "__main__":
    remote_db = RemoteDB()
    try:
        # plt_acc_by_year(remote_db)
        # plt_acc_by_weekday(remote_db)
        plt_acc_by_daytime(remote_db)
    except Exception as e:
        print(f"Exception {e} in plots.py")
    finally:
        remote_db.close()
