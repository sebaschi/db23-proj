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
    SELECT COUNT(*), accidentyear AS year FROM accidents
    GROUP BY year
    ORDER BY year;
    """
    result = db.execute_query(acc_year_sql)
    result_df = pd.DataFrame(result)

    fig = px.bar(result_df, y='year', x='count', orientation='h', title='No. of Accidents per Year')
    fig.write_image("fig/acc_by_year.png")
    fig.write_html("html/acc_by_year.html")


def plt_acc_by_weekday(db):
    acc_weekday_sql = f"""
    SELECT COUNT(*) AS count, accidentweekday_en AS weekday
    FROM accidents
    GROUP BY weekday
    ORDER BY COUNT(*);
    """

    result = db.execute_query(acc_weekday_sql)
    result_df = pd.DataFrame(result)

    fig = px.bar(result_df, y='weekday', x='count', orientation='h', title='No. of Accidents per Weekday')
    fig.write_image("fig/acc_by_weekday.png")
    fig.write_html("html/acc_by_weekday.html")


# def plt_acc_by_day_year_old(db):
#     acc_year_day_sql = """
#     SELECT accidentyear AS year, accidentweekday_en AS weekday, COUNT(*) AS count
#     FROM accidents
#     GROUP BY weekday, year
#     ORDER BY weekday, year, COUNT(*);
#     """
#
#     result = db.execute_query(acc_year_day_sql)
#     resut_df = pd.DataFrame(result)


def plt_acc_by_day_year(db):
    acc_year_day_sql = """
    SELECT accidentyear AS year, accidentweekday_en AS weekday, COUNT(*) AS count
    FROM accidents
    GROUP BY weekday, year
    ORDER BY weekday, year, COUNT(*);
    """
    result = db.execute_query(acc_year_day_sql)
    df = pd.DataFrame(result)
    print(df.head())
    fig = px.bar(
        df,
        x='weekday',
        y='count',
        title='Accidents by Weekday',
        animation_frame='year',
        labels={'weekday': 'Weekday', 'count': 'Number of Accidents'},
        category_orders={'weekday': ['Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']},
    )
    fig.update_yaxes(range=[0, 1000])
    # Customize the layout to include a slider
    fig.update_layout(
        updatemenus=[
            {
                'buttons': [
                    {
                        'args': [None, {'frame': {'duration': 1000, 'redraw': True}, 'fromcurrent': True}],
                        'label': 'Play',
                        'method': 'animate',
                    },
                    {
                        'args': [[None], {'frame': {'duration': 0, 'redraw': True}, 'mode': 'immediate',
                                          'transition': {'duration': 0}}],
                        'label': 'Pause',
                        'method': 'animate',
                    },
                ],
                'direction': 'left',
                'pad': {'r': 10, 't': 87},
                'showactive': False,
                'type': 'buttons',
                'x': 0.1,
                'xanchor': 'right',
                'y': 0,
                'yanchor': 'top',
            }
        ],
        sliders=[{
            'active': 0,
            'yanchor': 'top',
            'xanchor': 'left',
            'currentvalue': {
                'font': {'size': 20},
                'prefix': 'Year:',
                'visible': True,
                'xanchor': 'right',
            },
            'transition': {'duration': 300, 'easing': 'cubic-in-out'},
            'pad': {'b': 10, 't': 50},
            'len': 0.9,
            'x': 0.1,
            'y': 0,
            'steps': [{'label': str(year), 'method': 'animate',
                       'args': [[year], {'frame': {'duration': 0, 'redraw': True}, 'mode': 'immediate'}]} for year in
                      sorted(df['year'].unique())],
        }],
    )
    fig.write_image("fig/plt_acc_by_day_year.png")
    fig.write_html("html/plt_acc_by_day_year.html")


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
def acc_by_type(db):
    acc_by_type_sql = """
    SELECT accidentyear AS year, accidenttype_en as type, count(*) as count
    FROM accidents
    GROUP BY year, type;
    """

    result = db.execute_query(acc_by_type_sql)
    result_df = pd.DataFrame(result)

    fig = px.bar(result_df, x='count', y='year', color='type', barmode='stack', orientation='h',title='Accidents by type')

    fig.update_layout(xaxis_title="No. of Accidents", yaxis_title="Year", legend_title="Accident Type")
    fig.write_image("fig/acc_by_type.png")
    fig.write_html("html/acc_by_type.html")
    #fig.show()


def severity_by_year(db):
    severity_by_year_sql = """
    SELECT accidentyear as year, accidentseveritycategory as code, severity, count(*) as count
    FROM accident_copy
    GROUP BY year, code, severity;
    """

    result = db.execute_query(severity_by_year_sql)
    result_df = pd.DataFrame(result)

    fig = px.bar(result_df, x='year', y='count', color='severity', barmode='group', orientation='v', title="Severity over the years")
    fig.update_layout(xaxis_title="Year", yaxis_title="No. of Accidents", legend_title="Accident Severity")
    fig.write_image("fig/severity_by_year.png")
    fig.write_html("html/severity_by_year.html")
    #fig.show()


def ped_by_month(db):
    ped_by_month_sql = """
    SELECT accidentyear AS year, accidentmonth AS month, count(*) as count
    FROM accidents
    WHERE accidentinvolvingpedestrian IS TRUE
    GROUP BY year, month
    ORDER BY year, month;
    """

    result = db.execute_query(ped_by_month_sql)
    result_df = pd.DataFrame(result)
    result_df['year-month'] = pd.to_datetime(result_df['year'].astype(str) + "-" + result_df['month'].astype(str))
    fig = px.line(result_df, x='year-month', y='count', markers=True)
    fig.update_layout(
        xaxis_title='Year',
        yaxis_title='No. of accidents',
        title='Accidents involving Pedestrians')
    fig.update_xaxes(tickmode='array', tickvals=result_df['year'], ticktext=result_df['year'])
    fig.write_image("fig/ped_by_month.png")
    fig.write_html("html/ped_by_month.html")

    #fig.show()
    #fig.write_html('ped_by_month.html')

def bike_by_month(db):
    bike_by_month_sql = """
    SELECT accidentyear AS year, accidentmonth AS month, count(*) as count
    FROM accidents
    WHERE accidentinvolvingbicycle IS TRUE
    GROUP BY year, month
    ORDER BY year, month;
    """

    result = db.execute_query(bike_by_month_sql)
    result_df = pd.DataFrame(result)
    result_df['year-month'] = pd.to_datetime(result_df['year'].astype(str) + "-" + result_df['month'].astype(str))
    fig = px.line(result_df, x='year-month', y='count', markers=True)
    fig.update_layout(
        xaxis_title='Year',
        yaxis_title='No. of accidents',
        title='Accidents involving Bicycles')
    fig.update_xaxes(tickmode='array', tickvals=result_df['year'], ticktext=result_df['year'])
    fig.write_image("fig/bike_by_month.png")
    fig.write_html("html/bike_by_month.html")
    #fig.show()

# TO TEDIOUS :/
# def acc_by_involved(db):
#     acc_by_involved_sql = """
#     SELECT accidentyear AS year, accidentmonth AS month, accidentinvolvingpedestrian AS ped,
#      accidentinvolvingbicycle as bike,
#      accidentinvolvingmotorcycle as moto,count(*) as count
#     FROM accidents
#     GROUP BY year, month, ped, bike, moto
#     ORDER BY year, month;
#     """
#
#     result = db.execute_query(acc_by_involved_sql)
#     result_df = pd.DataFrame(result)
#     result_df['year-month'] = pd.to_datetime(result_df['year'].astype(str) + "-" + result_df['month'].astype(str))
#
#     fig = px.line(result_df, x='year-month', y='count', color='')


def severity_by_month(db):
    severity_by_year_sql = """
    SELECT accidentyear as year, accidentmonth as month, severity, count(*) as count
    FROM accident_copy
    GROUP BY year, month, severity
    ORDER BY year, month;
    """

    result = db.execute_query(severity_by_year_sql)
    result_df = pd.DataFrame(result)
    result_df['year-month'] = pd.to_datetime(result_df['year'].astype(str) + "-" + result_df['month'].astype(str))
    fig = px.line(result_df, x='year-month', y='count', color='severity', orientation='v', title='Accident severity')
    #fig = px.bar(result_df, x='year', y='count', color='severity', barmode='group', orientation='v', title="Severity over the years")
    fig.update_layout(xaxis_title="Time", yaxis_title="No. of Accidents", legend_title="Accident Severity")
    fig.write_image("fig/severity_by_month.png")
    fig.write_html("html/severity_by_month.html")
    #fig.show()


# Utilities ===========================================================================================================
def save_as_barplot(df, xname, yname, orientation, file_name):
    pass


def save_as_html():
    pass


if __name__ == "__main__":
    remote_db = RemoteDB()
    try:
        plt_acc_by_year(remote_db)
        plt_acc_by_weekday(remote_db)
        plt_acc_by_daytime(remote_db)
        plt_acc_by_day_year(remote_db)
        ped_by_month(remote_db)
        acc_by_type(remote_db)
        severity_by_year(remote_db)
        severity_by_month(remote_db)
        bike_by_month(remote_db)
    except Exception as e:
        print(f"Exception {e} in plots.py")
    finally:
        remote_db.close()
