import logging

from db_connector import RemoteDB
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import kaleido
import numpy as np

logging.getLogger("matplotlib").setLevel(logging.WARNING)

# TODO
def plt_acc_by_year(db):
    acc_year_sql = """
    SELECT COUNT(*), accidentyear FROM accidents
    GROUP BY accidentyear
    ORDER BY accidentyear;
    """
    result = db.execute_query(acc_year_sql)
    result_df = pd.DataFrame(result)
    plt.barh(result_df['accidentyear'],result_df['count'])
    plt.ylabel('Year')
    plt.xlabel('No. of Accidents')
    plt.show()


def plt_acc_by_weekday(db):
    acc_weekday_sql = f"""
    SELECT COUNT(*) AS count, accidentweekday_en AS weekday
    FROM accidents
    GROUP BY accidentweekday_en
    ORDER BY COUNT(*);
    """

    result = db.execute_query(acc_weekday_sql)
    result_df = pd.DataFrame(result)

    plt.barh(result_df['weekday'], result_df['count'])
    plt.ylabel('Weekday')
    plt.xlabel('No. of Accidents')
    plt.show()




def plt_acc_by_daytime(db):
    acc_weekday_sql = f"""
    SELECT COUNT(*) AS count, accidenthour AS hour
    FROM accidents
    GROUP BY accidenthour
    ORDER BY COUNT(*);
    """

    result = db.execute_query(acc_weekday_sql)
    result_df = pd.DataFrame(result)

    plt.barh(result_df['hour'], result_df['count'])
    plt.ylabel('hour')
    plt.xlabel('No. of Accidents')
    plt.show()

    fig = px.bar(result_df, y='hour', x='count', orientation='h')
    fig.write_image("fig/acc_by_day.png")
    fig.write_html("html/acc_by_day.html")


if __name__ == "__main__":
    remote_db = RemoteDB()
    try:
        #plt_acc_by_year(remote_db)
        #plt_acc_by_weekday(remote_db)
        plt_acc_by_daytime(remote_db)
    except Exception as e:
        print(f"Exception {e} in plots.py")
    finally:
        remote_db.close()

