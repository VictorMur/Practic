import pandas as pd
from sys import path
import schedule
import os
def imp_data():
    path.append("C:\\Program Files (x86)\\Microsoft.NET\\ADOMD.NET\\160")
    from pyadomd import Pyadomd

    model_name = 'TPS_SKU'
    port_number = r'RUC\T'

    connection_string = f'Provider=MSOLAP;Data Source={port_number};Catalog={model_name};'

    dax_query = """
    /* START QUERY BUILDER */
    EVALUATE
    SUMMARIZECOLUMNS(
        'Date'[Year],
        'Date'[MonthNumber, Year],
        TPS_SKU_FACTS[Complex Product],
        TPS_SKU_FACTS[Distribution Channel],
        TPS_SKU_FACTS[Partner Code],
        'Final Substitution'[Mark code],
        "SI Lor RuR", [SI Lor RuR],
        "SO Lor RuR", [SO Lor RuR],
        "SI UN", [SI UN],
        "SO UN", [SO UN]
    )
    ORDER BY 
        'Date'[Year] ASC,
        'Date'[MonthNumber, Year] ASC,
        TPS_SKU_FACTS[Complex Product] ASC,
        TPS_SKU_FACTS[Distribution Channel] ASC,
        TPS_SKU_FACTS[Partner Code] ASC,
        'Final Substitution'[Mark code] ASC
    /* END QUERY BUILDER */
    """
    con = Pyadomd(connection_string)
    con.open()
    result = con.cursor().execute(dax_query)

    folder_path = "C:\\Users\\victor.murlykov\\OneDrive - L'Oréal\\Bi"
    file_name = "Data SI.xlsx"
    col_names = [i.name for i in result.description]
    df = pd.DataFrame(result.fetchall(), columns=col_names)

    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, file_name)
    with open(file_path, "wb") as file:
        df.to_excel(file, index=False, header=True)

    con.close()


schedule.every().day.at("11:22").do(imp_data)

while True:
    schedule.run_pending()
