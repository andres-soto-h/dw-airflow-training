from datetime import datetime
import pandas as pd
import pytz

def execution_date(self) -> pd.DataFrame:
        """
        Adds a new column to a pandas DataFrame with the current execution date.

        Returns
        -------
        pd.DataFrame
            The modified DataFrame with a new column called "fecha_ejecucion" containing the execution date.
        """
        fecha_actual = datetime.now(self.tz)
        fecha_formateada = fecha_actual.strftime("%Y-%m-%d")
        fecha_formateada=int(fecha_formateada.replace("-",""))
        self.df['fecha_ejecucion'] = fecha_formateada