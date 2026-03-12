import pandas as pd
from sqlalchemy import create_engine
import urllib

# Configuración SQL Server (Origen)
params = urllib.parse.quote_plus(
    r'DRIVER={ODBC Driver 17 for SQL Server};'
    r'SERVER=192.10.10.30,1433;' # Corregí el puerto con coma si es necesario
    r'DATABASE=BackOfficeMartelPDV;'
    r'UID=sa;'
    r'PWD=admin'
)
mssql_engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# Configuración Supabase (Destino)
# Reemplaza con tus datos reales de Supabase (Settings -> Database)
supabase_url = "postgresql://postgres:[TU_PASSWORD]@db.[TU_REF].supabase.co:5432/postgres"
supabase_engine = create_engine(supabase_url)

def run_sync():
    print("Extrayendo datos de SQL Server...")
    query = "SELECT * FROM dbo.VENTASUCURSALFECHA"
    df = pd.read_sql(query, mssql_engine)
    
    print(f"Cargando {len(df)} filas a Supabase...")
    # 'replace' vacía la tabla y la vuelve a llenar
    df.to_sql('ventas_sucursal_fecha', supabase_engine, if_exists='replace', index=False)
    print("¡Sincronización completa!")

if __name__ == "__main__":
    run_sync()