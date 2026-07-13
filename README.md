# Detroit Land Information Archive

## Environment Variables

Create a `.env` file in the project root with the following variables:

```
# Database connection (required)
DLIA_DB_USER=          # PostgreSQL username (required)
DLIA_DB_PASSWORD=      # PostgreSQL password (optional, omit for peer/ident auth)
DLIA_DB_HOST=          # Database host (default: edw)
DLIA_DB_PORT=          # Database port (default: 5432)

# Data source paths (optional, defaults shown)
VAULT_PATH=            # Path to vault storage (default: /mnt/v)
PROPRIETARY_PATH=      # Path to proprietary data (default: /mnt/s/1_PROPRIETARY/PROPRIETARY)
DUA_PATH=              # Path to DUA data (default: /mnt/q)
```
