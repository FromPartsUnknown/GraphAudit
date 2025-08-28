# GraphAudit

[![Python](https://img.shields.io/badge/python-3.7%2B-blue.svg)](https://www.python.org/downloads/)
[![Microsoft Graph](https://img.shields.io/badge/Microsoft-Graph%20API-orange.svg)](https://developer.microsoft.com/en-us/graph)

GraphAudit is a security auditing and monitoring tool for Microsoft Entra ID (formerly Azure AD). It leverages the Microsoft Graph API to collect data on Service Principals, Applications, role assignments, and directory roles. The tool loads this data into an in-memory DuckDB database for fast analysis and runs customisable SQL-based detection templates to identify risks and misconfigurations.

Key use cases include detecting Service Principals with dangerous permissions (e.g., to Microsoft Graph) and third-party applications with elevated access. GraphAudit also includes a diff mode to monitor changes to Service Principal credentials over time, acting like a tripwire for unauthorised modifications.

<p align="center">
<img src="https://drive.google.com/uc?export=view&id=1RRd2_dUUB-Iz5QVbBe2DNHcbw6dt0g0J"/>
</p>

## ✨ Features

- **Automated Data Collection**: Asynchronously fetches Service Principals, Applications, and related objects using  the official Microsoft Graph Python SDK (Kiota) with retry logic and batch processing to avoid throttling.

- **Customisable Detections**: Write YAML templates with embedded SQL queries. GraphAudit will run them against the in-memory database, fetch enriched SP objects, and render results.

- **In-Memory Analytics**  
  Fast analysis using DuckDB in-memory database. Stores data on disk in both DuckDB and SQLite formats for flexibility.

- **Credential Change Detection**: Acts like a lightweight Tripwire for Service Principals by hashing credential sets and alerting on additions, removals, or modifications.

- **Rich Terminal Output**: Formats results with colourful tables via the Rich library, configurable through JMESPath expressions in `render_config.yaml`.

- **Export Options**: Save full JSON objects for detected items to a file for further analysis.

- **Authentication Caching**: Optional token caching to skip repeated browser logins.

## 🧩 How It Works

GraphAudit follows a simple workflow to turn raw Graph data into actionable security insights.

```
┌───────────────────┐      ┌─────────────────┐      ┌──────────────────┐
│ Microsoft Graph   │ ───> │  GraphCrawler   │ ───> │    GraphData     │
│ API (Async)       │      │ (Data Collector)│      │ (DuckDB/SQLite)  │
└───────────────────┘      └─────────────────┘      └──────────────────┘
                                                             │
                                                             ▼
┌───────────────────┐      ┌─────────────────┐      ┌──────────────────┐
│ Rich Terminal     │ <─── │   Detection     │ <─── │ DetectionFactory │
│ Output (Rendered) │      │ (Runs SQL Query)│      │ (Templates)      │
└───────────────────┘      └─────────────────┘      └──────────────────┘
```

1.  **GraphCrawler**: Asynchronously fetches Service Principal, Application, and related objects from the MS Graph API.
2.  **GraphData**: Manages the in-memory DuckDB database and persistence to disk. It also enriches objects with related data (role assignments, application details, etc.).
3.  **DetectionFactory & Detection**: Loads detection logic from your YAML templates and creates detection instances which execute SQL queries against the in-memory database.
4.  **ScreenRender**: Takes the query results and formats them into clean, readable tables in your terminal.

## 🔧 Installation

```bash
git clone https://github.com/FromPartsUnknown/GraphAudit.git
cd GraphAudit
python3 -m venv .venv
source .venv/bin/activate   
pip install .
```
*(On Windows, activate the virtual environment with `.venv\Scripts\activate`)*


## 🚦 Quick Start

### 1\. Collect Graph Data

Perform the initial data collection from your tenant. This will create a `graph_data.db` file in your directory. You'll be prompted to authenticate in your browser.

```bash
graphaudit --collect
```

*Tip: Use `--auth-cache` on subsequent runs to avoid logging in every time.*

### 2\. Run Detections

Run all detection templates located in the `detections/` directory against the cached data. 

```bash
graphaudit
```

### 3\. Detect Credential Changes

Run GraphAudit in diff mode to compare the current state against the last collection and report any changes to Service Principal credentials.

```bash
graphaudit --diff
```

## ⚙️ Command Line Options

| Option | Description |
|--------|-------------|
| `--collect` | Fetch fresh data from Microsoft Graph API |
| `--diff` | Compare current data with previous collection to detect changes |
| `--dt-path` | Path to detection templates (directory or specific YAML file) |
| `--db-path` | Custom database file location (default: graph_data.db) |
| `--auth-cache` | Cache authentication credentials  |
| `--debug-count` | Limit Service Principals collected for testing |
| `--output-file` | Export detailed JSON results to file |

## 📄 Detection Templates

Detections are defined in YAML files. Each template specifies a SQL query to identify risky principals and an output configuration to display the findings to terminal.

By default templates are loaded from the `detections` directory. You can specify a different path or individual file using the `--dt-path` command line option. 

```yaml
name: "Detection Name"
description: |
  Multi-line description explaining what this detection identifies
  and the security implications.

query: |
  SELECT DISTINCT sp_id
  FROM service_principals sp
  WHERE sp.condition = 'value'
  AND sp.accountEnabled = 1
  
output:
  - type: table
    title: "Detection Results"
    columns:
      -
        - data_view: "service_principal.displayName"
        - data_view: "service_principal.appRoleAssignments[]"
```
- **query** → The DuckDB SQL query to execute. This query should return a list of Service Principal ids that match the detection criteria.
    
    **Tip**: To explore the data and schema for writing queries, you can open the generated graph_data.db file with a tool like DB Browser for SQLite.

- **output** →  Defines the layout for the results table in the terminal. The data_view keys are JMESPath expressions used to extract data from the final, enriched Service Principal object. The display titles and styles for these paths are configured in `config/render_config.yaml`.

### Available Database Tables

Detection queries can reference the following tables populated by GraphCrawler:

- `service_principals` - Core SP data
- `applications` - Application data
- `app_role_assignments` - Outbound role assignments from SPs
- `app_role_assigned_to` - Inbound role assignments to SPs
- `app_roles` - Available application roles and permissions
- `sp_oauth_grants` - OAuth2 permission grants
- `sp_member_of` - Directory role memberships

### Data Enrichment

GraphData performs automatic enrichment of Service Principal objects:

- `appRoleImports` - Roles assigned TO this SP (permissions it has)
- `appRoleExports` - Roles assigned BY this SP (permissions it grants)
- `oauth2PermissionGrants` - OAuth delegated permissions
- `application` - Linked application registration with enriched `requiredResourceAccess`
- `member_of` - Directory role memberships

### Customising Output

The visual presentation of detection results in the terminal—such as titles, comments, and property names—is controlled by the config/render_config.yaml file. You can edit this file to change how data is displayed without altering the detection logic.

### Built-in Detections

See the detections directory:

- **Graph Permissions**: Finds enabled SPs with Graph app roles and client credentials.
- **Directory Roles**: Identifies SPs with membership to a directory role and client credentials. 
- **Third-Party Apps**: Detects external applications with permissions or app role assignments.

