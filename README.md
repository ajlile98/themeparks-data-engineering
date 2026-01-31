# Themeparks Data Engineering

A Python data engineering pipeline for collecting and analyzing theme park data from the [themeparks.wiki API](https://api.themeparks.wiki/docs).

## Features

- Query destinations (theme park resorts worldwide)
- Get live wait times and attraction status
- Retrieve park schedules and operating hours
- Async-ready HTTP client with httpx

## Setup

This project uses [uv](https://docs.astral.sh/uv/) for package management.

```bash
# Install dependencies
uv sync

# Run the demo
uv run python src/themeparks_client.py
```

## API Endpoints Used

| Endpoint | Description |
|----------|-------------|
| `/destinations` | List all theme park destinations |
| `/entity/{id}` | Get details for a specific entity |
| `/entity/{id}/children` | Get child entities (attractions, shows, etc.) |
| `/entity/{id}/live` | Get live wait times and status |
| `/entity/{id}/schedule` | Get operating schedules |

## Project Structure

```
themeparks-data-engineering/
├── src/
│   └── themeparks_client.py  # API client and demo
├── pyproject.toml            # Project configuration
└── README.md
```

## Example Usage

```python
from src.themeparks_client import ThemeparksClient

with ThemeparksClient() as client:
    # Get all destinations
    destinations = client.get_destinations()
    
    # Get live data for a park
    live_data = client.get_entity_live("park-id-here")
    
    # Get schedule
    schedule = client.get_entity_schedule("park-id-here", year=2026, month=1)
```

## License

MIT
