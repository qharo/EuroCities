# EuroCity - Urban Accessibility Dashboard



https://github.com/user-attachments/assets/0f668dcb-32c9-41e7-8eac-b5eba47d259a



EuroCity is an interactive web application for analyzing and visualizing urban accessibility metrics across European cities. The dashboard provides insights into transit accessibility, building age distribution, and green space proximity.

## Project Structure

- `main.py`: Backend data processing pipeline using OpenStreetMap and PySpark
- `index.html`: Main web dashboard interface
- `Cities/`: Directory containing processed data for each city
  - Each city has two files:
    - `[city].geojson`: Building-level geospatial data with metrics
    - `[city]_city_metrics.json`: Aggregated city-level statistics

## Supported Cities

Currently includes data for 20 European cities including: Paris, Barcelona, Vienna, Amsterdam, Madrid, Milan, Rome, Brussels, and more.

## Getting Started

1. Clone the repository
2. Open `index.html` in a web browser
3. Select a city from the dropdown to start exploring urban metrics

For development or adding new cities, run `main.py` with the target city as a parameter.
