*Final Project – Big Data Analytics*

# Free Power Hours

*Predicting Cheap and Negative Electricity Prices in Austria from the Weather*

## Topic

Electricity on the Austrian day-ahead market is becoming more volatile and negative more often. On sunny, windy weekends, prices regularly drop below zero, while still evenings can be very expensive. We want to find out which weather patterns drive these price swings, and whether tomorrow's weather forecast is enough to predict the cheap hours. Then we translate the result into concrete savings for households with a dynamic tariff (EV charging, heat pump).

## Team Members

| Name | Main Responsibility |
|---|---|
| Scheder Marcel | Data Source Layer (APIs, scraping, data quality) |
| Trappl Paul | Data Storage & Processing (MongoDB, Spark pipeline) |
| Yilmaz Tarik | Analytics & Presentation (ML model, visualizations, story) |

*GitHub Repository:*  https://github.com/free-power-hours/free-power-hours

## Planned Data Sources

| # | Source | Access | Content |
|---|---|---|---|
| 1 | **aWATTar API** | REST API (JSON), free, no key | Austrian day-ahead spot prices (EUR/MWh), hourly history (at least back to 2019) |
| 2 | **Open-Meteo Historical & Forecast API** | REST API (JSON), free | Hourly weather for several Austrian and German locations: solar radiation, cloud cover, wind speed, temperature, precipitation |
| 3 | **ENTSO-E Transparency Platform** | REST API (XML), security token | Actual generation by type (solar, wind, hydro, gas) and total load for Austria and Germany |
| 4 | **Public holidays** (holidays package) | Python library | Austrian and German holidays as demand features |

- **Why German weather too?** Austria is strongly interconnected with Germany (one joint price zone until October 2018), so German solar and wind output strongly influences Austrian prices.
- **Data quality checks planned:** missing hours, daylight-saving-time duplicates and gaps, outliers (extreme price spikes), and the change of market resolution from hourly to 15-minute intervals in autumn 2025, which will be resampled to a consistent time grid.

## Planned Data Storage

- **MongoDB (NoSQL)** using **time-series collections**, a feature designed for timestamped measurement data. Raw API responses are stored as-is (raw zone); cleaned and merged data is stored separately (curated zone).
- **Parquet flat files** as the exchange format for Spark processing and ML.
- Python scripts (pymongo) for import and export; a small ingestion script fetches new prices and forecasts daily (velocity aspect).
- Git repository for code sharing; Docker Compose for MongoDB and Spark so everyone has the same environment.

---

## Planned Procedure

1. **Ingest:** Fetch historical data (approx. 2019–2026) from all APIs and store it in MongoDB.
2. **Clean & merge (PySpark):** Unify time zones (UTC), resample to an hourly grid, and join prices, weather, generation and holidays.
3. **Exploratory analysis:** Price distributions, number of negative hours per year, price vs. solar and wind scatter plots, typical daily and weekly profiles, and correlations.
4. **Feature engineering (Spark window functions):** Lag features, rolling averages, hour, weekday, month, holiday flag, and "sun + wind" combined indicators.
5. **Machine learning (Spark MLlib / scikit-learn):**
   - Classification: Will a given hour be negative or "cheap"?
   - Regression: Predict the day-ahead price per hour.
   - Comparison against a naive baseline ("same hour last week"); time-based train/test split.
   - Feature importance to explain which weather factors matter most.
6. **Value analysis:** Simulate the savings of shifting EV charging or heat-pump usage into the predicted cheap hours, compared to a fixed tariff.
7. **Big Data reflection:** Evaluate the project against the 5 Vs (Volume, Velocity, Variety, Veracity, Value) and the 4 data-handling layers.

## Expected Output

- Jupyter notebooks documenting all four layers (code, explanations, sources, library versions).
- Visualizations: calendar heatmap of negative-price hours, interactive price vs. weather charts, and feature-importance plots.
- Prediction demo: "Tomorrow's cheap hours" based on the current weather forecast, shown live in the presentation.
- Savings calculator: how much a household could save per year by smart timing.
- Key story: "When the sun shines and the wind blows, electricity is free. Can we see it coming?"

## Required Components – Checklist

| Requirement | How we cover it |
|---|---|
| ≥ 2 data sources | aWATTar, Open-Meteo, ENTSO-E, holidays |
| Web API / scraping | Three REST APIs |
| NoSQL | MongoDB time-series collections |
| Spark | PySpark for cleaning, joining, feature engineering, MLlib |
| Machine learning | Classification (cheap or negative hour) + price regression |

## Open Points / Risks

- ENTSO-E API access must be requested by email (response within ~3 working days). Fallback: aWATTar and Open-Meteo alone are sufficient for the core analysis.
- aWATTar asks for fair use (about 100 requests per day), so we fetch historical data in monthly batches and cache it.
