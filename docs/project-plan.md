# Project Plan

## Procedure

1. **Ingest (Data Source Layer):** Fetch history (2019–2026) from the APIs, cache the raw responses and publish them to Kafka. A live producer adds new prices and forecasts continuously.
2. **Streaming into storage (Spark Structured Streaming):** Consume the Kafka topics, parse the JSON and upsert by key into MongoDB time-series collections.
3. **Clean & merge (Spark batch):** Check the time grid (UTC), handle gaps and outliers, join prices, weather, generation and holidays. Write Parquet snapshots for ML.
4. **Exploratory analysis:** Price distributions, number of negative hours per year, price vs. solar and wind scatter plots, typical daily and weekly profiles, and correlations.
5. **Feature engineering (Spark window functions):** Lag features, rolling averages, hour, weekday, month, holiday flag, and "sun + wind" combined indicators.
6. **Machine learning (Spark MLlib / scikit-learn):**
   - Classification: Will a given hour be negative or "cheap"?
   - Regression: Predict the day-ahead price per hour.
   - Comparison against a naive baseline ("same hour last week"); time-based train/test split; trained on *historical forecasts* (≈ 2022 onward).
   - Feature importance to explain which weather factors matter most.
7. **Live prediction:** New forecasts arrive via `openmeteo.forecast` and are scored to show "tomorrow's cheap hours".
8. **Value analysis:** Simulate the savings of shifting EV charging or heat-pump usage into the predicted cheap hours, compared to a fixed tariff.
9. **Big Data reflection:** Evaluate the project against the 5 Vs (Volume, Velocity, Variety, Veracity, Value) and the 4 data-handling layers.

## Expected Output

- Jupyter notebooks documenting all four layers (code, explanations, sources, library versions).
- Visualizations: calendar heatmap of negative-price hours, interactive price vs. weather charts, and feature-importance plots.
- Prediction demo: "Tomorrow's cheap hours" based on the current weather forecast, streamed live during the presentation.
- Savings calculator: how much a household could save per year by smart timing.

## Milestones

| When | Deliverable |
|---|---|
| Class 4 | Topic submission (title, team, sources, storage, procedure, expected output) |
| Class 6 | Intermediate status discussion |
| Class 8 | Final delivery in Moodle + 10-minute team presentation |