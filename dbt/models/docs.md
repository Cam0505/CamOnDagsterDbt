
# A
{% docs alcoholic_type %}
    The alcoholic content type of the beverage. The possible values are:
    - `Non alcoholic`: The beverage contains no alcohol.
    - `Optional alcohol`: The beverage can optionally include alcohol.
    - `Alcoholic`: The beverage contains alcohol.
{% enddocs %}
# B
{% docs base_geo_description %}
    The `base_geo` model contains foundational geographic data that can be used to enrich other datasets with location context. It includes information like city name, country, coordinates, and region grouping.

    This model is useful for joining geographic metadata into analytical datasets.
{% enddocs %}

{% docs base_uv_description %}
    The `base_uv` model contains UV index data for various locations, along with ozone levels. It is useful for analyzing the intensity of UV radiation and its relationship to environmental factors such as ozone.

    This model is useful when working with environmental data, weather patterns, or other geospatial analytics.
{% enddocs %}

{% docs base_weather_description %}
    The `base_weather` model contains weather-related data for various locations. It includes metrics like temperature, humidity, precipitation, and wind speed, useful for analyzing and predicting weather patterns and environmental conditions.

    This model can be useful in a variety of analytical contexts, including climate modeling, forecasting, and geospatial analysis.
{% enddocs %}

{% docs base_beverages %}
    The `base_beverages` model contains data related to various beverages sourced from TheCocktailDB API.
    The dataset includes details about the beverage name, type, category, and alcoholic content, as well as unique identifiers for each beverage.
    This model provides valuable information for analyzing drink preferences, categories, and alcoholic content.
{% enddocs %}

{% docs beverage_name %}
    The name of the beverage as defined in TheCocktailDB API. This includes cocktails, mocktails, and other types of beverages.
{% enddocs %}

{% docs beverage_id %}
    A unique identifier for each beverage. This ID is used to uniquely identify a beverage entry in the database.
{% enddocs %}

{% docs beverage_type %}
    The type of the beverage (e.g., Cocktail, Shot, etc.). This helps categorize beverages for easier filtering and analysis.
{% enddocs %}

{% docs beverage_category_sk %}
    The category of the beverage, such as "Ordinary Drink", "Cocktail", "Shot", etc. This field is useful for grouping beverages based on category.
{% enddocs %}
# C
{% docs city %}
The name of the city. This field is typically used as a primary identifier for a location in the dataset.
{% enddocs %}

{% docs country %}
The country where the city is located. Useful for aggregating or filtering by national boundaries.
{% enddocs %}

{% docs continent %}
The continent where the city is located. This field helps in broader geographic aggregations and global segmentation.
{% enddocs %}
# D
# E
# F
# G
{% docs glass_type %}
    The specific glass type used for serving the beverage, such as "Collins glass", "Margarita glass", or "Highball glass". This provides more detailed information about how to serve the drink.
{% enddocs %}
# H
{% docs humidity %}
    The relative humidity of the air at a given location, measured as a percentage (%). High humidity levels can affect comfort and play a role in weather events like precipitation.
{% enddocs %}
# I
# J
# K
# L
{% docs latitude %}
The latitude coordinate of the city in decimal degrees. Positive values indicate locations north of the equator.
{% enddocs %}

{% docs longitude %}
    The longitude coordinate of the city in decimal degrees. Positive values indicate locations east of the Prime Meridian.
{% enddocs %}
# M
# N
# O
{% docs ozone %}
    The concentration of ozone in the atmosphere, typically measured in Dobson Units (DU). Ozone plays a key role in absorbing UV radiation, and its concentration can influence the level of UV exposure.
{% enddocs %}
# P
{% docs precipitation_sum %}
    The total amount of precipitation (rain, snow, etc.) that has fallen over a given period, measured in millimeters (mm) or inches (in). This value helps to track overall rainfall or snowfall accumulation, which is important for understanding seasonal or long-term trends in precipitation.
{% enddocs %}

# Q
# R
{% docs region %}
    A higher-level geographic grouping, such as a continent or internal administrative region. Optional but useful for rollups and segmentation.
{% enddocs %}
# S
{% docs sunshine_duration %}
    The total duration of sunshine at a given location over a specific period, typically measured in hours. This metric is important for assessing how much sunlight an area receives, which can affect agriculture, energy production, and climate conditions.
{% enddocs %}
# T
{% docs temperature_mean %}
    The mean temperature at a given location, averaged over a specific period (e.g., daily, weekly). This value helps in assessing general weather conditions and trends over time, providing a more stable representation of temperature compared to single-point measurements.
{% enddocs %}

{% docs temperature_max %}
    The maximum temperature at a given location, typically measured during a specific time frame (e.g., daily or hourly). This value represents the highest temperature recorded and is often used to analyze extreme weather conditions or heat events.
{% enddocs %}
# U
{% docs uv %}
    The ultraviolet (UV) index value representing the strength of UV radiation at a given location. The UV index typically ranges from 0 (minimal exposure) to 11+ (extreme exposure), with higher values indicating stronger radiation.
{% enddocs %}
# V
# W
{% docs wind_speed %}
    The speed of wind at a given location, typically measured in kilometers per hour (km/h) or miles per hour (mph). This value is crucial for forecasting storm conditions and understanding general weather dynamics.
{% enddocs %}

{% docs windgusts_max %}
The maximum wind gust recorded at a given location during a specific time period. Wind gusts are short bursts of high-speed wind and can be important indicators of severe weather events such as storms or tornadoes.
{% enddocs %}
# X
# Y
# Z