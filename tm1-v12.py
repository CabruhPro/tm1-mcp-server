from typing import Any
import httpx
from mcp.server.fastmcp import FastMCP
from TM1py.Services import TM1Service
from TM1py.Objects import Dimension,Element,Hierarchy,Cube
from dotenv import load_dotenv
import os

# Initialize FastMCP server
mcp = FastMCP("weather")

# Constants
NWS_API_BASE = "https://api.weather.gov"
USER_AGENT = "weather-app/1.0"

load_dotenv()
params = {
    "base_url": "https://us-east-1.planninganalytics.saas.ibm.com/api/RGLC2XR62EDS/v0/tm1/Watson_Integration/",
    "user": "apikey",
    "password": os.getenv("V12_API_KEY"),
    "async_requests_mode": True,
    "ssl": True,
    "verify": True
}

async def make_nws_request(url: str) -> dict[str, Any] | None:
    """Make a request to the NWS API with proper error handling."""
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "application/geo+json"
    }
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url, headers=headers, timeout=30.0)
            response.raise_for_status()
            return response.json()
        except Exception:
            return None

def format_alert(feature: dict) -> str:
    """Format an alert feature into a readable string."""
    props = feature["properties"]
    return f"""
Event: {props.get('event', 'Unknown')}
Area: {props.get('areaDesc', 'Unknown')}
Severity: {props.get('severity', 'Unknown')}
Description: {props.get('description', 'No description available')}
Instructions: {props.get('instruction', 'No specific instructions provided')}
"""

@mcp.tool()
async def get_alerts(state: str) -> str:
    """Get weather alerts for a US state.

    Args:
        state: Two-letter US state code (e.g. CA, NY)
    """
    url = f"{NWS_API_BASE}/alerts/active/area/{state}"
    data = await make_nws_request(url)

    if not data or "features" not in data:
        return "Unable to fetch alerts or no alerts found."

    if not data["features"]:
        return "No active alerts for this state."

    alerts = [format_alert(feature) for feature in data["features"]]
    return "\n---\n".join(alerts)

@mcp.tool()
async def get_forecast(latitude: float, longitude: float) -> str:
    """Get weather forecast for a location.

    Args:
        latitude: Latitude of the location
        longitude: Longitude of the location
    """
    # First get the forecast grid endpoint
    points_url = f"{NWS_API_BASE}/points/{latitude},{longitude}"
    points_data = await make_nws_request(points_url)

    if not points_data:
        return "Unable to fetch forecast data for this location."

    # Get the forecast URL from the points response
    forecast_url = points_data["properties"]["forecast"]
    forecast_data = await make_nws_request(forecast_url)

    if not forecast_data:
        return "Unable to fetch detailed forecast."

    # Format the periods into a readable forecast
    periods = forecast_data["properties"]["periods"]
    forecasts = []
    for period in periods[:5]:  # Only show next 5 periods
        forecast = f"""
{period['name']}:
Temperature: {period['temperature']}°{period['temperatureUnit']}
Wind: {period['windSpeed']} {period['windDirection']}
Forecast: {period['detailedForecast']}
"""
        forecasts.append(forecast)

    return "\n---\n".join(forecasts)

@mcp.tool()
async def get_cubes_tm1():
    """
    Get List of cubes from TM1 server.

    Args:
        none
    Returns:
        List of cube names
    """
    tm1 = TM1Service(**params)
    print("Connected Successfully: ",tm1.server.get_product_version())
    return tm1.cubes.get_all_names()

@mcp.tool()
async def create_dim_tm1(dim_name: str):
    """
    Creates a new dimension on the TM1 server.

    Args:
        dim_name: Name of the dimension to create
    Returns:
        str: Success message
    """
    tm1 = tm1 = TM1Service(**params)
    new_hier=Hierarchy(dimension_name=dim_name,name=dim_name)
    new_dim=Dimension(dim_name, hierarchies=[new_hier])
    tm1.dimensions.create(new_dim)
    return "Success!"

@mcp.tool()
async def add_dim_elements_tm1(dim_name: str, elements: list, el_type: str):
    """
    Adds elements to a dimension on the TM1 server.

    Args:
        dim_name: Name of the dimension to add elements to
        elements: List of element names to add
        el_type: Type of the new elements (e.g. "Numeric", "String")
    Returns:
        str: Success message
    """
    tm1 = tm1 = TM1Service(**params)
    dim = tm1.dimensions.get(dim_name)
    for item in elements:
        dim.default_hierarchy.add_element(element_name=item,element_type=el_type)
    tm1.dimensions.update(dimension=dim,keep_existing_attributes=True)
    return "Success!"

@mcp.tool()
async def create_cube_tm1(cube_name: str, dimensions: list):
    """
    Creates a new cube on the TM1 server.
    Args:
        cube_name: Name of the cube to create
        dimensions: List of dimension names for the cube, order matters - measures should be always last and dimensions should be in relative size order - ascending
    Returns:
        str: Success message
    """
    tm1 = TM1Service(**params)
    new_cube=Cube(name=cube_name, dimensions=dimensions)
    tm1.cubes.create(new_cube)
    return "Success!"

# @mcp.tool()
# async def write_to_tm1_cube(value: float, cube_name: str, at_intersection: list):
#     """
#     Writes a value to a specific cell in a TM1 cube.
    
#     Args:
#         cube_name: Name of the cube to write to
#         value: Value to write
#         coordinates: List of dimension coordinates for the cell
#     Returns:
#         str: Success message
#     """

#     tm1 = TM1Service(address='vm-training.acg.local', port=26471, user='admin', password='', ssl=True)
    
#     current_cube=tm1.cubes.get(cube_name)
#     dim_list=current_cube.dimensions
#     dim_validate=True

#     for dim in dim_list:
#         if   at_intersection[dim_list.index(dim)] ... needs more work
#             dim_validate=False
    
#     tm1.cubes.cells.write(value=value, cube_name=cube_name, coordinates=at_intersection)
#     return "Success!"

if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')