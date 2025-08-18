from typing import Any
#import httpx
from mcp.server.fastmcp import FastMCP
from TM1py.Services import TM1Service
from TM1py.Objects import Dimension,Element,Hierarchy,Cube
#from dotenv import load_dotenv
#import os

# Initialize FastMCP server
mcp = FastMCP("tm1-v12")

# Constants
# NWS_API_BASE = "https://api.weather.gov"
# USER_AGENT = "weather-app/1.0"

V12_API_KEY = "azE6dXNyX2QxY2ViYzI1LTQ2YmQtM2RlNS1hZTZkLTQxMmNlOGEwZjE1ODo2aE04a3NYUTJ5cnl1MHpjV29VQ1ZXVWxEcVFYdlBCanUxa0ZXZWJJVS80PTpDdDZy"

params = {
    "base_url": "https://us-east-1.planninganalytics.saas.ibm.com/api/RGLC2XR62EDS/v0/tm1/Watson_Integration/",
    "user": "apikey",
    "password": V12_API_KEY, #os.getenv("V12_API_KEY"),
    "async_requests_mode": True,
    "ssl": True,
    "verify": True
}
tm1 = TM1Service(**params)
    
##=============================================================== GETS ===============================================================================

@mcp.tool()
async def get_all_cubes_tm1():
    """
    Get all cubes from the TM1 server.
    Returns:
        List of cube names
    """
    cubes = tm1.cubes.get_all()
    return cubes

@mcp.tool()
async def get_all_dimensions_tm1():
    """
    Get all dimensions from the TM1 server.
    Returns:
        List of dimension names
    """
    dimensions_list = tm1.dimensions.get_all_names()
    return dimensions_list

@mcp.tool()
async def get_dimension_tm1(dim_name: str):
    """
    Get a dimension from the TM1 server.
    Returns:
        A Dimension object
    """
    dimension = tm1.dimensions.get(dimension_name=dim_name)
    return dimension

@mcp.tool()
async def get_dimensions_in_cube_tm1(cube_name: str):
    """
    Get all dimensions in a specific cube.
    Args:
        cube_name: Name of the cube
    Returns:
        List of dimension names in the cube
    """
    cube = tm1.cubes.get(cube_name=cube_name)
    return cube.dimensions

@mcp.tool()
async def get_all_elements_in_dimension_tm1(dim_name: str):
    """
    Get all elements in a specific dimension.
    Args:
        dim_name: Name of the dimension
    Returns:
        List of element names
    """
    elements = tm1.elements.get_elements(dimension_name=dim_name, hierarchy_name=dim_name)
    return elements

@mcp.tool()
async def get_view_from_cube_tm1(cube_name: str, view_name: str):
    """
    Get a view from a cube.
    Args:
        cube_name: Name of the cube
        view_name: Name of the view
    Returns:
        The view object or its MDX string
    """
    view_csv = tm1.cubes.cells.execute_view_csv(cube_name=cube_name, view_name=view_name)
    return view_csv

##=============================================================== EXECUTE =============================================================================

@mcp.tool()
async def execute_mdx_view_on_cube_tm1(mdx: str):
    """
    Execute an MDX view on a cube.
    Args:
        mdx: MDX query string
    Returns:
        Query result as a list of dictionaries
    """
    view_csv = tm1.cubes.cells.execute_mdx(mdx=mdx)
    return view_csv

# @mcp.tool()
# async def execute_mdx_view_on_dimension_tm1(mdx: str):
#     """
#     Execute an MDX view on a dimension.
#     Args:
#         mdx: MDX query string
#     Returns:
#         Query result as a list of dictionaries
#     """
#     return tm1.dimensions.hierarchies.elements.execute_mdx(mdx)

##=============================================================== CREATE ===============================================================================

@mcp.tool()
async def create_cube_tm1(cube_name: str, dimensions: list):
    """
    Creates a new cube on the TM1 server.
    Args:
        cube_name: Name of the cube to create
        dimensions: List of dimension names for the cube, order matters - measures should be always last and dimensions should be in relative size order - ascending
    Returns:
        Cube: the new tm1 cube object
    """
    new_cube=Cube(name=cube_name, dimensions=dimensions)
    tm1.cubes.create(new_cube)
    return new_cube

@mcp.tool()
async def create_dimension_tm1(dim_name: str):
    """
    Creates a new dimension on the TM1 server.

    Args:
        dim_name: Name of the dimension to create
    Returns:
        Dimension: the new tm1 dimension object
    """
    new_hier=Hierarchy(dimension_name=dim_name,name=dim_name)
    new_dim=Dimension(dim_name, hierarchies=[new_hier])
    tm1.dimensions.create(new_dim)
    return new_dim

##================================================================ WRITE ===============================================================================

@mcp.tool()
async def add_dimension_elements_tm1(dim_name: str, elements: list, el_type: str):
    """
    Adds elements to a dimension on the TM1 server.

    Args:
        dim_name: Name of the dimension to add elements to
        elements: List of element names to add
        el_type: Type of the new elements (e.g. "Numeric", "String")
    Returns:
        dim: the Dimension object we have inserted into
    """
    dim = tm1.dimensions.get(dim_name)
    for item in elements:
        dim.default_hierarchy.add_element(element_name=item,element_type=el_type)
    tm1.dimensions.update(dimension=dim,keep_existing_attributes=True)
    return dim

@mcp.tool()
async def write_to_cube_tm1(value: float, cube_name: str, at_intersection: list):
    """
    Writes a value to a specific cell in a TM1 cube.
    
    Args:
        cube_name: Name of the cube to write to
        value: Value to write
        coordinates: List of dimension coordinates for the desired cell
    Returns:
        str: Success message
    """
    current_cube=tm1.cubes.get(cube_name)
    dim_list=current_cube.dimensions
    dim_validate=True

    for dim in dim_list:
        if tm1.elements.exists(dim, dim, at_intersection[dim_list.index(dim)])!=True:
            dim_validate=False
    
    if dim_validate==True:
        tm1.cubes.cells.write_value(value=value, cube_name=cube_name, element_tuple=at_intersection)
        return "Success!"
    else:
        return "Failed, check dimension element order"
    
#main method - starts MCP server
if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')