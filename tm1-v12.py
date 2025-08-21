from typing import Any
#import httpx
from mcp.server.fastmcp import FastMCP
from TM1py.Services import TM1Service
from TM1py.Objects import Dimension,Element,Hierarchy,Cube,Process,Chore,ChoreTask,ChoreFrequency,ChoreStartTime
#from dotenv import load_dotenv
#import os

#Current Outstanding
#   -create a chore of them with configs
#   -delete process, cube, dimension, dimension elements
#
#Business workflow
#   -create views and export them to csv
#       time, prod, currency, measure
#   -import csvs into views

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

@mcp.tool()
async def get_data_in_cube(cube_name: str, intersect: list):
    """
    Retrieves specific cell data from a tm1 cube 

    Args:
        cube_name: the name of the cube
        intersect: a list containing the dimension elements that make up the desired intersection
    Returns:
        float: the value at the cell
    """
    str_formatted = ""
    for item in intersect:
        if str_formatted == "":
            str_formatted=item
        else:
            str_formatted=str_formatted+","+item
    return (tm1.cells.get_value(cube_name=cube_name,elements=str_formatted))

@mcp.tool()
async def get_process_parameters(process_name: str):
    """
    Get all parameters for a given TI process.
    Args:
        process_name: Name of the process
    Returns:
        dict: A dictionary of process parameters
    """
    return tm1.processes.get(name_process=process_name).parameters

@mcp.tool()
async def get_all_processes():
    """
    Get all processes from the TM1 server.
    Returns:
        List of process objects
    """
    return tm1.processes.get_all()

@mcp.tool()
async def get_TI_process_code(process_name: str, code_section: str):
    """
    Gets entire code section for a given TI process.
    Args:
        process_name: Name of the process
        code_section: the section of code you want to receive. 
            valid options are prolog, metadata, data, and epilog
    Returns:
        str: the relevant code section
    """
    proc = tm1.processes.get(name_process=process_name)
    if code_section == 'prolog':
        return proc.prolog_procedure
    elif code_section == 'metadata':
        return proc.metadata_procedure
    elif code_section == 'data':
        return proc.data_procedure
    elif code_section == 'epilog':
        return proc.epilog_procedure
    else:
        return 'Failed - Please enter a valid code section. options are prolog, metadata, data, and epilog'

@mcp.tool()
async def get_all_chores():
    """
    Get all chores from the TM1 server.
    Returns:
        List of chore objects
    """
    return tm1.chores.get_all()

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

@mcp.tool()
async def execute_ti_process(process_name: str, parameters: dict):
    """
    Execute a TI process located on the TM1 server.
    
    Args:
        process_name: the name of the process on the server
        parameters: the parameters to pass to the process, if unknown use get_process_parameters
    Returns:
        str: Success message
    """
    proc = tm1.processes.get(process_name)
    for key in parameters.keys():
        for param in proc.parameters:
            if (param['Name']==key):
                param['Value']=parameters[key]

    return tm1.processes.execute_process_with_return(proc)

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
        dimensions: List of dimension names for the cube, order matters - 
            measures should be always last and dimensions should be in 
            relative size order - ascending
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

@mcp.tool()
async def create_process_tm1(proc_name:str):
    """
    Creates a new process on the TM1 server.

    Args:
        proc_name: Name of the process to create
    Returns:
        Process: the new tm1 process object
    """
    proc=Process(name=proc_name)
    tm1.processes.create(proc)
    return proc

#this function is not complete, only works to establish config options, cannot yet add processes becuase of depreciated tm1py functions
#will proceed with REST API calls when I finish it
@mcp.tool()
async def create_chore_tm1(chore_name:str, start_time:str, frequency:str, execution_mode:str, active:bool, task_list:list[dict]):
    """
    Creates a new chore on the TM1 server. Tasks must be added seperately via insert_process_into_chore()

    Args:
        chore_name: a name for the chore
        start_time: the time to start the chore in ISO 8601 format
            Ex: '2025-08-21T10:00:00' 
        frequency: the frequency of the chore, in ISO 8601 duration format
            Ex: "P01DT00H00M00S"
        execution_mode: the execution mode of the chore, valid options are 'MultipleCommit', 'SingleCommit'
        active: whether the chore is active or not
        task_list: a list of dictionaries that contain ChoreTask objects
            Ex: [ChoreTask:{"Process@odata.bind": "Processes('Sample Process - Daily Backup')", "Parameters": []}, 
                ChoreTask:{"Process@odata.bind": "Processes('Sample Process - Weekly Report')", "Parameters": [{"Name": "P1", "Value": "abc"}, {"Name": "P2", "Value": 99}]}]
    Returns:
        Chore: the new tm1 chore object
    """
    chore = Chore(
        name=chore_name, 
        start_time=ChoreStartTime.from_string(start_time), 
        frequency=ChoreFrequency.from_string(frequency), 
        execution_mode=execution_mode, 
        active=active,
        dst_sensitivity=False,
        tasks=task_list
    )

    tm1.chores.create(chore)
    return chore

# @mcp.tool()
# async def create_view_tm1(view_name:str, ):

#     return ''

##=============================================================== DELETE ===============================================================================

@mcp.tool()
async def delete_dimension_tm1(dim_name: str):
    """
    Deletes a dimension on the TM1 server. You cannot delete dimensions that are currently used in cubes

    Args:
        dim_name: Name of the dimension to delete
    Returns:
        
    """
    return tm1.dimensions.delete(dimension_name=dim_name)

@mcp.tool()
async def delete_cube_tm1(cube_name: str):
    """
    Deletes a cube on the TM1 server.
    Args:
        cube_name: Name of the cube to delete
    Returns:
        
    """
    return tm1.cubes.delete(cube_name=cube_name)

@mcp.tool()
async def delete_process_tm1(proc_name:str):
    """
    Deletes a process on the TM1 server.
    Args:
        proc_name: Name of the process to delete
    Returns:
        
    """
    return tm1.processes.delete(proc_name)

@mcp.tool()
async def delete_chore_tm1(chore_name:str):
    """
    Deletes a chore on the TM1 server.
    Args:
        chore_name: Name of the process to delete
    Returns:
        
    """
    return tm1.chores.delete(chore_name=chore_name)

##================================================================ WRITE ===============================================================================

@mcp.tool()
async def insert_dimension_elements_tm1(dim_name: str, elements: list, el_type: str):
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
async def insert_float_data_into_cube_tm1(value: float, cube_name: str, at_intersection: list):
    """
    Writes a numeric value to a specific cell in a TM1 cube. If the dimension order is unknown use 
    get_dimensions_in_cube_tm1() to retreive the ordering
    
    Args:
        cube_name: Name of the cube to write to
        value: Value to write
        coordinates: List of dimension coordinates for the desired cell
        Ex:    ['Jan','Tokyo','Temperature']
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
    
@mcp.tool()
async def insert_string_data_into_cube_tm1(value: str, cube_name: str, at_intersection: list):
    """
    Writes a string value to a specific cell in a TM1 cube. If the dimension order is unknown use 
    get_dimensions_in_cube_tm1() to retreive the ordering
    
    Args:
        cube_name: Name of the cube to write to
        value: Value to write
        coordinates: List of dimension coordinates for the desired cell
        Ex:    ['Jan','Tokyo','Temperature']
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
    
@mcp.tool()
async def bulk_insert_data_into_cube_tm1(cube_name: str, data_records: list[dict]):
    """
    Bulk insert multiple data points into a TM1 cube.
    
    Args:
        cube_name: Name of the cube
        data_records: List of dictionaries with 'coordinates' and 'value' keys
                     [{"coordinates": ["Jan", "Tokyo", "Temperature"], "value": 40},
                      {"coordinates": ["Feb", "Cairo", "Temperature"], "value": 81},
                      {"coordinates": ["Aug", "Cairo", "Rainfall"], "value": 42}, ...]
    
    Returns:
        Success message with count of records inserted
    """
    cellset = {}
    for record in data_records:
        # Build coordinate tuple for tm1py
        coord_tuple = tuple(record['coordinates'])
        cellset[coord_tuple] = record['value']
    
    tm1.cubes.cells.write_values(cube_name, cellset)
    return f"Successfully inserted {len(data_records)} records into {cube_name}"

@mcp.tool()
async def inject_into_process_code(process_name:str, code_section:str, new_code:str):
    '''
    Writes code to a given TI process by appending the provided code onto the existing
    process section. Valid options for code section are: prolog, metadata, data, and epilog
    
    Args:
        process_name: the name of the process to inject code into
        code_section: the TI process section where the code should be inserted. 
            valid options are: prolog, metadata, data, and epilog
        new_code: a string containing the exact code and formatting to insert
    Returns:
        Process: the process object which has been changed
    '''
    proc = tm1.processes.get(name_process=process_name)
    if code_section == 'prolog':
        proc.prolog_procedure+=new_code
    elif code_section == 'metadata':
        proc.metadata_procedure+=new_code
    elif code_section == 'data':
        proc.data_procedure+=new_code
    elif code_section == 'epilog':
        proc.epilog_procedure+=new_code
    else:
        return 'Failed - Please enter a valid code section. options are prolog, metadata, data, and epilog'
    tm1.processes.update(proc)
    return proc

@mcp.tool()
async def overwrite_process_code_section(process_name:str, code_section:str, new_code:str):
    '''
    Overwrites a provided process section with new code. Valid options for code_section 
    are: prolog, metadata, data, and epilog. The provided new_code will completely
    replace the existing code in that section. Use this in combination with get functions
    for code sections to insert new code in the middle of a section or make edits to existing
    code.

    Args:
        process_name: the name of the process to inject code into
        code_section: the TI process section where the code should be inserted. 
            valid options are: prolog, metadata, data, and epilog
        new_code: a string containing the exact code and formatting to insert
    Returns:
        Process: the process object which has been changed
    '''
    proc = tm1.processes.get(name_process=process_name)
    if code_section == 'prolog':
        proc.prolog_procedure=new_code
    elif code_section == 'metadata':
        proc.metadata_procedure=new_code
    elif code_section == 'data':
        proc.data_procedure=new_code
    elif code_section == 'epilog':
        proc.epilog_procedure=new_code
    else:
        return 'Failed - Please enter a valid code section. options are prolog, metadata, data, and epilog'
    tm1.processes.update(proc)
    return proc
    
# @mcp.tool()
# async def insert_process_into_chore(chore_name: str, process_name: str, parameters: list[dict[str, str]], position: int = 1):
#     """
#     Inserts a process into a chore on the TM1 server.
#     Args:
#         chore_name: Name of the chore to insert the process into
#         process_name: Name of the process to insert
#     Returns:
#         Chore: the updated chore object
#     """
#     chore = tm1.chores.get(chore_name)
#     task = ChoreTask(
#         process_name=process_name,
#         step=position,
#         parameters=parameters
#     )
#     chore.tasks.insert(task)
#     return chore

##================================================================ EXISTS ===============================================================================

@mcp.tool()
async def element_exists_in_dim(el_name: str, dim: str):
    """
    Checks if a specified element exists in a dimension on the tm1 server
    
    Args:
        el_name: element name
        dim: dimension name
    Returns:
        bool: True or False
    """
    return tm1.elements.exists(dimension_name=dim,hierarchy_name=dim,element_name=el_name)

#main method - starts MCP server
if __name__ == "__main__":
    # Initialize and run the server
    mcp.run(transport='stdio')