import os
import logging

from dotenv import load_dotenv
from fastapi import FastAPI
from discover_functions_manager import DiscoverFunctionsManager, DiscoverFunctionsModel
from discover_agents_manager import DiscoverAgentsManager, DiscoverAgentsModel
from functions_and_agents_metadata import FunctionsAndAgentsMetadata, AddFunctionModel, AddFunctionInput, GetAgentModel, UpsertAgentModel, UpsertAgentInput
from cachetools import TTLCache
from rate_limiter import RateLimiter, SyncRateLimiter
rate_limiter = RateLimiter(rate=5, period=1)  # Allow 5 tasks per second
rate_limiter_sync = SyncRateLimiter(rate=5, period=1)
# Load environment variables
load_dotenv()

app = FastAPI()

# Initialize logging
LOGFILE_PATH = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), 'app.log')
logging.basicConfig(filename=LOGFILE_PATH, filemode='w',
                    format='%(asctime)s.%(msecs)03d %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S', force=True, level=logging.INFO)


discover_functions_manager = DiscoverFunctionsManager(rate_limiter, rate_limiter_sync)
discover_agents_manager = DiscoverAgentsManager(rate_limiter, rate_limiter_sync)
functions_and_agents_metadata = FunctionsAndAgentsMetadata(rate_limiter, rate_limiter_sync)

agentcache = TTLCache(maxsize=16384, ttl=36000)
discoverfunctioncache = TTLCache(maxsize=16384, ttl=36000)
discoveragentscache = TTLCache(maxsize=16384, ttl=36000)

@app.post('/discover_functions/')
async def discoverFunctions(function_input: DiscoverFunctionsModel):
    """Endpoint to get functions based on provided input."""
    result = discoverfunctioncache.get(function_input)
    if result is not None:
        logging.info(f'Found functions in cache, result {result}')
        return {'response': result, 'elapsed_time': 0}
    logging.info(f'Processing Action Item: {function_input.action_items}')
    result, elapsed_time = await discover_functions_manager.pull_functions(function_input)
    if len(result) > 0:
        discoverfunctioncache[function_input] = result
    return {'response': result, 'elapsed_time': elapsed_time}

@app.post('/add_function/')
async def addFunction(function_output: AddFunctionInput):
    """Endpoint to push functions based on provided functions."""
    logging.info(f'Adding function: {function_output.name}')
    functions = {}
    function_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming']

    function_output.category = function_output.category.lower().replace(' ', '_')
    if function_output.category not in function_types:
        return {'response': f'Invalid category for function {function_output.name}, must be one of {function_types}'}

    # Append the new function to the category
    new_function = {
        'name': function_output.name,
        'description': function_output.description
    }
    functions[function_output.category] = [new_function]

    # Push the functions
    result, elapsed_time1 = await functions_and_agents_metadata.set_function(AddFunctionModel(function_output))
    if result != "success":
        return {'response': result, 'elapsed_time': elapsed_time1}
    result, elapsed_time2 = await discover_functions_manager.push_functions(function_output.user_id, function_output.api_key, functions)
    return {'response': result, 'elapsed_time': elapsed_time1+elapsed_time2}

@app.post('/get_agent/')
async def getAgent(agent_input: GetAgentModel):
    """Endpoint to get agent."""
    agentcache
    result = agentcache.get(agent_input.name)
    if result is not None:
        logging.info(f'Found agent in cache, result {result}')
        return {'response': result, 'elapsed_time': 0}
    response, elapsed_time = functions_and_agents_metadata.get_agent(agent_input.name)
    if len(response["name"]) > 0:
        agentcache[agent_input.name] = response
    return {'response': response, 'elapsed_time': elapsed_time}

@app.post('/upsert_agent/')
async def upsertAgent(agent_input: UpsertAgentInput):
    """Endpoint to upsert agent."""
    logging.info(f'Adding agent: {agent_input.name}')
    agents = {}
    agent_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming', 'planning', 'groups']

    agent_input.category = agent_input.category.lower().replace(' ', '_')
    if agent_input.category not in agent_types:
        return {'response': f'Invalid category for agent {agent_input.name}, must be one of {agent_types}'}

    # Append the new agent to the category
    new_agent = {
        'name': agent_input.name,
        'description': agent_input.description
    }
    agents[agent_input.category] = [new_agent]

    # Push the agent
    response, elapsed_time1 = functions_and_agents_metadata.upsert_agent(UpsertAgentModel(agent_input))
    if response != "success":
        return {'response': result, 'elapsed_time': elapsed_time1}

    agentcache.pop(agent_input.name)
    discoveragentscache.clear()
    result, elapsed_time2 = await discover_agents_manager.push_agents(agent_input.user_id, agent_input.api_key, agents)
    return {'response': result, 'elapsed_time': elapsed_time1+elapsed_time2}

@app.post('/discover_agents/')
async def discoverAgents(agent_input: DiscoverAgentsModel):
    """Endpoint to upsert an agent."""
    if agent_input.query:
        result = discoveragentscache.get(agent_input.query)
        if result is not None:
            logging.info(f'Found agents in cache, result {result}')
            return {'response': result, 'elapsed_time': 0}
    result, elapsed_time = await discover_agents_manager.pull_agents(agent_input)
    if len(result) > 0 and agent_input.query:
        discoveragentscache[agent_input.query] = result
    return {'response': result, 'elapsed_time': elapsed_time}