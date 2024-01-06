import os
import logging
import time
import json
import git

from dotenv import load_dotenv
from fastapi import FastAPI
from discover_functions_manager import DiscoverFunctionsManager, DiscoverFunctionsModel
from discover_agents_manager import DiscoverAgentsManager, DiscoverAgentsModel
from discover_groups_manager import DiscoverGroupsManager, DiscoverGroupsModel
from repository_service import RepositoryService
from metagpt_service import MetaGPTService
from discover_coding_assistants_manager import DiscoverCodingAssistantsManager, DiscoverCodingAssistantsModel
from discover_code_repository_manager import DiscoverCodeRepositoryManager, DiscoverCodeRepositoryModel
from functions_and_agents_metadata import CodeExecInput, WebResearchInput, CodeAssistantInput, CodeRequestInput, DeleteGroupsModel, DeleteCodeAssistantsModel, GetCodingAssistantsModel, UpsertCodingAssistantInput, DeleteCodeRepositoryModel, GetCodeRepositoryModel, UpsertCodeRepositoryInput, GetFunctionModel, FunctionsAndAgentsMetadata, GetGroupModel, UpsertGroupInput, UpdateComms, AddFunctionInput, GetAgentModel, DeleteAgentModel, UpsertAgentModel
from rate_limiter import RateLimiter, SyncRateLimiter
from typing import List
from metagpt.const import DEFAULT_WORKSPACE_ROOT
from pathlib import Path

rate_limiter = RateLimiter(rate=10, period=1)  # Allow 5 tasks per second
rate_limiter_sync = SyncRateLimiter(rate=10, period=1)
# Load environment variables
load_dotenv()

app = FastAPI()
MAX_CAPABILITY = 128

# Initialize logging
LOGFILE_PATH = os.path.join(os.path.dirname(
    os.path.abspath(__file__)), 'app.log')
logging.basicConfig(filename=LOGFILE_PATH, filemode='w',
                    format='%(asctime)s.%(msecs)03d %(name)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S', force=True, level=logging.INFO)


discover_functions_manager = DiscoverFunctionsManager(rate_limiter, rate_limiter_sync)
discover_agents_manager = DiscoverAgentsManager(rate_limiter, rate_limiter_sync)
discover_groups_manager = DiscoverGroupsManager(rate_limiter, rate_limiter_sync)
discover_coding_assistants_manager = DiscoverCodingAssistantsManager(rate_limiter, rate_limiter_sync)
discover_code_repository_manager = DiscoverCodeRepositoryManager(rate_limiter, rate_limiter_sync)
functions_and_agents_metadata = FunctionsAndAgentsMetadata()


@app.post('/discover_functions/')
async def discoverFunctions(function_input: DiscoverFunctionsModel):
    """Endpoint to get functions based on provided input."""
    start = time.time()
    if function_input.auth.api_key == '':
        return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
    if function_input.auth.namespace_id == '':
        return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
    function_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming', 'planning']

    if function_input.category != "" and function_input.category not in function_types:
        return {'response': json.dumps({"error": f'Invalid category {function_input.category}, must be one of {function_types}'})}

    logging.info(f'Discovering function: {function_input}')
    result = await discover_functions_manager.pull_functions(function_input)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/upsert_functions/')
async def upsertFunctions(function_inputs: List[AddFunctionInput]):
    """Endpoint to upsert functions based on provided functions."""
    start = time.time()
    if len(function_inputs) == 0:
        return {'response': json.dumps({"error": "No functions provided"}), 'elapsed_time': 0}
    functions = {}
    for function_input in function_inputs:
        if function_input.status == "accepted" and function_input.function_code:
            return {'response': json.dumps({"error": "Status cannot be set to accept while function_code is also set."}), 'elapsed_time': 0}
        if function_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if function_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        function_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming', 'planning']
        if function_input.category and function_input.category not in function_types:
            return {'response': json.dumps({"error": f'Invalid category for function {function_input.name}, must be one of {function_types}'})}

        function_status = ['development', 'testing', 'accepted']
        if function_input.status and function_input.status not in function_status:
            return {'response': json.dumps({"error": f'Invalid status for function {function_input.name}, must be one of {function_status}'})}

        if function_input.description:
            # Append the new function to the category
            new_function = {
                'name': function_input.name,
                'description': function_input.description
            }
            if function_input.category in functions:
                functions[function_input.category].append(new_function)
            else:
                functions[function_input.category] = [new_function]

    # Push the functions
    result = await functions_and_agents_metadata.upsert_functions(function_inputs)
    if result != "success":
        end = time.time()
        return {'response': result, 'elapsed_time': end-start}
    if len(functions) > 0:
        result = await discover_functions_manager.push_functions(function_input.auth, functions)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/get_agents/')
async def getAgents(agent_inputs: List[GetAgentModel]):
    """Endpoint to get agent."""
    start = time.time()
    if len(agent_inputs) == 0:
        return {'response': json.dumps({"error": "No agents provided"}), 'elapsed_time': 0}
    for agent in agent_inputs:
        if agent.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if agent.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if agent.name == '':
            return {'response': json.dumps({"error": json.dumps({"error": "agent name not provided!"})}), 'elapsed_time': 0}
    response, err = await functions_and_agents_metadata.get_agents(agent_inputs)
    if err is not None:
        for agent in agent_inputs:
            agent.auth.namespace_id = ""
        response, err = await functions_and_agents_metadata.get_agents(agent_inputs)
        if err is not None:
            response = err
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/upsert_agents/')
async def upsertAgents(agent_inputs: List[UpsertAgentModel]):
    """Endpoint to upsert agent."""
    if len(agent_inputs) == 0:
        return {'response': "No agents provided", 'elapsed_time': 0}
    start = time.time()
    agents = {}
    for agent_input in agent_inputs:
        if agent_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if agent_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if agent_input.name == '':
            return {'response': json.dumps({"error": "agent name not provided!"}), 'elapsed_time': 0}
        if agent_input.description and agent_input.description == '':
            return {'response': json.dumps({"error": "agent description not provided!"}), 'elapsed_time': 0}
        if agent_input.category:
            agent_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming', 'planning']
            if agent_input.category not in agent_types:
                return {'response': json.dumps({"error": f'Invalid category for agent {agent_input.name}, must be one of {agent_types}'}), 'elapsed_time': 0}
        if agent_input.human_input_mode:
            human_input_types = ['ALWAYS', 'NEVER', 'TERMINATE']
            if agent_input.human_input_mode not in human_input_types:
                return {'response': json.dumps({"error": f'Invalid human_input_mode for agent {agent_input.human_input_mode}, must be one of {human_input_types}'}), 'elapsed_time': 0}
        if agent_input.capability:
            if agent_input.capability < 0 or agent_input.capability > (MAX_CAPABILITY*2 - 1):
                return {'response': json.dumps({"error": f'Invalid capability for agent {agent_input.capability}'}), 'elapsed_time': 0}
    # Push the agent
    response = await functions_and_agents_metadata.upsert_agents(agent_inputs)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    for agent_input in agent_inputs:
        if not agent_input.description or not agent_input.category:
            continue
        # Append the new agent to the category
        new_agent = {
            'name': agent_input.name,
            'description': agent_input.description,
        }
        if agent_input.category in agents:
            agents[agent_input.category].append(new_agent)
        else:
            agents[agent_input.category] = [new_agent]
    if len(agents) > 0:
        response = await discover_agents_manager.push_agents(agent_inputs[0].auth, agents)
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}


@app.post('/update_communication_stats/')
async def updateComms(comms_input: UpdateComms):
    """Endpoint to update communication (incoming/outgoing) stats an agent."""
    start = time.time()
    if not comms_input.sender or not comms_input.receiver:
        return {'response': json.dumps({"error": "sender and receiver not provided"}), 'elapsed_time': 0}
    if comms_input.auth.namespace_id == '':
        return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
    response = await functions_and_agents_metadata.update_comms(comms_input)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/discover_agents/')
async def discoverAgents(agent_input: DiscoverAgentsModel):
    """Endpoint to discover agents."""
    start = time.time()
    if agent_input.auth.api_key == '':
        return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
    if agent_input.auth.namespace_id == '':
        return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
    agent_types = ['information_retrieval', 'communication', 'data_processing', 'sensory_perception', 'programming', 'planning']

    if agent_input.category != "" and agent_input.category not in agent_types:
        return {'response': json.dumps({"error": f'Invalid category {agent_input.category}, must be one of {agent_types}'})}

    result = await discover_agents_manager.pull_agents(agent_input)
    
    # Flatten the result if it's a list of lists
    if result and isinstance(result[0], list):
        result = [item for sublist in result for item in sublist]

    # Extract agent names from the result
    agent_names = [agent['name'] for agent in result if 'name' in agent]

    # Get the groups for these agents
    agent_groups = await functions_and_agents_metadata.get_agent_groups(agent_names, agent_input.auth.namespace_id)

    # Add the group names to the result
    for agent in result:
        agent['group_names'] = agent_groups.get(agent['name'], [])

    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/discover_groups/')
async def discoverGroups(group_input: DiscoverGroupsModel):
    """Endpoint to discover groups."""
    start = time.time()
    if group_input.auth.api_key == '':
        return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
    if group_input.auth.namespace_id == '':
        return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}

    result = await discover_groups_manager.pull_groups(group_input)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/get_groups/')
async def getGroups(group_inputs: List[GetGroupModel]):
    """Endpoint to get group info."""
    start = time.time()
    if len(group_inputs) == 0:
        return {'response': "No groups provided", 'elapsed_time': 0}
    for group_input in group_inputs:
        if group_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if group_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if group_input.name == '':
            return {'response': json.dumps({"error": "group name not provided!"}), 'elapsed_time': 0}
    response, err = await functions_and_agents_metadata.get_groups(group_inputs)
    if err is not None:
        for group in group_inputs:
            group.auth.namespace_id = ""
        response, err = await functions_and_agents_metadata.get_groups(group_inputs)
        if err is not None:
            response = err
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}


@app.post('/get_coding_assistants/')
async def getCodingAssistants(code_inputs: List[GetCodingAssistantsModel]):
    """Endpoint to get coding assistant info."""
    start = time.time()
    if len(code_inputs) == 0:
        return {'response': "No coding assistants provided", 'elapsed_time': 0}
    for code_input in code_inputs:
        if code_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if code_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if code_input.name == '':
            return {'response': json.dumps({"error": "Assistant name not provided!"}), 'elapsed_time': 0}
    response, err = await functions_and_agents_metadata.get_coding_assistants(code_inputs)
    if err is not None:
        for group in code_inputs:
            group.auth.namespace_id = ""
        response, err = await functions_and_agents_metadata.get_coding_assistants(code_inputs)
        if err is not None:
            response = err
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/get_code_repositories/')
async def getCodeRepositories(code_inputs: List[GetCodeRepositoryModel]):
    """Endpoint to get code repository info."""
    start = time.time()
    if len(code_inputs) == 0:
        return {'response': "No code repositories provided", 'elapsed_time': 0}
    for code_input in code_inputs:
        if code_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if code_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if code_input.name == '':
            return {'response': json.dumps({"error": "Repository name not provided!"}), 'elapsed_time': 0}
    response, err = await functions_and_agents_metadata.get_code_repository(code_inputs)
    if err is not None:
        for group in code_inputs:
            group.auth.namespace_id = ""
        response, err = await functions_and_agents_metadata.get_code_repository(code_inputs)
        if err is not None:
            response = err
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/discover_coding_assistants/')
async def discoverCodingAssistants(code_input: DiscoverCodingAssistantsModel):
    """Endpoint to discover coding assistants."""
    start = time.time()
    if code_input.auth.api_key == '':
        return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
    if code_input.auth.namespace_id == '':
        return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}

    result = await discover_coding_assistants_manager.pull_coding_assistants(code_input)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/discover_code_repositories/')
async def discoverCodeRepositories(code_input: DiscoverCodeRepositoryModel):
    """Endpoint to discover coding assistants."""
    start = time.time()
    if code_input.auth.api_key == '':
        return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
    if code_input.auth.namespace_id == '':
        return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}

    result = await discover_code_repository_manager.pull_code_repository(code_input)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/get_functions/')
async def getFunctions(function_inputs: List[GetFunctionModel]):
    """Endpoint to get function info."""
    start = time.time()
    if len(function_inputs) == 0:
        return {'response': "No groups provided", 'elapsed_time': 0}
    for function_input in function_inputs:
        if function_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if function_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if function_input.name == '':
            return {'response': json.dumps({"error": "function name not provided!"}), 'elapsed_time': 0}
    response = await functions_and_agents_metadata.get_functions(function_inputs)
    if not response:
        for fn in function_inputs:
            fn.auth.namespace_id = ""
        response = await functions_and_agents_metadata.get_functions(function_inputs)
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}


@app.post('/upsert_groups/')
async def upsertGroups(group_inputs: List[UpsertGroupInput]):
    """Endpoint to upsert group."""
    if len(group_inputs) == 0:
        return {'response': "No groups provided", 'elapsed_time': 0}
    start = time.time()
    groups = []
    for group_input in group_inputs:
        if group_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if group_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if group_input.name == '':
            return {'response': json.dumps({"error": "group name not provided!"}), 'elapsed_time': 0}
        if group_input.description and group_input.description == '':
            return {'response': json.dumps({"error": "group description not provided!"}), 'elapsed_time': 0}

    # Push the group
    response = await functions_and_agents_metadata.upsert_groups(group_inputs)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    for group_input in group_inputs:
        if not group_input.description:
            continue
        new_group = {
            'name': group_input.name,
            'description': group_input.description
        }
        groups.append(new_group)
    
    if len(groups) > 0:
        response = await discover_groups_manager.push_groups(group_inputs[0].auth, groups)
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}


@app.post('/upsert_coding_assistants/')
async def upsertCodingAssistants(code_inputs: List[UpsertCodingAssistantInput]):
    """Endpoint to upsert coding assistants."""
    if len(code_inputs) == 0:
        return {'response': "No coding assistants provided", 'elapsed_time': 0}
    start = time.time()
    assistants = []
    for code_input in code_inputs:
        if code_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if code_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if code_input.name == '':
            return {'response': json.dumps({"error": "Assistant name not provided!"}), 'elapsed_time': 0}
        if code_input.description and code_input.description == '':
            return {'response': json.dumps({"error": "coding assistant description not provided!"}), 'elapsed_time': 0}
        if code_input.repository_name == '':
            return {'response': json.dumps({"error": "Repository name not provided!"}), 'elapsed_time': 0}
    # Push the assistant
    response = await functions_and_agents_metadata.upsert_coding_assistants(code_inputs)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    for code_input in code_inputs:
        if not code_input.description:
            continue
        new_group = {
            'name': code_input.name,
            'description': code_input.description
        }
        assistants.append(new_group)
    
    if len(assistants) > 0:
        response = await discover_coding_assistants_manager.push_coding_assistants(code_inputs[0].auth, assistants)
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/upsert_code_repositories/')
async def upsertCodeRepositories(code_inputs: List[UpsertCodeRepositoryInput]):
    """Endpoint to upsert coding repositories."""
    if len(code_inputs) == 0:
        return {'response': "No code repositories provided", 'elapsed_time': 0}
    start = time.time()
    assistants = []
    for code_input in code_inputs:
        if code_input.auth.gh_pat == '':
            return {'response': json.dumps({"error": "Github PAT not provided"}), 'elapsed_time': 0}
        if code_input.auth.gh_user == '':
            return {'response': json.dumps({"error": "Github User not provided"}), 'elapsed_time': 0}
        if code_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if code_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if code_input.description and code_input.description == '':
            return {'response': json.dumps({"error": "coding repository description not provided!"}), 'elapsed_time': 0}
        if code_input.gh_remote_url and code_input.gh_remote_url == '':
            return {'response': json.dumps({"error": "gh_remote_url not provided!"}), 'elapsed_time': 0}
        if code_input.name == '':
            return {'response': json.dumps({"error": "Repository name not provided!"}), 'elapsed_time': 0}
        user_workspace = DEFAULT_WORKSPACE_ROOT / code_input.auth.gh_user
        user_workspace.mkdir(parents=True, exist_ok=True)
        code_input.workspace = user_workspace / code_input.name
        working_gh_remote_url_response = RepositoryService.create_github_remote_repo(code_input.auth, code_input.name, code_input.description, code_input.private, code_input.gh_remote_url)
        if 'error' in working_gh_remote_url_response:
            return {'response': json.dumps(working_gh_remote_url_response), 'elapsed_time': 0}
        working_gh_remote_url = working_gh_remote_url_response
        code_input.is_forked = code_input.gh_remote_url and working_gh_remote_url != code_input.gh_remote_url
        code_input.gh_remote_url = working_gh_remote_url
        clone_response = RepositoryService.clone_repo(code_input.auth, code_input.gh_remote_url, code_input.workspace)
        if 'error' in clone_response:
            return {'response': json.dumps(clone_response), 'elapsed_time': 0}
        code_input.workspace = str(code_input.workspace)
    # Push the repo
    response = await functions_and_agents_metadata.upsert_code_repository(code_inputs)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
        
    for code_input in code_inputs:
        if not code_input.description:
            continue
        new_group = {
            'name': code_input.name,
            'description': code_input.description
        }
        assistants.append(new_group)
    
    if len(assistants) > 0:
        response = await discover_code_repository_manager.push_code_repository(code_inputs[0].auth, assistants)
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/delete_agents/')
async def deleteAgent(agent_inputs: List[DeleteAgentModel]):
    """Endpoint to delete agent."""
    start = time.time()
    for agent_input in agent_inputs:
        if agent_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if agent_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if agent_input.name == '':
            return {'response': json.dumps({"error": "agent name not provided!"}), 'elapsed_time': 0}
    # delete the agent
    response = await functions_and_agents_metadata.delete_agents(agent_inputs)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    agent_names = [agent_input.name for agent_input in agent_inputs]
    result = discover_agents_manager.delete_agents(agent_input.auth, agent_names)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/delete_code_assistants/')
async def deleteCodeAssistants(code_inputs: List[DeleteCodeAssistantsModel]):
    """Endpoint to delete assistants."""
    start = time.time()
    for agent_input in code_inputs:
        if agent_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if agent_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if agent_input.name == '':
            return {'response': json.dumps({"error": "Assistant name not provided!"}), 'elapsed_time': 0}
    # delete the assistant
    response = await functions_and_agents_metadata.delete_code_assistants(code_inputs)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    assistant_names = [code_input.name for code_input in code_inputs]
    result = discover_coding_assistants_manager.delete_coding_assistants(code_inputs[0].auth, assistant_names)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/delete_code_repositories/')
async def deleteCodeRepostories(code_inputs: List[DeleteCodeRepositoryModel]):
    """Endpoint to delete repos."""
    start = time.time()
    for agent_input in code_inputs:
        if agent_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if agent_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if agent_input.name == '':
            return {'response': json.dumps({"error": "Repository name not provided!"}), 'elapsed_time': 0}
    # delete the repo
    response = await functions_and_agents_metadata.delete_code_repository(code_inputs)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    repo_names = [code_input.name for code_input in code_inputs]
    result = discover_code_repository_manager.delete_code_repository(code_inputs[0].auth, repo_names)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}

@app.post('/delete_groups/')
async def deleteGroups(group_inputs: List[DeleteGroupsModel]):
    """Endpoint to delete groups."""
    start = time.time()
    for agent_input in group_inputs:
        if agent_input.auth.api_key == '':
            return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
        if agent_input.auth.namespace_id == '':
            return {'response': json.dumps({"error": "namespace_id not provided"}), 'elapsed_time': 0}
        if agent_input.name == '':
            return {'response': json.dumps({"error": "Group name not provided!"}), 'elapsed_time': 0}
    # delete the group
    response = await functions_and_agents_metadata.delete_groups(group_inputs)
    if response != "success":
        end = time.time()
        return {'response': response, 'elapsed_time': end-start}
    group_names = [group_input.name for group_input in group_inputs]
    result = discover_groups_manager.delete_groups(group_inputs[0].auth, group_names)
    end = time.time()
    return {'response': result, 'elapsed_time': end-start}


@app.post('/code_issue_pull_request/')
async def codePullRequest(code_input: CodeRequestInput):
    """Endpoint to issue PR."""
    start = time.time()
    if code_input.auth.gh_pat == '':
        return {'response': json.dumps({"error": "Github PAT not provided"}), 'elapsed_time': 0}
    if code_input.auth.gh_user == '':
        return {'response': json.dumps({"error": "Github User not provided"}), 'elapsed_time': 0}
    if code_input.title == '':
        return {'response': json.dumps({"error": "Pull request title cannot be empty!"}), 'elapsed_time': 0}
    if code_input.body == '':
        return {'response': json.dumps({"error": "Pull request body cannot be empty!"}), 'elapsed_time': 0}
    if code_input.branch == '':
        return {'response': json.dumps({"error": "Pull request active branch cannot be empty!"}), 'elapsed_time': 0}
    if code_input.repository_name == '':
        return {'response': json.dumps({"error": "Repository name not provided!"}), 'elapsed_time': 0}
    response = RepositoryService.create_github_pull_request(code_input.auth, code_input.repository_name, code_input.title,  code_input.body,  code_input.branch)
    if 'error' in response:
        return {'response': json.dumps(response), 'elapsed_time': 0}
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/code_assistant_run/')
async def codeAssistantRun(code_input: CodeAssistantInput):
    """Endpoint to run code assistant."""
    start = time.time()
    if code_input.auth.api_key == '':
        return {'response': json.dumps({"error": "LLM API key not provided"}), 'elapsed_time': 0}
    if code_input.auth.gh_pat == '':
        return {'response': json.dumps({"error": "Github PAT not provided"}), 'elapsed_time': 0}
    if code_input.auth.gh_user == '':
        return {'response': json.dumps({"error": "Github User not provided"}), 'elapsed_time': 0}
    if code_input.reqa_file and code_input.reqa_file == '':
        return {'response': json.dumps({"error": "reqa_file was empty"}), 'elapsed_time': 0}
    if code_input.project_name == '':
        return {'response': json.dumps({"error": "Code assistant project_name not provided!"}), 'elapsed_time': 0}
    service = MetaGPTService()
    response = await service.run(code_input.auth, Path(code_input.workspace), code_input.project_name, code_input.reqa_file, code_input.command_message)
    if 'error' in response:
        return {'response': json.dumps(response), 'elapsed_time': 0}
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/web_research/')
async def webResearch(code_input: WebResearchInput):
    """Endpoint to run code assistant."""
    start = time.time()
    if code_input.topic == '':
        return {'response': json.dumps({"error": "Topic was not provided!"}), 'elapsed_time': 0}
    response = await MetaGPTService.web_research(code_input.topic)
    if 'error' in response:
        return {'response': json.dumps(response), 'elapsed_time': 0}
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}

@app.post('/code_execute_git_command/')
async def execGitCommand(code_input: CodeExecInput):
    """Endpoint to run git cmd."""
    start = time.time()
    if code_input.workspace == '':
        return {'response': json.dumps({"error": "workspace not provided!"}), 'elapsed_time': 0}
    if code_input.command_git_command == '':
        return {'response': json.dumps({"error": "command_git_command not provided!"}), 'elapsed_time': 0}
    try:
        repo = git.Repo(Path(code_input.workspace), search_parent_directories=False)
    except Exception as e:
        return {'response': json.dumps({"error": f"Could not open Git directory: {e}"}), 'elapsed_time': 0}
    response = await RepositoryService.execute_git_command(repo, code_input.command_git_command)
    if 'error' in response:
        return {'response': json.dumps(response), 'elapsed_time': 0}
    end = time.time()
    return {'response': response, 'elapsed_time': end-start}
